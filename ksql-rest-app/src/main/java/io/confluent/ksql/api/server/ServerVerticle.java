/*
 * Copyright 2020 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.api.server;

import io.confluent.ksql.api.spi.Endpoints;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.ServerWebSocket;
import io.vertx.core.http.UpgradeRejectedException;
import io.vertx.core.http.WebSocket;
import io.vertx.core.http.WebSocketBase;
import io.vertx.core.http.WebSocketConnectOptions;
import io.vertx.core.http.WebSocketFrame;
import io.vertx.core.net.SocketAddress;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The server deploys multiple server verticles. This is where the HTTP2 requests are handled. The
 * actual implementation of the endpoints is provided by an implementation of {@code Endpoints}.
 */
public class ServerVerticle extends AbstractVerticle {

  private static final Logger log = LoggerFactory.getLogger(ServerVerticle.class);

  private final Endpoints endpoints;
  private final HttpServerOptions httpServerOptions;
  private final Server server;
  private final boolean proxyEnabled;
  private ConnectionQueryManager connectionQueryManager;
  private volatile HttpServer httpServer;

  private HttpClient proxyClient;
  private SocketAddress proxyTarget;

  public ServerVerticle(final Endpoints endpoints, final HttpServerOptions httpServerOptions,
      final Server server, final boolean proxyEnabled) {
    this.endpoints = Objects.requireNonNull(endpoints);
    this.httpServerOptions = Objects.requireNonNull(httpServerOptions);
    this.server = Objects.requireNonNull(server);
    this.proxyEnabled = proxyEnabled;
  }

  @Override
  public void start(final Promise<Void> startPromise) {
    this.proxyClient = vertx
        .createHttpClient(
            new HttpClientOptions().setMaxPoolSize(10).setMaxInitialLineLength(65536));
    this.connectionQueryManager = new ConnectionQueryManager(context, server);
    httpServer = vertx.createHttpServer(httpServerOptions).requestHandler(setupRouter())
        //  .webSocketHandler(this::websocketHandlerOld)
        .exceptionHandler(ServerUtils::unhandledExceptonHandler);
    httpServer.listen(ar -> {
      if (ar.succeeded()) {
        startPromise.complete();
      } else {
        startPromise.fail(ar.cause());
      }
    });
  }

  @Override
  public void stop(final Promise<Void> stopPromise) {
    proxyClient.close();
    if (httpServer == null) {
      stopPromise.complete();
    } else {
      httpServer.close(stopPromise.future());
    }
  }

  int actualPort() {
    return httpServer.actualPort();
  }

  private Router setupRouter() {
    final Router router = Router.router(vertx);
    router.route(HttpMethod.POST, "/query-stream")
        .produces("application/vnd.ksqlapi.delimited.v1")
        .produces("application/json")
        .handler(BodyHandler.create())
        .handler(new QueryStreamHandler(endpoints, connectionQueryManager, context,
            server));
    router.route(HttpMethod.POST, "/inserts-stream")
        .produces("application/vnd.ksqlapi.delimited.v1")
        .produces("application/json")
        .handler(new InsertsStreamHandler(context, endpoints, server.getWorkerExecutor()));
    router.route(HttpMethod.POST, "/close-query").handler(BodyHandler.create())
        .handler(new CloseQueryHandler(server));
    router.route(HttpMethod.GET, "/ws/*").handler(this::websocketHandler);

    if (proxyEnabled) {
      // Everything else is proxied
      router.route().handler(new ProxyHandler(proxyTarget, proxyClient, server));
    }
    router.route().failureHandler(this::handleFailure);
    return router;
  }

  private void handleFailure(final RoutingContext routingContext) {
    if (routingContext.statusCode() == 500) {
      log.error("Unexpected exception in router", routingContext.failure());
    }
    routingContext.response().setStatusCode(routingContext.statusCode()).end();
  }

  private void websocketHandler(final RoutingContext routingContext) {
    if (proxyTarget == null) {
      proxyTarget = server.getProxyTarget();
    }
    final HttpServerRequest request = routingContext.request();
    final WebSocketConnectOptions options = new WebSocketConnectOptions()
        .setHost(proxyTarget.host())
        .setPort(proxyTarget.port())
        .setHeaders(request.headers())
        .setURI(request.uri());
    request.pause();
    proxyClient.webSocket(options, ar -> {
      request.resume();
      if (ar.succeeded()) {
        final WebSocket webSocket = ar.result();
        final ServerWebSocket serverWebSocket = request.upgrade();
        WebsocketPipe.pipe(serverWebSocket, webSocket);
        WebsocketPipe.pipe(webSocket, serverWebSocket);
      } else {
        if (ar.cause() instanceof UpgradeRejectedException) {
          final UpgradeRejectedException uge = (UpgradeRejectedException) ar.cause();
          request.response().setStatusCode(uge.getStatus()).setStatusMessage(uge.getMessage())
              .end();
        } else {
          log.error("Failed to proxy websocket", ar.cause());
        }
      }
    });
  }

  private void websocketHandlerOld(final ServerWebSocket serverWebSocket) {
    if (proxyTarget == null) {
      proxyTarget = server.getProxyTarget();
    }
    serverWebSocket.pause();
    final WebSocketConnectOptions options = new WebSocketConnectOptions()
        .setHost(proxyTarget.host())
        .setPort(proxyTarget.port())
        .setHeaders(serverWebSocket.headers())
        .setURI(serverWebSocket.uri());
    proxyClient.webSocket(options, ar -> {
      if (ar.succeeded()) {
        final WebSocket webSocket = ar.result();
        WebsocketPipe.pipe(serverWebSocket, webSocket);
        WebsocketPipe.pipe(webSocket, serverWebSocket);
        serverWebSocket.resume();
      } else {
        log.error("Failed to proxy websocket", ar.cause());
        serverWebSocket.close();
      }
    });
  }

  private static final class WebsocketPipe {

    private final WebSocketBase from;
    private final WebSocketBase to;
    private boolean drainHandlerSet;

    public static void pipe(final WebSocketBase from, final WebSocketBase to) {
      new WebsocketPipe(from, to);
    }

    private WebsocketPipe(final WebSocketBase from, final WebSocketBase to) {
      this.from = from;
      this.to = to;

      from.frameHandler(this::frameHandler);
      from.exceptionHandler(this::exceptionHandler);
      // Don't need to close the from websocket on end of to websocket as we proxy
      // close frames too
    }

    private void frameHandler(final WebSocketFrame wsf) {
      if (to.isClosed()) {
        return;
      }
      to.writeFrame(wsf);
      if (!drainHandlerSet && to.writeQueueFull()) {
        drainHandlerSet = true;
        from.pause();
        to.drainHandler(this::drainHandler);
      }
    }

    private void drainHandler(final Void v) {
      drainHandlerSet = false;
      from.resume();
    }

    private void exceptionHandler(final Throwable t) {
      log.error("Exception in proxying websocket", t);
    }

  }


}
