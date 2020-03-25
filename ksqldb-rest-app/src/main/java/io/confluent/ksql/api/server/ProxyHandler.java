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

import io.vertx.core.MultiMap;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.http.HttpClientRequest;
import io.vertx.core.http.HttpClientResponse;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.http.ServerWebSocket;
import io.vertx.core.http.UpgradeRejectedException;
import io.vertx.core.http.WebSocket;
import io.vertx.core.http.WebSocketBase;
import io.vertx.core.http.WebSocketConnectOptions;
import io.vertx.core.http.WebSocketFrame;
import io.vertx.core.net.SocketAddress;
import io.vertx.core.streams.ReadStream;
import io.vertx.core.streams.WriteStream;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import org.apache.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Used to proxy HTTP and websocket traffic from Vert.x to an internal Jetty server. This proxy is
 * used to make the migration from Jetty to Vert.x simpler. Once we have migrated all endpoints to
 * Vert.x, we will remove Jetty and remove this proxy
 */
class ProxyHandler {

  private static final Logger log = LoggerFactory.getLogger(ProxyHandler.class);

  private final HttpClient proxyClient;
  private final Server server;
  private SocketAddress proxyTarget;

  ProxyHandler(final Server server) {
    this.proxyClient = server.getVertx()
        .createHttpClient(
            new HttpClientOptions().setMaxPoolSize(10));
    this.server = server;
  }

  void close() {
    proxyClient.close();
  }

  void setupRoutes(final Router router) {
    router.route(HttpMethod.GET, "/ws/*").handler(this::websocketProxyHandler);
    router.route().handler(this::httpProxyHandler);
  }

  void httpProxyHandler(final RoutingContext routingContext) {

    final HttpServerRequest serverRequest = routingContext.request();
    final HttpClientRequest clientRequest = proxyClient.request(serverRequest.method(),
        proxyTarget(), proxyTarget.port(), proxyTarget.host(),
        serverRequest.path(),
        resp -> responseHandler(resp, serverRequest))
        .exceptionHandler(ProxyHandler::exceptionHandler);

    clientRequest.headers().setAll(serverRequest.headers());

    if (serverRequest.isEnded()) {
      clientRequest.end();
    } else {
      Pipe.pipe(serverRequest, clientRequest);
    }
  }

  void websocketProxyHandler(final RoutingContext routingContext) {
    final HttpServerRequest request = routingContext.request();
    final WebSocketConnectOptions options = new WebSocketConnectOptions()
        .setHost(proxyTarget().host())
        .setPort(proxyTarget().port())
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
          request.response().setStatusCode(HttpStatus.SC_INTERNAL_SERVER_ERROR).end();
        }
      }
    });
  }

  private SocketAddress proxyTarget() {
    if (proxyTarget == null) {
      proxyTarget = server.getProxyTarget();
    }
    return proxyTarget;
  }

  private static void responseHandler(final HttpClientResponse clientResponse,
      final HttpServerRequest serverRequest) {
    // If the target server closes the connection we need to close ours too
    clientResponse.request().connection().closeHandler(v -> serverRequest.connection().close());

    final HttpServerResponse serverResponse = serverRequest.response();
    serverResponse.setStatusCode(clientResponse.statusCode());
    serverResponse.setStatusMessage(clientResponse.statusMessage());
    serverResponse.headers().setAll(clientResponse.headers());

    // We do our own pipe as we need to intercept end to add any trailers
    Pipe.pipe(clientResponse, serverResponse, () -> {
      final MultiMap trailers = clientResponse.trailers();
      serverResponse.trailers().setAll(trailers);
    }, () -> checkFirstWrite(clientResponse, serverResponse));
  }

  // Vert.x requires that content-length is set if not chunked but Jetty sometimes sends
  // response with no content-length and not chunked, so we workaround this by adding a
  // tranfer encoding response header with chunked in this case
  private static void checkFirstWrite(final HttpClientResponse clientResponse,
      final HttpServerResponse serverResponse) {
    if (clientResponse.getHeader("content-length") == null
        && clientResponse.getHeader("transfer-encoding") == null) {
      serverResponse.putHeader("transfer-encoding", "chunked");
    }
  }

  private static void exceptionHandler(final Throwable t) {
    log.error("Exception in making proxy request", t);
  }

  private static final class Pipe {

    private final ReadStream<Buffer> from;
    private final WriteStream<Buffer> to;
    private boolean drainHandlerSet;
    private final Runnable beforeEndHook;
    private Runnable beforeFirstWriteHook;


    private static void pipe(final ReadStream<Buffer> from, final WriteStream<Buffer> to) {
      new Pipe(from, to, null, null);
    }

    private static void pipe(final ReadStream<Buffer> from, final WriteStream<Buffer> to,
        final Runnable beforeEndHook, final Runnable beforeWriteHook) {
      new Pipe(from, to, beforeEndHook, beforeWriteHook);
    }

    private Pipe(final ReadStream<Buffer> from, final WriteStream<Buffer> to,
        final Runnable beforeEndHook, final Runnable beforeFirstWriteHook) {
      this.from = from;
      this.to = to;
      this.beforeEndHook = beforeEndHook;
      this.beforeFirstWriteHook = beforeFirstWriteHook;
      from.endHandler(this::requestEnded);
      from.handler(this::handler);
      from.exceptionHandler(this::exceptionHandler);
      to.exceptionHandler(this::exceptionHandler);
    }

    private void handler(final Buffer buffer) {
      if (beforeFirstWriteHook != null) {
        beforeFirstWriteHook.run();
        beforeFirstWriteHook = null;
      }
      to.write(buffer);
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

    private void requestEnded(final Void v) {
      if (beforeEndHook != null) {
        beforeEndHook.run();
      }
      to.end();
    }

    private void exceptionHandler(final Throwable t) {
      log.error("Exception in proxying", t);
    }

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
      try {
        to.close();
      } catch (Exception ignore) {
        // Ignore
      }
      try {
        from.close();
      } catch (Exception ignore) {
        // Ignore
      }

    }
  }

}
