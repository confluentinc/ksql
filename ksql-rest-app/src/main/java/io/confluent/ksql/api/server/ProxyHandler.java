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

import io.vertx.core.Handler;
import io.vertx.core.MultiMap;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientRequest;
import io.vertx.core.http.HttpClientResponse;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.net.SocketAddress;
import io.vertx.core.streams.ReadStream;
import io.vertx.core.streams.WriteStream;
import io.vertx.ext.web.RoutingContext;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ProxyHandler implements Handler<RoutingContext> {

  private static final Logger log = LoggerFactory.getLogger(ProxyHandler.class);

  private final HttpClient proxyClient;
  private final Server server;
  private SocketAddress proxyTarget;

  public ProxyHandler(final SocketAddress proxyTarget, final HttpClient proxyClient,
      final Server server) {
    this.proxyTarget = proxyTarget;
    this.proxyClient = proxyClient;
    this.server = server;
  }

  @Override
  public void handle(final RoutingContext routingContext) {
    if (proxyTarget == null) {
      proxyTarget = server.getProxyTarget();
    }
    final HttpServerRequest serverRequest = routingContext.request();
    final HttpClientRequest clientRequest = proxyClient.request(serverRequest.method(),
        proxyTarget, proxyTarget.port(), proxyTarget.host(),
        serverRequest.path(),
        resp -> responseHandler(resp, serverRequest))
        .exceptionHandler(ProxyHandler::exceptionHandler);

    final MultiMap headers = MultiMap.caseInsensitiveMultiMap();
    for (Map.Entry<String, String> header : serverRequest.headers()) {
      if (!header.getKey().equalsIgnoreCase("host")) {
        headers.add(header.getKey(), header.getValue());
      }
    }
    clientRequest.headers().setAll(headers);

    if (serverRequest.isEnded()) {
      clientRequest.end();
    } else {
      Pipe.pipe(serverRequest, clientRequest);
    }
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
    });
  }

  private static void exceptionHandler(final Throwable t) {
    log.error("Exception in making proxy request", t);
  }

  private static final class Pipe {

    private final ReadStream<Buffer> from;
    private final WriteStream<Buffer> to;
    private boolean drainHandlerSet;
    private final Runnable beforeEndHook;

    private static void pipe(final ReadStream<Buffer> from, final WriteStream<Buffer> to) {
      new Pipe(from, to, null);
    }

    private static void pipe(final ReadStream<Buffer> from, final WriteStream<Buffer> to,
        final Runnable beforeEndHook) {
      new Pipe(from, to, beforeEndHook);
    }

    private Pipe(final ReadStream<Buffer> from, final WriteStream<Buffer> to,
        final Runnable beforeEndHook) {
      this.from = from;
      this.to = to;
      this.beforeEndHook = beforeEndHook;
      from.endHandler(this::requestEnded);
      from.handler(this::handler);
      from.exceptionHandler(this::exceptionHandler);
      to.exceptionHandler(this::exceptionHandler);
    }

    private void handler(final Buffer buffer) {
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

}
