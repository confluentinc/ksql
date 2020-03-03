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
import io.vertx.ext.web.RoutingContext;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ProxyHandler implements Handler<RoutingContext> {

  private static final Logger log = LoggerFactory.getLogger(ProxyHandler.class);

  private SocketAddress proxyTarget;
  private final HttpClient proxyClient;
  private final Server server;

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
        .exceptionHandler(this::exceptionHandler);
    RequestPipe.pipe(serverRequest, clientRequest);
  }

  private void responseHandler(final HttpClientResponse clientResponse,
      final HttpServerRequest serverRequest) {
    // If the target server closes the connection we need to close ours too
    clientResponse.request().connection().closeHandler(v -> serverRequest.connection().close());
    // We do our own pipe as we need to intercept end to add any trailers
    ResponsePipe.pipe(clientResponse, serverRequest.response());
  }

  private void exceptionHandler(final Throwable t) {
    log.error("Exception in making proxy request", t);
  }

  private static final class RequestPipe {

    private final HttpServerRequest from;
    private final HttpClientRequest to;
    private boolean drainHandlerSet;

    private static void pipe(final HttpServerRequest from, final HttpClientRequest to) {
      new RequestPipe(from, to);
    }

    private RequestPipe(final HttpServerRequest from, final HttpClientRequest to) {
      this.from = from;
      this.to = to;
      final MultiMap headers = MultiMap.caseInsensitiveMultiMap();
      for (Map.Entry<String, String> header : from.headers()) {
        if (!header.getKey().equalsIgnoreCase("host")) {
          headers.add(header.getKey(), header.getValue());
        }
      }
      to.headers().setAll(headers);

      from.exceptionHandler(this::exceptionHandler);
      if (from.isEnded()) {
        to.end();
      } else {
        from.endHandler(this::requestEnded);
        from.handler(this::handler);
      }

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
      to.end();
    }

    private void exceptionHandler(final Throwable t) {
      log.error("Exception in proxy request", t);
    }

  }

  private static final class ResponsePipe {

    private final HttpClientResponse from;
    private final HttpServerResponse to;
    private boolean drainHandlerSet;

    private static void pipe(final HttpClientResponse from, final HttpServerResponse to) {
      new ResponsePipe(from, to);
    }

    private ResponsePipe(final HttpClientResponse from, final HttpServerResponse to) {
      this.from = from;
      this.to = to;
      to.setStatusCode(from.statusCode());
      to.setStatusMessage(from.statusMessage());
      to.headers().setAll(from.headers());

      from.exceptionHandler(this::exceptionHandler);
      from.endHandler(this::responseEnded);
      from.handler(this::handler);
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

    private void responseEnded(final Void v) {
      final MultiMap trailers = from.trailers();
      to.trailers().setAll(trailers);
      to.end();
    }

    private void exceptionHandler(final Throwable t) {
      log.error("Exception in proxy response", t);
    }

  }

}
