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

  private final SocketAddress socketAddress;
  private final HttpClient proxyClient;

  public ProxyHandler(final SocketAddress socketAddress, final HttpClient proxyClient) {
    this.socketAddress = socketAddress;
    this.proxyClient = proxyClient;
  }

  @Override
  public void handle(final RoutingContext routingContext) {
    final HttpServerRequest serverRequest = routingContext.request();
    final HttpClientRequest clientRequest = proxyClient.request(serverRequest.method(),
        socketAddress, socketAddress.port(), socketAddress.host(),
        serverRequest.path(),
        resp -> responseHandler(resp, serverRequest.response()))
        .exceptionHandler(this::exceptionHandler);
    final MultiMap headers = MultiMap.caseInsensitiveMultiMap();
    for (Map.Entry<String, String> header : serverRequest.headers()) {
      if (!header.getKey().equalsIgnoreCase("host")) {
        headers.add(header.getKey(), header.getValue());
      }
    }
    clientRequest.headers().setAll(headers);
    serverRequest.pipeTo(clientRequest);
  }

  private void responseHandler(final HttpClientResponse clientResponse,
      final HttpServerResponse serverResponse) {
    // We do our own pipe as we need to intercept end to add any trailers
    ResponsePipe.pipe(clientResponse, serverResponse);
  }

  private void exceptionHandler(final Throwable t) {
    log.error("Exception in making proxy request", t);
  }

  private static class ResponsePipe {

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
      from.bodyHandler(this::bodyHandler);
    }

    private void bodyHandler(final Buffer buffer) {
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
