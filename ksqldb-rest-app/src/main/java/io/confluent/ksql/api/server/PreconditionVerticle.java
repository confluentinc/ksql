/*
 * Copyright 2022 Confluent Inc.
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


import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.api.util.ApiServerUtils;
import io.confluent.ksql.rest.server.state.ServerState;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.ext.web.Router;
import java.util.Objects;

/**
 * The server deploys multiple server verticles. This is where the HTTP2 requests are handled. The
 * actual implementation of the endpoints is provided by an implementation of {@code Endpoints}.
 */
public class PreconditionVerticle extends AbstractVerticle {
  private final HttpServerOptions httpServerOptions;
  private final ServerState serverState;
  private HttpServer httpServer;

  @SuppressFBWarnings(value = "EI_EXPOSE_REP2")
  public PreconditionVerticle(
      final HttpServerOptions httpServerOptions,
      final ServerState serverState
  ) {
    this.httpServerOptions = Objects.requireNonNull(httpServerOptions);
    this.serverState = Objects.requireNonNull(serverState, "serverState");
  }

  @Override
  public void start(final Promise<Void> startPromise) {
    httpServer = vertx.createHttpServer(httpServerOptions).requestHandler(setupRouter())
        .exceptionHandler(ApiServerUtils::unhandledExceptionHandler);
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
    if (httpServer == null) {
      stopPromise.complete();
    } else {
      httpServer.close(ar -> stopPromise.complete());
    }
  }

  private Router setupRouter() {
    final Router router = Router.router(vertx);
    router.route(HttpMethod.GET, "/chc/ready").handler(ApiServerUtils::chcHandler);
    router.route(HttpMethod.GET, "/chc/live").handler(ApiServerUtils::chcHandler);
    router.route().handler(new ServerStateHandler(serverState));
    router.route().failureHandler(new FailureHandler());
    return router;
  }
}
