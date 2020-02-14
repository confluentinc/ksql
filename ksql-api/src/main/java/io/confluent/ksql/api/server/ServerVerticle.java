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

import io.confluent.ksql.api.impl.Utils;
import io.confluent.ksql.api.spi.Endpoints;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.WorkerExecutor;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.BodyHandler;
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
  private final WorkerExecutor workerExecutor;
  private ConnectionQueryManager connectionQueryManager;
  private HttpServer httpServer;

  public ServerVerticle(final Endpoints endpoints, final HttpServerOptions httpServerOptions,
      final Server server, final WorkerExecutor workerExecutor) {
    this.endpoints = endpoints;
    this.httpServerOptions = httpServerOptions;
    this.server = server;
    this.workerExecutor = workerExecutor;
  }

  @Override
  public void start(final Promise<Void> startPromise) {
    this.connectionQueryManager = new ConnectionQueryManager(context, server);
    httpServer = vertx.createHttpServer(httpServerOptions).requestHandler(setupRouter())
        .exceptionHandler(ServerUtils::unhandledExceptonHandler);
    final Future<HttpServer> listenFuture = Promise.<HttpServer>promise().future();
    Utils.connectPromise(listenFuture.map(s -> null), startPromise);
    httpServer.listen(listenFuture);
    vertx.getOrCreateContext().exceptionHandler(ServerUtils::unhandledExceptonHandler);
  }

  @Override
  public void stop(final Promise<Void> stopPromise) {
    if (httpServer == null) {
      stopPromise.complete();
    } else {
      httpServer.close(stopPromise.future());
    }
  }

  private Router setupRouter() {
    final Router router = Router.router(vertx);
    router.route(HttpMethod.POST, "/query-stream")
        .produces("application/vnd.ksqlapi.delimited.v1")
        .produces("application/json")
        .handler(BodyHandler.create())
        .handler(new QueryStreamHandler(endpoints, connectionQueryManager, context,
            workerExecutor));
    router.route(HttpMethod.POST, "/inserts-stream")
        .produces("application/vnd.ksqlapi.delimited.v1")
        .produces("application/json")
        .handler(new InsertsStreamHandler(context, endpoints, workerExecutor));
    router.route(HttpMethod.POST, "/close-query").handler(BodyHandler.create())
        .handler(new CloseQueryHandler(server));
    return router;
  }
}
