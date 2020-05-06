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
import io.confluent.ksql.api.spi.InternalEndpoints;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import java.util.Objects;
import java.util.Optional;

/**
 * The server deploys multiple server verticles. This is where the HTTP2 requests are handled. The
 * actual implementation of the endpoints is provided by an implementation of {@code Endpoints}
 * and {@code InternalEndpoints}.
 */
public class ServerVerticle extends AbstractServerVerticle {
  private static final boolean EXCLUDE_SHARED_ENDPOINTS = false;

  private final Endpoints endpoints;
  private final Optional<InternalEndpoints> internalEndpoints;

  public ServerVerticle(
      final Endpoints endpoints,
      final Optional<InternalEndpoints> internalEndpoints,
      final HttpServerOptions httpServerOptions,
      final Server server) {
    super(httpServerOptions, server);
    this.endpoints = Objects.requireNonNull(endpoints);
    this.internalEndpoints = Objects.requireNonNull(internalEndpoints);;
  }

  protected Router setupRouter() {
    final Router router = Router.router(vertx);

    KsqlCorsHandler.setupCorsHandler(server, router);

    // /chc endpoints need to be before server state handler but after CORS handler as they
    // need to be usable from browser with cross origin policy
    router.route(HttpMethod.GET, "/chc/ready").handler(ServerVerticle::chcHandler);
    router.route(HttpMethod.GET, "/chc/live").handler(ServerVerticle::chcHandler);

    PortedEndpoints.setupFailureHandler(router);
    internalEndpoints.ifPresent(ie ->
        PortedEndpoints.setupFailureHandlerInternal(router, EXCLUDE_SHARED_ENDPOINTS));

    router.route().failureHandler(RequestFailureHandler::handleFailure);

    RequestAuthenticationHandler.setupAuthHandlers(server, router);

    router.route().handler(new ServerStateHandler(server.getServerState()));

    router.route(HttpMethod.POST, "/query-stream")
        .produces("application/vnd.ksqlapi.delimited.v1")
        .produces("application/json")
        .handler(BodyHandler.create())
        .handler(new QueryStreamHandler(endpoints, connectionQueryManager, context, server));
    router.route(HttpMethod.POST, "/inserts-stream")
        .produces("application/vnd.ksqlapi.delimited.v1")
        .produces("application/json")
        .handler(new InsertsStreamHandler(context, endpoints, server.getWorkerExecutor()));
    router.route(HttpMethod.POST, "/close-query")
        .handler(BodyHandler.create())
        .handler(new CloseQueryHandler(server));

    PortedEndpoints.setupEndpoints(endpoints, server, router);
    internalEndpoints.ifPresent(ie ->
        PortedEndpoints.setupEndpointsInternal(ie, server, router, EXCLUDE_SHARED_ENDPOINTS));

    return router;
  }

  private static void chcHandler(final RoutingContext routingContext) {
    routingContext.response().putHeader(HttpHeaders.CONTENT_TYPE.toString(), "application/json")
        .end(new JsonObject().toBuffer());
  }
}
