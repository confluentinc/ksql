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

import static io.confluent.ksql.api.server.InternalEndpointHandler.CONTEXT_DATA_IS_INTERNAL;
import static io.confluent.ksql.api.server.OldApiUtils.handleOldApiRequest;
import static io.netty.handler.codec.http.HttpResponseStatus.TEMPORARY_REDIRECT;

import io.confluent.ksql.api.auth.ApiSecurityContext;
import io.confluent.ksql.api.auth.DefaultApiSecurityContext;
import io.confluent.ksql.api.spi.Endpoints;
import io.confluent.ksql.rest.entity.ClusterTerminateRequest;
import io.confluent.ksql.rest.entity.HeartbeatMessage;
import io.confluent.ksql.rest.entity.KsqlRequest;
import io.confluent.ksql.rest.entity.LagReportingMessage;
import io.confluent.ksql.rest.entity.Versions;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.ServerWebSocket;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import java.nio.channels.ClosedChannelException;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The server deploys multiple server verticles. This is where the HTTP2 requests are handled. The
 * actual implementation of the endpoints is provided by an implementation of {@code Endpoints}.
 */
// CHECKSTYLE_RULES.OFF: ClassDataAbstractionCoupling
public class ServerVerticle extends AbstractVerticle {

  // CHECKSTYLE_RULES.ON: ClassDataAbstractionCoupling
  private static final Logger log = LoggerFactory.getLogger(ServerVerticle.class);

  private static final String JSON_CONTENT_TYPE = "application/json";
  private static final String DELIMITED_CONTENT_TYPE = "application/vnd.ksqlapi.delimited.v1";

  private final Endpoints endpoints;
  private final HttpServerOptions httpServerOptions;
  private final Server server;
  private ConnectionQueryManager connectionQueryManager;
  private HttpServer httpServer;
  private final Optional<Boolean> isInternalListener;

  public ServerVerticle(
      final Endpoints endpoints,
      final HttpServerOptions httpServerOptions,
      final Server server,
      final Optional<Boolean> isInternalListener) {
    this.endpoints = Objects.requireNonNull(endpoints);
    this.httpServerOptions = Objects.requireNonNull(httpServerOptions);
    this.server = Objects.requireNonNull(server);
    this.isInternalListener = Objects.requireNonNull(isInternalListener);
  }

  @Override
  public void start(final Promise<Void> startPromise) {
    this.connectionQueryManager = new ConnectionQueryManager(context, server);
    httpServer = vertx.createHttpServer(httpServerOptions).requestHandler(setupRouter())
        .exceptionHandler(ServerVerticle::unhandledExceptionHandler);;
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
      httpServer.close(stopPromise.future());
    }
  }

  int actualPort() {
    return httpServer.actualPort();
  }

  private Router setupRouter() {
    final Router router = Router.router(vertx);

    KsqlCorsHandler.setupCorsHandler(server, router);

    // /chc endpoints need to be before server state handler but after CORS handler as they
    // need to be usable from browser with cross origin policy
    router.route(HttpMethod.GET, "/chc/ready").handler(ServerVerticle::chcHandler);
    router.route(HttpMethod.GET, "/chc/live").handler(ServerVerticle::chcHandler);

    router.route().failureHandler(new FailureHandler());

    isInternalListener.ifPresent(isInternal ->
        router.route().handler(new InternalEndpointHandler(isInternal)));

    AuthHandlers.setupAuthHandlers(server, router, isInternalListener.orElse(false));

    router.route().handler(new ServerStateHandler(server.getServerState()));

    // The new query and insert streaming API
    // --------------------------------------

    router.route(HttpMethod.POST, "/query-stream")
        .produces(DELIMITED_CONTENT_TYPE)
        .produces(JSON_CONTENT_TYPE)
        .handler(BodyHandler.create())
        .handler(new QueryStreamHandler(endpoints, connectionQueryManager, context, server));
    router.route(HttpMethod.POST, "/inserts-stream")
        .produces(DELIMITED_CONTENT_TYPE)
        .produces(JSON_CONTENT_TYPE)
        .handler(new InsertsStreamHandler(context, endpoints, server.getWorkerExecutor()));
    router.route(HttpMethod.POST, "/close-query")
        .handler(BodyHandler.create())
        .handler(new CloseQueryHandler(server));

    // The old API which we continue to support as-is
    // ----------------------------------------------

    router.route(HttpMethod.GET, "/")
        .handler(this::handleInfoRedirect);
    router.route(HttpMethod.POST, "/ksql")
        .handler(BodyHandler.create())
        .produces(Versions.KSQL_V1_JSON)
        .produces(JSON_CONTENT_TYPE)
        .handler(this::handleKsqlRequest);
    router.route(HttpMethod.POST, "/ksql/terminate")
        .handler(BodyHandler.create())
        .produces(Versions.KSQL_V1_JSON)
        .produces(JSON_CONTENT_TYPE)
        .handler(this::handleTerminateRequest);
    router.route(HttpMethod.POST, "/query")
        .handler(BodyHandler.create())
        .produces(Versions.KSQL_V1_JSON)
        .produces(JSON_CONTENT_TYPE)
        .handler(this::handleQueryRequest);
    router.route(HttpMethod.GET, "/info")
        .produces(Versions.KSQL_V1_JSON)
        .produces(JSON_CONTENT_TYPE)
        .handler(this::handleInfoRequest);
    router.route(HttpMethod.POST, "/heartbeat")
        .handler(BodyHandler.create())
        .produces(Versions.KSQL_V1_JSON)
        .produces(JSON_CONTENT_TYPE)
        .handler(this::handleHeartbeatRequest);
    router.route(HttpMethod.GET, "/clusterStatus")
        .produces(Versions.KSQL_V1_JSON)
        .produces(JSON_CONTENT_TYPE)
        .handler(this::handleClusterStatusRequest);
    router.route(HttpMethod.GET, "/status/:type/:entity/:action")
        .produces(Versions.KSQL_V1_JSON)
        .produces(JSON_CONTENT_TYPE)
        .handler(this::handleStatusRequest);
    router.route(HttpMethod.GET, "/status")
        .produces(Versions.KSQL_V1_JSON)
        .produces(JSON_CONTENT_TYPE)
        .handler(this::handleAllStatusesRequest);
    router.route(HttpMethod.POST, "/lag")
        .handler(BodyHandler.create())
        .produces(Versions.KSQL_V1_JSON)
        .produces(JSON_CONTENT_TYPE)
        .handler(this::handleLagReportRequest);
    router.route(HttpMethod.GET, "/healthcheck")
        .produces(Versions.KSQL_V1_JSON)
        .produces(JSON_CONTENT_TYPE)
        .handler(this::handleHealthcheckRequest);
    router.route(HttpMethod.GET, "/v1/metadata")
        .produces(Versions.KSQL_V1_JSON)
        .produces(JSON_CONTENT_TYPE)
        .handler(this::handleServerMetadataRequest);
    router.route(HttpMethod.GET, "/v1/metadata/id")
        .produces(Versions.KSQL_V1_JSON)
        .produces(JSON_CONTENT_TYPE)
        .handler(this::handleServerMetadataClusterIdRequest);
    router.route(HttpMethod.GET, "/ws/query")
        .produces(Versions.KSQL_V1_JSON)
        .produces(JSON_CONTENT_TYPE)
        .handler(this::handleWebsocket);

    return router;
  }

  private void handleKsqlRequest(final RoutingContext routingContext) {
    handleOldApiRequest(server, routingContext, KsqlRequest.class,
        (ksqlRequest, apiSecurityContext) ->
            endpoints
                .executeKsqlRequest(ksqlRequest, server.getWorkerExecutor(),
                    DefaultApiSecurityContext.create(routingContext))
    );
  }

  private void handleTerminateRequest(final RoutingContext routingContext) {
    handleOldApiRequest(server, routingContext, ClusterTerminateRequest.class,
        (request, apiSecurityContext) ->
            endpoints
                .executeTerminate(request, server.getWorkerExecutor(),
                    DefaultApiSecurityContext.create(routingContext))
    );
  }

  private void handleQueryRequest(final RoutingContext routingContext) {

    final CompletableFuture<Void> connectionClosedFuture = new CompletableFuture<>();
    routingContext.request().connection().closeHandler(v -> connectionClosedFuture.complete(null));

    handleOldApiRequest(server, routingContext, KsqlRequest.class,
        (request, apiSecurityContext) ->
            endpoints
                .executeQueryRequest(request, server.getWorkerExecutor(), connectionClosedFuture,
                    DefaultApiSecurityContext.create(routingContext),
                    isInternalRequest(routingContext))
    );
  }

  private void handleInfoRequest(final RoutingContext routingContext) {
    handleOldApiRequest(server, routingContext, null,
        (request, apiSecurityContext) ->
            endpoints.executeInfo(DefaultApiSecurityContext.create(routingContext))
    );
  }

  private void handleClusterStatusRequest(final RoutingContext routingContext) {
    handleOldApiRequest(server, routingContext, null,
        (request, apiSecurityContext) ->
            endpoints.executeClusterStatus(DefaultApiSecurityContext.create(routingContext))
    );
  }

  private void handleHeartbeatRequest(final RoutingContext routingContext) {
    handleOldApiRequest(server, routingContext, HeartbeatMessage.class,
        (request, apiSecurityContext) ->
            endpoints.executeHeartbeat(request, DefaultApiSecurityContext.create(routingContext))
    );
  }

  private void handleStatusRequest(final RoutingContext routingContext) {
    final HttpServerRequest request = routingContext.request();
    final String type = request.getParam("type");
    final String entity = request.getParam("entity");
    final String action = request.getParam("action");
    handleOldApiRequest(server, routingContext, null,
        (r, apiSecurityContext) ->
            endpoints.executeStatus(type, entity, action,
                DefaultApiSecurityContext.create(routingContext))
    );
  }

  private void handleAllStatusesRequest(final RoutingContext routingContext) {
    handleOldApiRequest(server, routingContext, null,
        (r, apiSecurityContext) ->
            endpoints.executeAllStatuses(DefaultApiSecurityContext.create(routingContext))
    );
  }

  private void handleLagReportRequest(final RoutingContext routingContext) {
    handleOldApiRequest(server, routingContext, LagReportingMessage.class,
        (request, apiSecurityContext) ->
            endpoints.executeLagReport(request, DefaultApiSecurityContext.create(routingContext))
    );
  }

  private void handleHealthcheckRequest(final RoutingContext routingContext) {
    handleOldApiRequest(server, routingContext, null,
        (request, apiSecurityContext) ->
            endpoints.executeCheckHealth(DefaultApiSecurityContext.create(routingContext))
    );
  }

  private void handleServerMetadataRequest(final RoutingContext routingContext) {
    handleOldApiRequest(server, routingContext, null,
        (request, apiSecurityContext) ->
            endpoints.executeServerMetadata(DefaultApiSecurityContext.create(routingContext))
    );
  }

  private void handleServerMetadataClusterIdRequest(final RoutingContext routingContext) {
    handleOldApiRequest(server, routingContext, null,
        (request, apiSecurityContext) ->
            endpoints
                .executeServerMetadataClusterId(DefaultApiSecurityContext.create(routingContext))
    );
  }

  private void handleInfoRedirect(final RoutingContext routingContext) {
    // We redirect to the /info endpoint.
    // (This preserves behaviour of the old API)
    routingContext.response().putHeader("location", "/info")
        .setStatusCode(TEMPORARY_REDIRECT.code()).end();
  }

  private void handleWebsocket(final RoutingContext routingContext) {
    final ApiSecurityContext apiSecurityContext = DefaultApiSecurityContext.create(routingContext);
    final ServerWebSocket serverWebSocket = routingContext.request().upgrade();
    endpoints
        .executeWebsocketStream(serverWebSocket, routingContext.request().params(),
            server.getWorkerExecutor(), apiSecurityContext);
  }

  private static void chcHandler(final RoutingContext routingContext) {
    routingContext.response().putHeader(HttpHeaders.CONTENT_TYPE.toString(), "application/json")
        .end(new JsonObject().toBuffer());
  }

  private static void unhandledExceptionHandler(final Throwable t) {
    if (t instanceof ClosedChannelException) {
      log.debug("Unhandled ClosedChannelException (connection likely closed early)", t);
    } else {
      log.error("Unhandled exception", t);
    }
  }

  /**
   * If the request was received on the internal listener.
   *
   * @return If an internal listener is in use and this is an internal request, or
   * {@code Optional.empty} if an internal listener is not enabled.
   */
  private static Optional<Boolean> isInternalRequest(final RoutingContext routingContext) {
    return Optional.ofNullable(routingContext.get(CONTEXT_DATA_IS_INTERNAL));
  }
}
