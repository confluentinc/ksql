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

import static io.confluent.ksql.rest.Errors.toErrorCode;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableSet;
import io.confluent.ksql.api.auth.ApiServerConfig;
import io.confluent.ksql.api.auth.DefaultApiSecurityContext;
import io.confluent.ksql.api.auth.JaasAuthProvider;
import io.confluent.ksql.api.auth.KsqlAuthorizationFilter;
import io.confluent.ksql.api.spi.EndpointResponse;
import io.confluent.ksql.api.spi.Endpoints;
import io.confluent.ksql.json.JsonMapper;
import io.confluent.ksql.rest.entity.KsqlErrorMessage;
import io.confluent.ksql.rest.entity.KsqlRequest;
import io.confluent.ksql.rest.entity.Versions;
import io.confluent.ksql.util.KsqlException;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.ext.auth.AuthProvider;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.AuthHandler;
import io.vertx.ext.web.handler.BasicAuthHandler;
import io.vertx.ext.web.handler.BodyHandler;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import javax.ws.rs.core.MediaType;
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
  private static final Set<String> OLD_API_ENDPOINTS = ImmutableSet.of("/ksql");

  // Disabled for now, as there is some security work to do first
  public static volatile boolean HOST_OLD_ENDPOINTS = false;

  private final Endpoints endpoints;
  private final HttpServerOptions httpServerOptions;
  private final Server server;
  private final ProxyHandler proxyHandler;
  private ConnectionQueryManager connectionQueryManager;
  private volatile HttpServer httpServer;

  public ServerVerticle(final Endpoints endpoints, final HttpServerOptions httpServerOptions,
      final Server server, final boolean proxyEnabled) {
    this.endpoints = Objects.requireNonNull(endpoints);
    this.httpServerOptions = Objects.requireNonNull(httpServerOptions);
    this.server = Objects.requireNonNull(server);
    if (proxyEnabled) {
      this.proxyHandler = new ProxyHandler(server);
    } else {
      this.proxyHandler = null;
    }
  }

  @Override
  public void start(final Promise<Void> startPromise) {

    this.connectionQueryManager = new ConnectionQueryManager(context, server);
    httpServer = vertx.createHttpServer(httpServerOptions).requestHandler(setupRouter())
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
    if (proxyHandler != null) {
      proxyHandler.close();
    }
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

    router.route().failureHandler(ServerVerticle::handleFailure);

    getAuthHandler(server).ifPresent(authHandler -> {
      router.route().handler(ServerVerticle::pauseHandler);
      router.route().handler(authHandler);
      server
          .getSecurityExtension()
          .getAuthorizationProvider()
          .ifPresent(ksqlAuthorizationProvider -> router.route()
              .handler(new KsqlAuthorizationFilter(server.getWorkerExecutor(),
                  ksqlAuthorizationProvider)));
      router.route().handler(ServerVerticle::resumeHandler);
    });

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

    if (HOST_OLD_ENDPOINTS) {
      router.route(HttpMethod.POST, "/ksql")
          .handler(BodyHandler.create())
          .produces(Versions.KSQL_V1_JSON)
          .produces(MediaType.APPLICATION_JSON)
          .handler(this::handleKsqlRequests);
    }

    if (proxyHandler != null) {
      proxyHandler.setupRoutes(router);
    }

    return router;
  }

  private void handleKsqlRequests(final RoutingContext routingContext) {
    final HttpServerResponse response = routingContext.response();
    final ObjectMapper objectMapper = JsonMapper.INSTANCE.mapper;
    final KsqlRequest ksqlRequest;
    try {
      ksqlRequest = objectMapper.readValue(routingContext.getBody().getBytes(), KsqlRequest.class);
    } catch (Exception e) {
      handleException(response, "Failed to deserialise request", e);
      return;
    }
    final CompletableFuture<EndpointResponse> completableFuture = endpoints
        .executeKsqlRequest(ksqlRequest, server.getWorkerExecutor(),
            DefaultApiSecurityContext.create(routingContext));
    completableFuture.thenAccept(endpointResponse -> {

      final Buffer responseBody;
      try {
        final byte[] bytes = objectMapper.writeValueAsBytes(endpointResponse.getResponseBody());
        responseBody = Buffer.buffer(bytes);
      } catch (JsonProcessingException e) {
        handleException(response, "Failed to serialize response", e);
        return;
      }

      response.setStatusCode(endpointResponse.getStatusCode())
          .setStatusMessage(endpointResponse.getStatusMessage())
          .end(responseBody);

    }).exceptionally(t -> {
      log.error("Failed to execute ksql request", t);
      routingContext.response().setStatusCode(500).end();
      return null;
    });
  }

  private static void handleException(final HttpServerResponse response, final String message,
      final Exception e) {
    log.error(message, e);
    response.setStatusCode(500).end();
  }

  private static void handleFailure(final RoutingContext routingContext) {
    if (routingContext.failure() != null) {
      log.error(String.format("Failed to handle request %d %s", routingContext.statusCode(),
          routingContext.request().path()),
          routingContext.failure());
    }

    final int statusCode = routingContext.statusCode();

    if (OLD_API_ENDPOINTS.contains(routingContext.normalisedPath())) {
      final KsqlErrorMessage ksqlErrorMessage = new KsqlErrorMessage(
          toErrorCode(statusCode),
          routingContext.failure().getMessage());
      try {
        final byte[] bytes = JsonMapper.INSTANCE.mapper.writeValueAsBytes(ksqlErrorMessage);
        routingContext.response().setStatusCode(statusCode)
            .end(Buffer.buffer(bytes));
      } catch (JsonProcessingException e) {
        throw new KsqlException(e);
      }
    } else {
      // DOTO do we need to send our own proper error responses in the body?
      routingContext.response().setStatusCode(statusCode).end();
    }
  }

  private static Optional<AuthHandler> getAuthHandler(final Server server) {
    final String authMethod = server.getConfig()
        .getString(ApiServerConfig.AUTHENTICATION_METHOD_CONFIG);
    switch (authMethod) {
      case ApiServerConfig.AUTHENTICATION_METHOD_BASIC:
        return Optional.of(basicAuthHandler(server));
      case ApiServerConfig.AUTHENTICATION_METHOD_NONE:
        return Optional.empty();
      default:
        throw new IllegalStateException(String.format(
            "Unexpected value for %s: %s",
            ApiServerConfig.AUTHENTICATION_METHOD_CONFIG,
            authMethod
        ));
    }
  }

  private static AuthHandler basicAuthHandler(final Server server) {
    final AuthProvider authProvider = new JaasAuthProvider(server, server.getConfig());
    final String realm = server.getConfig().getString(ApiServerConfig.AUTHENTICATION_REALM_CONFIG);
    final AuthHandler basicAuthHandler = BasicAuthHandler.create(authProvider, realm);
    // It doesn't matter what we set here as we actually do the authorisation at the
    // authentication stage and cache the result, but we must add an authority or
    // no authorisation will be done
    basicAuthHandler.addAuthority("ksql");
    return basicAuthHandler;
  }

  private static void pauseHandler(final RoutingContext routingContext) {
    // prevent auth handler from reading request body
    routingContext.request().pause();
    routingContext.next();
  }

  private static void resumeHandler(final RoutingContext routingContext) {
    // Un-pause body handling as async auth provider calls have completed by this point
    routingContext.request().resume();
    routingContext.next();
  }

}
