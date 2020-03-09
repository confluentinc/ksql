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

import io.confluent.ksql.api.auth.ApiServerConfig;
import io.confluent.ksql.api.auth.JaasAuthProvider;
import io.confluent.ksql.api.auth.KsqlAuthorizationFilter;
import io.confluent.ksql.api.spi.Endpoints;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.ext.auth.AuthProvider;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.AuthHandler;
import io.vertx.ext.web.handler.BasicAuthHandler;
import io.vertx.ext.web.handler.BodyHandler;
import java.util.Objects;
import java.util.Optional;
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

    if (proxyHandler != null) {
      proxyHandler.setupRoutes(router);
    }

    return router;
  }

  private static void handleFailure(final RoutingContext routingContext) {
    if (routingContext.failure() != null) {
      log.error(String.format("Failed to handle request %d %s", routingContext.statusCode(),
          routingContext.request().path()),
          routingContext.failure());
      ServerUtils.handleError(
          routingContext.response(),
          routingContext.statusCode(),
          routingContext.statusCode(),
          routingContext.failure().getMessage()
      );
    } else {
      routingContext.response().setStatusCode(routingContext.statusCode()).end();
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
    return BasicAuthHandler.create(authProvider, realm);
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
