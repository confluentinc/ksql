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

import com.google.common.collect.ImmutableSet;
import io.confluent.ksql.api.auth.ApiServerConfig;
import io.confluent.ksql.api.auth.AuthenticationPlugin;
import io.confluent.ksql.api.auth.AuthorizationPlugin;
import io.confluent.ksql.api.auth.JaasAuthProvider;
import io.confluent.ksql.api.spi.Endpoints;
import io.confluent.ksql.security.KsqlSecurityExtension;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Handler;
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
import java.security.Principal;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
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

  private static final Set<String> NEW_API_ENDPOINTS = ImmutableSet
      .of("/query-stream", "/inserts-stream", "/close-query");

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

    // We only want to apply failure handler and auth handlers to routes that are not proxied
    // as Jetty will do it's own auth and failure handler
    routeFailureToNewApi(router, ServerVerticle::handleFailure);

    final Optional<AuthHandler> authHandler = getAuthHandler(server);
    final KsqlSecurityExtension securityExtension = server.getSecurityExtension();
    final Optional<AuthenticationPlugin> authenticationPlugin = server.getAuthenticationPlugin();

    if (authHandler.isPresent() || authenticationPlugin.isPresent()) {
      routeToNewApi(router, ServerVerticle::pauseHandler);
      if (authenticationPlugin.isPresent()) {
        routeToNewApi(router, createAuthenticationPluginHandler(authenticationPlugin.get()));
      } else {
        routeToNewApi(router, authHandler.get());
      }
      securityExtension.getAuthorizationProvider()
          .ifPresent(ksqlAuthorizationProvider -> routeToNewApi(router,
              new AuthorizationPlugin(server.getWorkerExecutor(), ksqlAuthorizationProvider)));

      routeToNewApi(router, ServerVerticle::resumeHandler);
    }

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

  private Handler<RoutingContext> createAuthenticationPluginHandler(
      final AuthenticationPlugin securityHandlerPlugin) {
    return routingContext -> {
      final CompletableFuture<Principal> cf = securityHandlerPlugin
          .handleAuth(routingContext, server.getWorkerExecutor());
      cf.thenAccept(principal -> {
        if (principal == null) {
          // Not authenticated
          // Do nothing, response is already ended by the plugin
        } else {
          routingContext.next();
        }
      }).exceptionally(t -> {
        routingContext.fail(t);
        return null;
      });
    };
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

  private void routeToNewApi(final Router router, final Handler<RoutingContext> handler) {
    for (String path : NEW_API_ENDPOINTS) {
      router.route(path).handler(handler);
    }
  }

  private void routeFailureToNewApi(final Router router, final Handler<RoutingContext> handler) {
    for (String path : NEW_API_ENDPOINTS) {
      router.route(path).failureHandler(handler);
    }
  }


}
