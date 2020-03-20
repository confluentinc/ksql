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
import io.confluent.ksql.api.auth.AuthenticationPluginHandler;
import io.confluent.ksql.api.auth.JaasAuthProvider;
import io.confluent.ksql.api.auth.KsqlAuthorizationProviderHandler;
import io.confluent.ksql.api.server.protocol.ErrorResponse;
import io.confluent.ksql.api.spi.Endpoints;
import io.confluent.ksql.security.KsqlSecurityExtension;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Handler;
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

  private static final Set<String> NON_PROXIED_ENDPOINTS = ImmutableSet
      .of("/query-stream", "/inserts-stream", "/close-query", "/ksql");

  // Quick switch so we can easily revert to not serving ported endpoints directly
  private static final boolean SERVE_PORTED_ENDPOINTS = true;

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
        .exceptionHandler(ServerVerticle::unhandledExceptionHandler);
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

    if (SERVE_PORTED_ENDPOINTS) {
      PortedEndpoints.setupFailureHandler(router);
    }

    setUpFailureHandler(router);

    setupAuthHandlers(router);

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

    if (SERVE_PORTED_ENDPOINTS) {
      PortedEndpoints.setupEndpoints(endpoints, server, router);
    }

    if (proxyHandler != null) {
      proxyHandler.setupRoutes(router);
    }

    return router;
  }

  private void setupAuthHandlers(final Router router) {
    final Optional<AuthHandler> authHandler = getAuthHandler(server);
    final KsqlSecurityExtension securityExtension = server.getSecurityExtension();
    final Optional<AuthenticationPlugin> authenticationPlugin = server.getAuthenticationPlugin();
    
    if (authHandler.isPresent() || authenticationPlugin.isPresent()) {
      routeToNonProxiedEndpoints(router, ServerVerticle::pauseHandler);
      if (authenticationPlugin.isPresent()) {
        // Authentication plugin has precedence
        routeToNonProxiedEndpoints(router,
            new AuthenticationPluginHandler(server, authenticationPlugin.get()));
      } else {
        // Otherwise use user-configured JAAS auth handler
        routeToNonProxiedEndpoints(router, authHandler.get());
      }
      // For authorization use auth provider configured via security extension (if any)
      securityExtension.getAuthorizationProvider()
          .ifPresent(ksqlAuthorizationProvider -> routeToNonProxiedEndpoints(router,
              new KsqlAuthorizationProviderHandler(server.getWorkerExecutor(),
                  ksqlAuthorizationProvider)));

      routeToNonProxiedEndpoints(router, ServerVerticle::resumeHandler);
    }
  }

  private static void failureHandler(final RoutingContext routingContext) {

    log.error(String.format("Failed to handle request %d %s", routingContext.statusCode(),
        routingContext.request().path()),
        routingContext.failure());

    final int statusCode = routingContext.statusCode();
    final Throwable failure = routingContext.failure();

    if (failure != null) {
      if (failure instanceof KsqlApiException) {
        final KsqlApiException ksqlApiException = (KsqlApiException) failure;
        handleError(
            routingContext.response(),
            statusCode,
            ksqlApiException.getErrorCode(),
            ksqlApiException.getMessage()
        );
      } else {
        final int errorCode;
        if (statusCode == 401) {
          errorCode = ErrorCodes.ERROR_FAILED_AUTHENTICATION;
        } else if (statusCode == 403) {
          errorCode = ErrorCodes.ERROR_FAILED_AUTHORIZATION;
        } else {
          errorCode = ErrorCodes.ERROR_CODE_INTERNAL_ERROR;
        }
        handleError(
            routingContext.response(),
            statusCode,
            errorCode,
            failure.getMessage()
        );
      }
    } else {
      routingContext.response().setStatusCode(statusCode).end();
    }
  }

  private static void handleError(final HttpServerResponse response, final int statusCode,
      final int errorCode, final String errMsg) {
    final ErrorResponse errorResponse = new ErrorResponse(errorCode, errMsg);
    final Buffer buffer = errorResponse.toBuffer();
    response.setStatusCode(statusCode).end(buffer);
  }

  private static void unhandledExceptionHandler(final Throwable t) {
    log.error("Unhandled exception", t);
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

  // Applies the handler to all non proxied endpoints
  private static void routeToNonProxiedEndpoints(final Router router,
      final Handler<RoutingContext> handler) {
    for (String path : NON_PROXIED_ENDPOINTS) {
      router.route(path).handler(handler);
    }
  }

  private static void setUpFailureHandler(final Router router) {
    for (String path : NON_PROXIED_ENDPOINTS) {
      router.route(path).failureHandler(ServerVerticle::failureHandler);
    }
  }

}
