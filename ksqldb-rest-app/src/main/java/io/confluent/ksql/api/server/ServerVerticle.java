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

import static io.netty.handler.codec.http.HttpResponseStatus.FORBIDDEN;
import static io.netty.handler.codec.http.HttpResponseStatus.UNAUTHORIZED;

import com.google.common.collect.ImmutableSet;
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
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.JsonObject;
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

  public static final Set<String> NON_PROXIED_ENDPOINTS = ImmutableSet
      .of("/query-stream", "/inserts-stream", "/close-query",
          "/ksql", "/ksql/terminate", "/query", "/info", "/heartbeat", "/clusterStatus",
          "/status/:type/:entity/:action", "/status", "/lag", "/healthcheck", "/v1/metadata",
          "/v1/metadata/id", "/ws/query");

  private final Endpoints endpoints;
  private final HttpServerOptions httpServerOptions;
  private final Server server;
  private final boolean proxyEnabled;
  private ProxyHandler proxyHandler;
  private ConnectionQueryManager connectionQueryManager;
  private HttpServer httpServer;

  public ServerVerticle(final Endpoints endpoints, final HttpServerOptions httpServerOptions,
      final Server server, final boolean proxyEnabled) {
    this.endpoints = Objects.requireNonNull(endpoints);
    this.httpServerOptions = Objects.requireNonNull(httpServerOptions);
    this.server = Objects.requireNonNull(server);
    this.proxyEnabled = proxyEnabled;
  }

  @Override
  public void start(final Promise<Void> startPromise) {
    if (proxyEnabled) {
      this.proxyHandler = new ProxyHandler(server, context);
    } else {
      this.proxyHandler = null;
    }
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

    router.route(HttpMethod.GET, "/chc/ready").handler(ServerVerticle::chcHandler);
    router.route(HttpMethod.GET, "/chc/live").handler(ServerVerticle::chcHandler);

    PortedEndpoints.setupFailureHandler(router);

    KsqlCorsHandler.setupCorsHandler(server, router);

    setUpFailureHandler(router);

    setupAuthHandlers(router);

    setUpServerStateHandlers(router);

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
    
    return router;
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
        if (statusCode == UNAUTHORIZED.code()) {
          errorCode = ErrorCodes.ERROR_FAILED_AUTHENTICATION;
        } else if (statusCode == FORBIDDEN.code()) {
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

  private void setupAuthHandlers(final Router router) {
    final Optional<AuthHandler> jaasAuthHandler = getJaasAuthHandler(server);
    final KsqlSecurityExtension securityExtension = server.getSecurityExtension();
    final Optional<AuthenticationPlugin> authenticationPlugin = server.getAuthenticationPlugin();
    final Optional<Handler<RoutingContext>> pluginHandler =
        authenticationPlugin.map(plugin -> new AuthenticationPluginHandler(server, plugin));

    if (jaasAuthHandler.isPresent() || authenticationPlugin.isPresent()) {
      routeToNonProxiedEndpoints(router, ServerVerticle::pauseHandler);

      routeToNonProxiedEndpoints(router,
          rc -> wrappedAuthHandler(rc, jaasAuthHandler, pluginHandler));

      // For authorization use auth provider configured via security extension (if any)
      securityExtension.getAuthorizationProvider()
          .ifPresent(ksqlAuthorizationProvider -> routeToNonProxiedEndpoints(router,
              new KsqlAuthorizationProviderHandler(server.getWorkerExecutor(),
                  ksqlAuthorizationProvider)));

      routeToNonProxiedEndpoints(router, ServerVerticle::resumeHandler);
    }
  }

  private static void wrappedAuthHandler(final RoutingContext routingContext,
      final Optional<AuthHandler> jaasAuthHandler,
      final Optional<Handler<RoutingContext>> pluginHandler) {
    if (jaasAuthHandler.isPresent()) {
      // If we have a Jaas handler configured and we have Basic credentials then we should auth
      // with that
      final String authHeader = routingContext.request().getHeader("Authorization");
      if (authHeader != null && authHeader.toLowerCase().startsWith("basic ")) {
        jaasAuthHandler.get().handle(routingContext);
        return;
      }
    }
    // Fall through to authing with any authentication plugin
    if (pluginHandler.isPresent()) {
      pluginHandler.get().handle(routingContext);
    } else {
      // Fail the request as unauthorized - this will occur if no auth plugin but Jaas handler
      // is configured, but auth header is not basic auth
      routingContext
          .fail(UNAUTHORIZED.code(),
              new KsqlApiException("Unauthorized", ErrorCodes.ERROR_FAILED_AUTHENTICATION));
    }
  }

  private static Optional<AuthHandler> getJaasAuthHandler(final Server server) {
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

  private void setUpServerStateHandlers(final Router router) {
    // This will require special handling when removing the proxy server as only endpoints
    // defined in this repo (rather than in custom plugins) should reject requests based
    // on server state
    routeToNonProxiedEndpoints(router, new ServerStateHandler(server.getServerState()));
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

  private static void chcHandler(final RoutingContext routingContext) {
    routingContext.response().putHeader(HttpHeaders.CONTENT_TYPE.toString(), "application/json")
        .end(new JsonObject().toBuffer());
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
