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

import static io.confluent.ksql.rest.Errors.ERROR_CODE_UNAUTHORIZED;
import static io.netty.handler.codec.http.HttpResponseStatus.UNAUTHORIZED;

import io.confluent.ksql.api.auth.AuthenticationPlugin;
import io.confluent.ksql.api.auth.AuthenticationPluginHandler;
import io.confluent.ksql.api.auth.JaasAuthProvider;
import io.confluent.ksql.api.auth.KsqlAuthorizationProviderHandler;
import io.confluent.ksql.api.auth.SystemAuthenticationHandler;
import io.confluent.ksql.rest.server.KsqlRestConfig;
import io.confluent.ksql.security.KsqlSecurityExtension;
import io.vertx.core.Handler;
import io.vertx.core.http.ClientAuth;
import io.vertx.ext.auth.AuthProvider;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.AuthHandler;
import io.vertx.ext.web.handler.BasicAuthHandler;
import java.net.URI;
import java.util.Optional;

public final class AuthHandlers {

  private AuthHandlers() {
  }

  static void setupAuthHandlers(final Server server, final Router router,
      final boolean isInternalListener) {
    final Optional<AuthHandler> jaasAuthHandler = getJaasAuthHandler(server);
    final KsqlSecurityExtension securityExtension = server.getSecurityExtension();
    final Optional<AuthenticationPlugin> authenticationPlugin = server.getAuthenticationPlugin();
    final Optional<Handler<RoutingContext>> pluginHandler =
        authenticationPlugin.map(plugin -> new AuthenticationPluginHandler(server, plugin));
    final Optional<SystemAuthenticationHandler> systemAuthenticationHandler
        = getSystemAuthenticationHandler(server, isInternalListener);

    systemAuthenticationHandler.ifPresent(handler -> router.route().handler(handler));

    if (jaasAuthHandler.isPresent() || authenticationPlugin.isPresent()) {
      router.route().handler(AuthHandlers::pauseHandler);

      router.route().handler(rc -> wrappedAuthHandler(rc, jaasAuthHandler, pluginHandler));

      // For authorization use auth provider configured via security extension (if any)
      securityExtension.getAuthorizationProvider()
          .ifPresent(ksqlAuthorizationProvider -> router.route()
              .handler(new KsqlAuthorizationProviderHandler(server, ksqlAuthorizationProvider)));

      router.route().handler(AuthHandlers::resumeHandler);
    }
  }

  private static void wrappedAuthHandler(final RoutingContext routingContext,
      final Optional<AuthHandler> jaasAuthHandler,
      final Optional<Handler<RoutingContext>> pluginHandler) {
    if (SystemAuthenticationHandler.isAuthenticatedAsSystemUser(routingContext)) {
      routingContext.next();
      return;
    }
    // Fall through to authing with Jaas
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
              new KsqlApiException("Unauthorized", ERROR_CODE_UNAUTHORIZED));
    }
  }

  private static Optional<AuthHandler> getJaasAuthHandler(final Server server) {
    final String authMethod = server.getConfig()
        .getString(KsqlRestConfig.AUTHENTICATION_METHOD_CONFIG);
    switch (authMethod) {
      case KsqlRestConfig.AUTHENTICATION_METHOD_BASIC:
        return Optional.of(basicAuthHandler(server));
      case KsqlRestConfig.AUTHENTICATION_METHOD_NONE:
        return Optional.empty();
      default:
        throw new IllegalStateException(String.format(
            "Unexpected value for %s: %s",
            KsqlRestConfig.AUTHENTICATION_METHOD_CONFIG,
            authMethod
        ));
    }
  }

  private static AuthHandler basicAuthHandler(final Server server) {
    final AuthProvider authProvider = new JaasAuthProvider(server, server.getConfig());
    final String realm = server.getConfig().getString(KsqlRestConfig.AUTHENTICATION_REALM_CONFIG);
    final AuthHandler basicAuthHandler = BasicAuthHandler.create(authProvider, realm);
    // It doesn't matter what we set here as we actually do the authorisation at the
    // authentication stage and cache the result, but we must add an authority or
    // no authorisation will be done
    basicAuthHandler.addAuthority("ksql");
    return basicAuthHandler;
  }

  /**
   * Gets the SystemAuthenticationHandler, if the requirements are met for it to be installed.
   * The requirements for installation are that SSL mutual auth is in effect for the connection
   * (meaning that the request is verified to be coming from a known set of servers in the cluster),
   * and that it came on the internal listener interface, meaning that it's being done with the
   * authorization of the system rather than directly on behalf of the user. Mutual auth is only
   * enforced when SSL is used.
   * @param server The server to potentially install the handler
   * @param isInternalListener If this handler is being considered for the internal listener
   * @return The SystemAuthenticationHandler if the requirements are met
   */
  private static Optional<SystemAuthenticationHandler> getSystemAuthenticationHandler(
      final Server server, final boolean isInternalListener) {
    final String internalListener = server.getConfig().getString(
        KsqlRestConfig.INTERNAL_LISTENER_CONFIG);
    if (internalListener == null) {
      return Optional.empty();
    }
    final String scheme = URI.create(internalListener).getScheme();
    if (server.getConfig().getClientAuthInternal() == ClientAuth.REQUIRED
        && "https".equalsIgnoreCase(scheme) && isInternalListener) {
      return Optional.of(new SystemAuthenticationHandler());
    }
    // Fall back on other authentication methods.
    return Optional.empty();
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
