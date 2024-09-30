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
import io.confluent.ksql.api.auth.RoleBasedAuthZHandler;
import io.confluent.ksql.api.auth.SystemAuthenticationHandler;
import io.confluent.ksql.rest.server.KsqlRestConfig;
import io.confluent.ksql.security.KsqlSecurityExtension;
import io.vertx.core.Handler;
import io.vertx.core.http.ClientAuth;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BasicAuthHandler;
import java.net.URI;
import java.util.Optional;

public final class AuthHandlers {

  private static final String PROVIDER_KEY = "provider";

  private enum Provider {
    SYSTEM, JAAS, PLUGIN
  }

  private AuthHandlers() {
  }

  static void setupAuthHandlers(final Server server, final Router router,
      final boolean isInternalListener) {
    final Optional<BasicAuthHandler> jaas = getJaasAuthHandler(server);
    final KsqlSecurityExtension securityExtension = server.getSecurityExtension();
    final Optional<AuthenticationPlugin> authenticationPlugin = server.getAuthenticationPlugin();
    final Optional<Handler<RoutingContext>> pluginHandler =
        authenticationPlugin.map(plugin -> new AuthenticationPluginHandler(server, plugin));
    final Optional<SystemAuthenticationHandler> systemAuthenticationHandler
        = getSystemAuthenticationHandler(server, isInternalListener);

    systemAuthenticationHandler.ifPresent(handler -> router.route().handler(handler));

    if (jaas.isPresent() || authenticationPlugin.isPresent()) {
      router.route().handler(rc -> selectHandler(rc, jaas.isPresent(), pluginHandler.isPresent()));

      // set up using JAAS
      jaas.ifPresent(h -> router.route().handler(wrappedHandler(h, Provider.JAAS)));
      router.route().handler(wrappedHandler(
          new RoleBasedAuthZHandler(
              server.getConfig().getList(KsqlRestConfig.AUTHENTICATION_ROLES_CONFIG)),
          Provider.JAAS));

      // set up using PLUGIN
      pluginHandler.ifPresent(h -> router.route().handler(wrappedHandler(h, Provider.PLUGIN)));

      router.route().handler(AuthHandlers::pauseHandler);
      // For authorization use auth provider configured via security extension (if any)
      securityExtension.getAuthorizationProvider()
          .ifPresent(ksqlAuthorizationProvider -> router.route()
              .handler(new KsqlAuthorizationProviderHandler(server, ksqlAuthorizationProvider)));
      router.route().handler(AuthHandlers::resumeHandler);
    }
  }

  private static Handler<RoutingContext> wrappedHandler(
      final Handler<RoutingContext> handler,
      final Provider provider
  ) {
    return rc -> {
      if (rc.data().get(PROVIDER_KEY) != provider) {
        rc.next();
      } else {
        handler.handle(rc);
      }
    };
  }

  /**
   * In order of preference, we see if the user is the system user, then we check for
   * JAAS configurations, and finally we check to see if there's a plugin we can use
   */
  private static void selectHandler(
      final RoutingContext context,
      final boolean jaasAvailable,
      final boolean pluginAvailable
  ) {
    if (SystemAuthenticationHandler.isAuthenticatedAsSystemUser(context)) {
      context.data().put(PROVIDER_KEY, Provider.SYSTEM);
      context.next();
      return;
    }

    final String authHeader = context.request().getHeader("Authorization");
    if (jaasAvailable && authHeader != null && authHeader.toLowerCase().startsWith("basic ")) {
      context.data().put(PROVIDER_KEY, Provider.JAAS);
    } else if (pluginAvailable) {
      context.data().put(PROVIDER_KEY, Provider.PLUGIN);
    } else {
      // Fail the request as unauthorized - this will occur if no auth plugin but Jaas handler
      // is configured, but auth header is not basic auth
      context.fail(
          UNAUTHORIZED.code(),
          new KsqlApiException("Unauthorized", ERROR_CODE_UNAUTHORIZED));
    }
    context.next();
  }

  private static Optional<BasicAuthHandler> getJaasAuthHandler(final Server server) {
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

  private static BasicAuthHandler basicAuthHandler(final Server server) {
    final String realm = server.getConfig().getString(KsqlRestConfig.AUTHENTICATION_REALM_CONFIG);
    final JaasAuthProvider authProvider = new JaasAuthProvider(server, realm);
    return BasicAuthHandler.create(authProvider, realm);
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
