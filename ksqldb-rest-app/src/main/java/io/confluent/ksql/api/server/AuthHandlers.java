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

import static io.confluent.ksql.api.server.ServerUtils.convertCommaSeparatedWilcardsToRegex;
import static io.confluent.ksql.rest.Errors.ERROR_CODE_UNAUTHORIZED;
import static io.netty.handler.codec.http.HttpResponseStatus.UNAUTHORIZED;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
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
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.regex.Pattern;

public final class AuthHandlers {

  private static final Set<String> KSQL_AUTHENTICATION_SKIP_PATHS = ImmutableSet
          .of("/v1/metadata", "/v1/metadata/id", "/healthcheck");

  private static final String PROVIDER_KEY = "provider";

  enum Provider {
    SYSTEM, JAAS, PLUGIN, SKIP
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

    systemAuthenticationHandler.ifPresent(handler -> registerAuthHandler(router, handler));

    if (jaas.isPresent() || authenticationPlugin.isPresent()) {
      final List<String> skipPaths = server.getConfig()
              .getList(KsqlRestConfig.AUTHENTICATION_SKIP_PATHS_CONFIG);
      final Pattern skipPathPattern = getAuthenticationSkipPathPattern(skipPaths);

      registerAuthHandler(
          router,
          rc -> selectHandler(rc, skipPathPattern, jaas.isPresent(), pluginHandler.isPresent()));

      // set up using JAAS
      jaas.ifPresent(h -> registerAuthHandler(router, selectiveHandler(h, Provider.JAAS::equals)));

      registerAuthHandler(
          router,
          selectiveHandler(new RoleBasedAuthZHandler(
              server.getConfig().getList(KsqlRestConfig.AUTHENTICATION_ROLES_CONFIG)),
              Provider.JAAS::equals));

      // set up using PLUGIN
      pluginHandler.ifPresent(h -> registerAuthHandler(router,
              selectiveHandler(h, Provider.PLUGIN::equals)));

      // For authorization use auth provider configured via security extension (if any)
      securityExtension.getAuthorizationProvider()
          .ifPresent(ksqlAuthorizationProvider ->
              registerAuthHandler(
                  router,
                  selectiveHandler(
                          new KsqlAuthorizationProviderHandler(server, ksqlAuthorizationProvider),
                          provider -> provider == Provider.JAAS || provider == Provider.PLUGIN
                  )));

      // since we're done with all the authorization plugins, we can resume the
      // request context
      router.route().handler(AuthHandlers::resumeHandler);
    }
  }

  /**
   * some handlers run code asynchronously from the event loop, which must be run while the
   * request is paused (see https://vertx.io/docs/vertx-web/java/#_request_body_handling).
   *
   * <p>the calls to #pauseHandler are defensive in case an underlying handler calls resume
   * on the context without our knowledge (which is what the default BasicAuthHandler in
   * vertx-web does as of version 4.x)
   */
  private static void registerAuthHandler(
      final Router router,
      final Handler<RoutingContext> routingContext
  ) {
    router.route().handler(AuthHandlers::pauseHandler);
    router.route().handler(routingContext);
  }

  private static Handler<RoutingContext> selectiveHandler(
      final Handler<RoutingContext> handler,
      final Predicate<Provider> providerTest
  ) {
    return rc -> {
      if (!providerTest.test((Provider)rc.data().get(PROVIDER_KEY))) {
        rc.next();
      } else {
        handler.handle(rc);
      }
    };
  }

  /**
   * In order of preference, we see if the path is supposed to be skipped, then, if the user is the
   * system user, then we check for JAAS configurations, and finally we check to see if there's a
   * plugin we can use.
   */
  @VisibleForTesting
  static void selectHandler(
      final RoutingContext context,
      final Pattern skipPathsPattern,
      final boolean jaasAvailable,
      final boolean pluginAvailable
  ) {
    if (skipPathsPattern.matcher(context.normalizedPath()).matches()) {
      context.data().put(PROVIDER_KEY, Provider.SKIP);
      context.next();
      return;
    }
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

  // default visibility for testing
  static Pattern getAuthenticationSkipPathPattern(final List<String> skipPaths) {
    // We add in all the hardcoded paths that don't require authentication/authorization
    final Set<String> unauthenticatedPaths = new HashSet<>(KSQL_AUTHENTICATION_SKIP_PATHS);
    // And then we add anything from the property authentication.skip.paths
    // This preserves the behaviour from the previous Jetty based implementation
    unauthenticatedPaths.addAll(skipPaths);
    final String paths = String.join(",", unauthenticatedPaths);
    final String converted = convertCommaSeparatedWilcardsToRegex(paths);
    return Pattern.compile(converted);
  }

}
