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

package io.confluent.ksql.api.auth;

import io.confluent.ksql.api.server.ApiServerConfig;
import io.confluent.ksql.api.server.ErrorCodes;
import io.confluent.ksql.api.server.KsqlApiException;
import io.confluent.ksql.api.server.Server;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.AuthProvider;
import io.vertx.ext.auth.User;
import io.vertx.ext.web.RoutingContext;
import java.security.Principal;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.http.HttpStatus;

/**
 * Handler that calls any authentication plugin
 */
public class AuthenticationPluginHandler implements Handler<RoutingContext> {

  private final Server server;
  private final AuthenticationPlugin securityHandlerPlugin;
  private final Set<String> unauthenticatedPaths = new HashSet<>();

  public AuthenticationPluginHandler(final Server server,
      final AuthenticationPlugin securityHandlerPlugin) {
    this.server = Objects.requireNonNull(server);
    this.securityHandlerPlugin = Objects.requireNonNull(securityHandlerPlugin);
    // We add in all the paths that don't require authorization from KsqlAuthorizationProviderHandler
    unauthenticatedPaths.addAll(KsqlAuthorizationProviderHandler.PATHS_WITHOUT_AUTHORIZATION);
    // And then we add anything from the property authentication.skip.paths
    // This preserves the behaviour from the previous Jetty based implementation
    final List<String> unauthed = server.getConfig()
        .getList(ApiServerConfig.AUTHENTICATION_SKIP_PATHS_CONFIG);
    unauthenticatedPaths.addAll(unauthed);
  }

  @Override
  public void handle(final RoutingContext routingContext) {
    if (unauthenticatedPaths.contains(routingContext.normalisedPath())) {
      routingContext.next();
      return;
    }
    final CompletableFuture<Principal> cf = securityHandlerPlugin
        .handleAuth(routingContext, server.getWorkerExecutor());
    cf.thenAccept(principal -> {
      if (principal == null) {
        // Not authenticated
        routingContext
            .fail(HttpStatus.SC_UNAUTHORIZED, new KsqlApiException("Failed authentication",
                ErrorCodes.ERROR_FAILED_AUTHENTICATION));
      } else {
        routingContext.setUser(new AuthPluginUser(principal));
        routingContext.next();
      }
    }).exceptionally(t -> {
      // An internal error occurred
      routingContext.fail(t);
      return null;
    });
  }

  private static class AuthPluginUser implements ApiUser {

    private final Principal principal;

    AuthPluginUser(final Principal principal) {
      this.principal = Objects.requireNonNull(principal);
    }

    @SuppressWarnings("deprecation")
    @Override
    public User isAuthorized(final String s, final Handler<AsyncResult<Boolean>> handler) {
      throw new UnsupportedOperationException();
    }

    @SuppressWarnings("deprecation")
    @Override
    public User clearCache() {
      throw new UnsupportedOperationException();
    }

    @Override
    public JsonObject principal() {
      throw new UnsupportedOperationException();
    }

    @SuppressWarnings("deprecation")
    @Override
    public void setAuthProvider(final AuthProvider authProvider) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Principal getPrincipal() {
      return principal;
    }
  }

}
