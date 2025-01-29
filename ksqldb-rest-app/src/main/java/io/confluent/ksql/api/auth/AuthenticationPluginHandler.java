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

import static io.confluent.ksql.rest.Errors.ERROR_CODE_UNAUTHORIZED;
import static io.netty.handler.codec.http.HttpResponseStatus.UNAUTHORIZED;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.api.server.KsqlApiException;
import io.confluent.ksql.api.server.Server;
import io.confluent.ksql.security.DefaultKsqlPrincipal;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.AuthProvider;
import io.vertx.ext.auth.User;
import io.vertx.ext.auth.authorization.Authorization;
import io.vertx.ext.web.RoutingContext;
import java.security.Principal;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/**
 * Handler that calls any authentication plugin
 */
public class AuthenticationPluginHandler implements Handler<RoutingContext> {

  private final Server server;
  private final AuthenticationPlugin securityHandlerPlugin;

  @SuppressFBWarnings(value = "EI_EXPOSE_REP2")
  public AuthenticationPluginHandler(final Server server,
      final AuthenticationPlugin securityHandlerPlugin) {
    this.server = Objects.requireNonNull(server);
    this.securityHandlerPlugin = Objects.requireNonNull(securityHandlerPlugin);
  }

  @Override
  public void handle(final RoutingContext routingContext) {
    final CompletableFuture<Principal> cf = securityHandlerPlugin
        .handleAuth(routingContext, server.getWorkerExecutor());
    cf.thenAccept(principal -> {
      if (principal == null) {
        // Not authenticated
        routingContext
            .fail(UNAUTHORIZED.code(), new KsqlApiException("Failed authentication",
                ERROR_CODE_UNAUTHORIZED));
      } else {
        routingContext.setUser(new AuthPluginUser(principal));
        routingContext.next();
      }
    }).exceptionally(t -> {
      // Avoid calling context.fail() twice
      if (!routingContext.failed()) {
        // An internal error occurred
        routingContext.fail(t);
      }
      return null;
    });
  }

  private static class AuthPluginUser implements ApiUser {

    private final DefaultKsqlPrincipal principal;

    AuthPluginUser(final Principal principal) {
      Objects.requireNonNull(principal);
      this.principal = new DefaultKsqlPrincipal(principal);
    }

    @Override
    public JsonObject attributes() {
      throw new UnsupportedOperationException();
    }

    @Override
    public User isAuthorized(
        final Authorization authority,
        final Handler<AsyncResult<Boolean>> resultHandler
    ) {
      throw new UnsupportedOperationException();
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
    public User merge(final User other) {
      throw new UnsupportedOperationException();
    }

    @Override
    public DefaultKsqlPrincipal getPrincipal() {
      return principal;
    }
  }

}
