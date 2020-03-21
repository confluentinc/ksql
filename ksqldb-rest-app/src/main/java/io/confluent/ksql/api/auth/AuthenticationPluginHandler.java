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
import java.util.concurrent.CompletableFuture;

public class AuthenticationPluginHandler implements Handler<RoutingContext> {

  private final Server server;
  private final AuthenticationPlugin securityHandlerPlugin;

  public AuthenticationPluginHandler(final Server server,
      final AuthenticationPlugin securityHandlerPlugin) {
    this.server = server;
    this.securityHandlerPlugin = securityHandlerPlugin;
  }

  @Override
  public void handle(final RoutingContext routingContext) {
    final CompletableFuture<Principal> cf = securityHandlerPlugin
        .handleAuth(routingContext, server.getWorkerExecutor());
    cf.thenAccept(principal -> {
      // Deliberately break things for now
      routingContext.fail(401, new KsqlApiException("Badgers!!!",
          ErrorCodes.ERROR_FAILED_AUTHENTICATION));
      // if (principal == null) {
      //   // Not authenticated
      //   routingContext.fail(401, new KsqlApiException("Failed authentication",
      //       ErrorCodes.ERROR_FAILED_AUTHENTICATION));
      // } else {
      //   routingContext.setUser(new AuthPluginUser(principal));
      //   routingContext.next();
      // }
    }).exceptionally(t -> {
      // An internal error occurred
      routingContext.fail(t);
      return null;
    });
  }

  private static class AuthPluginUser implements ApiUser {

    private final Principal principal;

    AuthPluginUser(final Principal principal) {
      this.principal = principal;
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
