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

import io.confluent.ksql.api.server.InternalEndpointHandler;
import io.confluent.ksql.api.server.Server;
import io.confluent.ksql.rest.server.KsqlRestConfig;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.http.ClientAuth;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.AuthProvider;
import io.vertx.ext.auth.User;
import io.vertx.ext.web.RoutingContext;
import java.net.URI;
import java.security.Principal;
import java.util.Objects;
import java.util.Optional;

public class SystemAuthenticationHandler implements Handler<RoutingContext> {

  public SystemAuthenticationHandler() {}

  @Override
  public void handle(final RoutingContext routingContext) {
    if (InternalEndpointHandler.INTERNAL_PATHS.contains(routingContext.normalisedPath())) {
      routingContext.setUser(new SystemUser(new SystemPrincipal()));
    }
    routingContext.next();
  }

  public static boolean isAuthenticatedAsSystemUser(final RoutingContext routingContext) {
    final User user = routingContext.user();
    return user instanceof SystemUser;
  }

  public static boolean hasAuthorization(final RoutingContext routingContext) {
    return isAuthenticatedAsSystemUser(routingContext);
  }

  public static Optional<SystemAuthenticationHandler> getSystemAuthenticationHandler(
      final Server server, final Optional<Boolean> isInternalListener) {
    // The requirements for being considered a system call on behalf of the SystemUser are that
    // SSL mutual auth is in effect for the connection (meaning that the request is verified to be
    // coming from a known set of servers in the cluster), and that it came on the internal
    // listener interface, meaning that it's being done with the authorization of the system
    // rather than directly on behalf of the user. Mutual auth is only enforced when SSL is used.
    final String internalListener = server.getConfig().getString(
        KsqlRestConfig.INTERNAL_LISTENER_CONFIG);
    if (internalListener == null) {
      return Optional.empty();
    }
    final String scheme = URI.create(internalListener).getScheme();
    final boolean isInternal = isInternalListener.isPresent() && isInternalListener.get();
    if (server.getConfig().getClientAuthInternal() == ClientAuth.REQUIRED
        && "https".equalsIgnoreCase(scheme) && isInternal) {
      return Optional.of(new SystemAuthenticationHandler());
    }
    // Fall back on other authentication methods.
    return Optional.empty();
  }

  private static class SystemPrincipal implements Principal {
    private static final String SYSTEM_NAME = "__system__";

    SystemPrincipal() {}

    @Override
    public String getName() {
      return SYSTEM_NAME;
    }
  }

  private static class SystemUser implements ApiUser {

    private final Principal principal;

    SystemUser(final Principal principal) {
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
