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

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.http.HttpConnection;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.AuthProvider;
import io.vertx.ext.auth.User;
import io.vertx.ext.web.RoutingContext;
import java.security.Principal;
import java.util.Objects;
import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSession;

public class SystemAuthenticationHandler implements Handler<RoutingContext> {

  public SystemAuthenticationHandler() {}

  @Override
  public void handle(final RoutingContext routingContext) {
    final HttpConnection httpConnection = routingContext.request().connection();
    if (!httpConnection.isSsl()) {
      throw new IllegalStateException("Should only have ssl connections");
    }
    final Principal peerPrincipal = getPeerPrincipal(httpConnection.sslSession());
    routingContext.setUser(new SystemUser(peerPrincipal));
    routingContext.next();
  }

  private static Principal getPeerPrincipal(final SSLSession sslSession) {
    try {
      return sslSession.getPeerPrincipal();
    } catch (SSLPeerUnverifiedException e) {
      throw new IllegalStateException("Peer should always be verified", e);
    }
  }

  public static boolean isAuthenticatedAsSystemUser(final RoutingContext routingContext) {
    final User user = routingContext.user();
    return user instanceof SystemUser;
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
