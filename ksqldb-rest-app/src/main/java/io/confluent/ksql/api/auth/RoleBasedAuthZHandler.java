/*
 * Copyright 2022 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"; you may not use
 * this file except in compliance with the License. You may obtain a copy of the
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

import com.google.common.annotations.VisibleForTesting;
import io.vertx.core.Handler;
import io.vertx.ext.auth.authorization.Authorization;
import io.vertx.ext.auth.authorization.AuthorizationContext;
import io.vertx.ext.auth.authorization.OrAuthorization;
import io.vertx.ext.auth.authorization.RoleBasedAuthorization;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.AuthorizationHandler;
import java.util.List;
import java.util.stream.Collectors;

public class RoleBasedAuthZHandler implements Handler<RoutingContext> {

  private final Authorization delegate;
  private final AuthorizationHandler handler;

  public RoleBasedAuthZHandler(final List<String> allowedRoles) {
    final List<String> parsedRoles = allowedRoles
        .stream()
        .filter(role -> !"*".equals(role))            // remove "*"
        .map(role -> "**".equals(role) ? "*" : role)  // Change "**" to "*"
        .collect(Collectors.toList());

    if (parsedRoles.contains("*")) {
      this.delegate = StarAuthHandler.INSTANCE;
    } else {
      this.delegate =
         parsedRoles
              .stream()
              .reduce(
                  OrAuthorization.create(),
                  (auth, role) -> auth.addAuthorization(RoleBasedAuthorization.create(role)),
                  OrAuthorization::addAuthorization);
    }
    this.handler = AuthorizationHandler.create(delegate);
  }

  @Override
  public void handle(final RoutingContext rc) {
    handler.handle(rc);
  }

  @VisibleForTesting
  Authorization getDelegate() {
    return delegate;
  }

  private static class StarAuthHandler implements Authorization {

    private static final StarAuthHandler INSTANCE = new StarAuthHandler();

    @Override
    public boolean match(final AuthorizationContext context) {
      return true;
    }

    @Override
    public boolean verify(final Authorization authorization) {
      return true;
    }
  }
}
