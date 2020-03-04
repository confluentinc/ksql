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

import com.google.common.collect.ImmutableSet;
import io.confluent.ksql.security.KsqlAuthorizationProvider;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.WorkerExecutor;
import io.vertx.ext.auth.User;
import io.vertx.ext.web.RoutingContext;
import java.security.Principal;
import java.util.Set;

public class AuthorizationPlugin implements Handler<RoutingContext> {

  private static final Set<String> PATHS_WITHOUT_AUTHORIZATION = ImmutableSet
      .of("/v1/metadata", "/healthcheck");

  private final WorkerExecutor workerExecutor;
  private final KsqlAuthorizationProvider ksqlAuthorizationProvider;

  public AuthorizationPlugin(final WorkerExecutor workerExecutor,
      final KsqlAuthorizationProvider ksqlAuthorizationProvider) {
    this.workerExecutor = workerExecutor;
    this.ksqlAuthorizationProvider = ksqlAuthorizationProvider;
  }

  @Override
  public void handle(final RoutingContext routingContext) {

    final String path = routingContext.normalisedPath();

    if (PATHS_WITHOUT_AUTHORIZATION.contains(path)) {
      routingContext.next();
      return;
    }

    workerExecutor.<Void>executeBlocking(
        promise -> authorize(promise, routingContext),
        ar -> handleAuthorizeResult(ar, routingContext));
  }

  private static void handleAuthorizeResult(final AsyncResult<Void> ar,
      final RoutingContext routingContext) {
    if (ar.succeeded()) {
      routingContext.next();
    } else {
      routingContext.fail(403, ar.cause());
    }
  }

  private void authorize(final Promise<Void> promise, final RoutingContext routingContext) {
    final User user = routingContext.user();
    if (user == null) {
      promise
          .fail(new IllegalStateException("Null user in " + AuthorizationPlugin.class));
      return;
    }
    final Principal principal = new ApiPrincipal(user.principal().getString("username"));
    try {
      ksqlAuthorizationProvider
          .checkEndpointAccess(principal, routingContext.request().method().toString(),
              routingContext.normalisedPath());
    } catch (Exception e) {
      promise.fail(e);
      return;
    }
    promise.complete();
  }
}
