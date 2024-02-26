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

import static io.netty.handler.codec.http.HttpResponseStatus.FORBIDDEN;

import io.confluent.ksql.api.server.Server;
import io.confluent.ksql.security.KsqlAuthorizationProvider;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.WorkerExecutor;
import io.vertx.ext.auth.User;
import io.vertx.ext.web.RoutingContext;

/**
 * Handler that calls a KsqlAuthorizationProvider plugin that can be used for custom authorization
 */
public class KsqlAuthorizationProviderHandler implements Handler<RoutingContext> {

  private final WorkerExecutor workerExecutor;
  private final KsqlAuthorizationProvider ksqlAuthorizationProvider;

  public KsqlAuthorizationProviderHandler(final Server server,
      final KsqlAuthorizationProvider ksqlAuthorizationProvider) {
    this.workerExecutor = server.getWorkerExecutor();
    this.ksqlAuthorizationProvider = ksqlAuthorizationProvider;
  }

  @Override
  public void handle(final RoutingContext routingContext) {
    workerExecutor.<Void>executeBlocking(
        promise -> authorize(promise, routingContext),
        false,
        ar -> handleAuthorizeResult(ar, routingContext));
  }

  private static void handleAuthorizeResult(final AsyncResult<Void> ar,
      final RoutingContext routingContext) {
    if (ar.succeeded()) {
      routingContext.next();
    } else {
      routingContext.fail(FORBIDDEN.code(), ar.cause());
    }
  }

  private void authorize(final Promise<Void> promise, final RoutingContext routingContext) {
    final User user = routingContext.user();
    if (user == null) {
      promise.fail(
          new IllegalStateException("Null user in " + KsqlAuthorizationProviderHandler.class));
      return;
    }
    if (!(user instanceof ApiUser)) {
      throw new IllegalStateException("Not an ApiUser: " + user);
    }
    final ApiUser apiUser = (ApiUser) user;
    try {
      ksqlAuthorizationProvider
          .checkEndpointAccess(apiUser.getPrincipal(), routingContext.request().method().toString(),
              routingContext.normalisedPath());
    } catch (Exception e) {
      promise.fail(e);
      return;
    }
    promise.complete();
  }
}
