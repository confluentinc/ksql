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

import io.confluent.ksql.security.KsqlAuthorizationProvider;
import io.vertx.core.Handler;
import io.vertx.core.WorkerExecutor;
import io.vertx.ext.auth.User;
import io.vertx.ext.web.RoutingContext;
import java.security.Principal;

public class KsqlAuthorizationFilter implements Handler<RoutingContext> {

  private final WorkerExecutor workerExecutor;
  private final KsqlAuthorizationProvider ksqlAuthorizationProvider;

  public KsqlAuthorizationFilter(final WorkerExecutor workerExecutor,
      final KsqlAuthorizationProvider ksqlAuthorizationProvider) {
    this.workerExecutor = workerExecutor;
    this.ksqlAuthorizationProvider = ksqlAuthorizationProvider;
  }

  @Override
  public void handle(final RoutingContext routingContext) {

    workerExecutor.executeBlocking(
        promise -> {
          final User user = routingContext.user();
          if (user == null) {
            promise.complete();
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
        },
        ar -> {
          if (ar.succeeded()) {
            routingContext.next();
          } else {
            routingContext.fail(401, ar.cause());
          }
        });
  }
}
