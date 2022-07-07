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

import io.vertx.core.WorkerExecutor;
import io.vertx.ext.web.RoutingContext;
import java.security.Principal;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Extension point for adding custom authentication. E.g. used for custom authentication in
 * Confluent platform
 */
public interface AuthenticationPlugin {

  void configure(Map<String, ?> map);

  /**
   * Handle authentication for the request. The plugin implementation should not end the response in
   * case of failure. ksqlDB will end the response appropriately in case of failure.
   *
   * @param routingContext The routing context
   * @param workerExecutor The worker executor
   * @return A CompletableFuture representing the result of the authentication containing either
   *         the principal (on successful authentication) or null (on unsuccessful authentication)
   */
  CompletableFuture<Principal> handleAuth(RoutingContext routingContext,
      WorkerExecutor workerExecutor);

  /**
   * Retrieve the authorization token from the request. This is different from {@code handleAuth}
   * since we need to expose the authorization token in order to provide forwarded inter-node
   * requests the correct credentials.
   *
   * @param routingContext The routing context
   * @return A String that is the authorization token that we can then use for forwarding
   *        inter-node requests.
   */
  default String getAuthToken(final RoutingContext routingContext) {
    return routingContext.request().getHeader("Authorization");
  }

}
