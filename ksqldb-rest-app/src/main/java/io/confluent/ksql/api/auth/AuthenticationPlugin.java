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
 * Extension point for adding custom authentication.
 */
public interface AuthenticationPlugin {

  void configure(Map<String, ?> map);

  /**
   * Handle authentication for the request. Please note that in the case of failure to authenticate
   * the plugin should end the response. This behaviour makes it compatible with existing auth
   * plugins which it wraps.
   *
   * @param routingContext The routing context
   * @param workerExecutor The worker executor
   * @return A CompletableFuture representing the result of the authentication
   */
  CompletableFuture<Principal> handleAuth(RoutingContext routingContext,
      WorkerExecutor workerExecutor);

}
