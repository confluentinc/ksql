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

package io.confluent.ksql.api.server;

import com.google.common.collect.ImmutableSet;
import io.vertx.core.Handler;
import io.vertx.ext.web.RoutingContext;
import java.util.Set;

public class InternalEndpointHandler implements Handler<RoutingContext> {
  private static final Set<String> INTERNAL_PATHS = ImmutableSet.of(
      "/heartbeat", "/lag");

  private final boolean isFromInternalListener;

  public InternalEndpointHandler(final boolean isFromInternalListener) {
    this.isFromInternalListener = isFromInternalListener;
  }


  @Override
  public void handle(final RoutingContext routingContext) {
    if (INTERNAL_PATHS.contains(routingContext.normalisedPath())
        && !isFromInternalListener) {
      routingContext.fail(400,
          new KsqlApiException("Can't call internal endpoint on public listener",
              ErrorCodes.ERROR_CODE_INTERNAL_ONLY));
    } else {
      routingContext.next();
    }
  }
}
