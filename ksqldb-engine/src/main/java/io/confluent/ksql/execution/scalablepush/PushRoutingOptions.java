/*
 * Copyright 2021 Confluent Inc.
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

package io.confluent.ksql.execution.scalablepush;

import java.util.Optional;

/**
 * Routing options given to scalable push queries.
 */
public interface PushRoutingOptions {

  // If we should avoid skipping forwarding the request because it's already been forwarded.
  boolean getHasBeenForwarded();

  boolean getIsDebugRequest();

  Optional<String> getContinuationToken();

  Optional<String> getCatchupConsumerGroup();

  boolean shouldOutputContinuationToken();

  boolean alosEnabled();

  /**
   * @return a human readable representation of the routing options, used
   *         to debug requests
   */
  default String debugString() {
    return "PushRoutingOptions{"
            + "getHasBeenForwarded: " + getHasBeenForwarded()
            + ", isDebugRequest: " + getIsDebugRequest()
            + "}";
  }
}
