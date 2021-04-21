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

package io.confluent.ksql.execution.streams;

import java.util.Set;

/**
 * These are options used for locating the host to retrieve data from.
 */
public interface RoutingOptions {
  // The offset lag allowed from a given host
  long getMaxOffsetLagAllowed();

  boolean getIsSkipForwardRequest();

  boolean getIsDebugRequest();

  Set<Integer> getPartitions();

  /**
   * @return a human readable representation of the routing options, used
   *         to debug requests
   */
  default String debugString() {
    return "RoutingOptions{"
        + "maxOffsetLagAllowed: " + getMaxOffsetLagAllowed()
        + ", isSkipForwardRequest: " + getIsSkipForwardRequest()
        + ", isDebugRequest: " + getIsDebugRequest()
        + ", partitions: " + getPartitions()
        + "}";
  }
}
