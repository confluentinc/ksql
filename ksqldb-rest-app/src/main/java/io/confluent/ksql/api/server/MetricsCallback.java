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

package io.confluent.ksql.api.server;

/**
 * Interface for reporting metrics to a resource. A resource may choose to break things down
 * arbitrarily, e.g. /query is used for both push and pull queries so we let the resource
 * determine how to report the metrics.
 */
public interface MetricsCallback {

  /**
   * Called to report metrics when the request is complete, error or success
   * @param requestBytes The request bytes
   * @param responseBytes The response bytes
   * @param startTimeNanos The start time of the request in nanos
   */
  void reportMetricsOnCompletion(long requestBytes, long responseBytes, long startTimeNanos);
}
