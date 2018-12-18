/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.metrics;

import java.util.Collection;

interface MetricCollector {
  default String getGroupId() {
    return null;
  }

  Collection<TopicSensors.Stat> stats(String topic, boolean isError);

  default double errorRate() {
    return 0.0;
  }

  double aggregateStat(String name, boolean isError);
}
