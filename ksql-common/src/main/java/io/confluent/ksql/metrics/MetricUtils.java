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
import java.util.stream.Collectors;

public final class MetricUtils {
  private MetricUtils() {
  }

  public static <T> double aggregateStat(
      final String name,
      final boolean isError,
      final Collection<TopicSensors<T>> sensors) {
    return sensors.stream()
        .flatMap(r -> r.stats(isError).stream())
        .filter(s -> s.name().equals(name))
        .mapToDouble(TopicSensors.Stat::getValue)
        .sum();
  }

  public static <T> Collection<TopicSensors.Stat> stats(
      final String topic,
      final boolean isError,
      final Collection<TopicSensors<T>> sensors) {
    return sensors.stream()
        .filter(counter -> counter.isTopic(topic))
        .flatMap(r -> r.stats(isError).stream())
        .collect(Collectors.toList());
  }
}
