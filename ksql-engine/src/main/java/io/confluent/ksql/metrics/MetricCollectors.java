/**
 * Copyright 2017 Confluent Inc.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.confluent.ksql.metrics;

import org.apache.kafka.common.metrics.JmxReporter;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.common.utils.SystemTime;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Topic based collectors for producer/consumer related statistics that can be mapped on to streams/tables/queries for ksql entities (Stream, Table, Query)
 */
public class MetricCollectors {


  private static final Map<String, MetricCollector> collectorMap = new HashMap<>();
  private static final Metrics metrics;

  static {
    MetricConfig metricConfig = new MetricConfig().samples(100).timeWindow(1000, TimeUnit.MILLISECONDS);
    List<MetricsReporter> reporters = new ArrayList<>();
    reporters.add(new JmxReporter("ksql.metrics"));
    metrics = new Metrics(metricConfig, reporters, new SystemTime());
  }

  static Metrics addCollector(String id, MetricCollector collector) {
    while (collectorMap.containsKey(id)) {
      id += collectorMap.size() + ".";
    }
    collectorMap.put(id, collector);
    return metrics;
  }

  static void remove(String id) {
    collectorMap.remove(id);
  }

  public static String getCollectorStatsByTopic(final String topic) {
    final StringBuilder results = new StringBuilder();
    collectorMap.values().forEach(c -> results.append(c.statsForTopic(topic.toLowerCase())));
    return results.toString();
  }

  public static Metrics getMetrics() {
    return metrics;
  }
}
