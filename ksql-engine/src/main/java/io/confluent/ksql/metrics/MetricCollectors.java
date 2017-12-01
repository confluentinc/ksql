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

import io.confluent.common.utils.Time;
import org.apache.kafka.common.metrics.JmxReporter;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.common.utils.SystemTime;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * Topic based collectors for producer/consumer related statistics that can be mapped on to streams/tables/queries for ksql entities (Stream, Table, Query)
 */
public class MetricCollectors {


  private static final Map<String, MetricCollector> collectorMap = new ConcurrentHashMap<>();
  private static final Metrics metrics;

  static {
    MetricConfig metricConfig = new MetricConfig().samples(100).timeWindow(1000, TimeUnit.MILLISECONDS);
    List<MetricsReporter> reporters = new ArrayList<>();
    reporters.add(new JmxReporter("io.confluent.ksql.metrics"));
    metrics = new Metrics(metricConfig, reporters, new SystemTime());
  }

  private static Time time = new io.confluent.common.utils.SystemTime();

  static String addCollector(String id, MetricCollector collector) {
    while (collectorMap.containsKey(id)) {
      id += "-" + collectorMap.size();
    }
    collectorMap.put(id, collector);
    return id;
  }

  static void remove(String id) {
    collectorMap.remove(id);
  }

  public static String getStatsFor(final String topic) {

    ArrayList<TopicSensors.Stat> allStats = new ArrayList<>();
    collectorMap.values().forEach(c -> allStats.addAll(c.stats(topic.toLowerCase())));

    Map<String, TopicSensors.Stat> aggregateStats = getAggregateMetrics(allStats);

    return format(aggregateStats.values());
  }

  static Map<String, TopicSensors.Stat> getAggregateMetrics(final List<TopicSensors.Stat> allStats) {
    Map<String, TopicSensors.Stat> results = new TreeMap<>();
    allStats.forEach(stat -> {
      results.computeIfAbsent(stat.name(), k -> new TopicSensors.Stat(stat.name(), 0L, stat.getTimestamp()));
      results.get(stat.name()).aggregate(stat.getValue());
    });
    return results;
  }

  private static String format(Collection<TopicSensors.Stat> stats) {
    StringBuilder results = new StringBuilder();
    stats.forEach(stat -> results.append(stat.formatted()).append("  "));
    if (stats.size() > 0) results.append(" last-event: ").append(stats.iterator().next().timestamp()).append(" \n");
    return results.toString();
  }

  public static Metrics getMetrics() {
    return metrics;
  }

  public static Time getTime() {
    return time;
  }
}
