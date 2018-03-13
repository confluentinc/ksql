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
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import io.confluent.common.utils.Time;

/**
 * Topic based collectors for producer/consumer related statistics that can be mapped on to
 * streams/tables/queries for ksql entities (Stream, Table, Query)
 */
public class MetricCollectors {


  private static Map<String, MetricCollector> collectorMap;
  private static Metrics metrics;

  static {
    initialize();
  }

  private static Time time = new io.confluent.common.utils.SystemTime();

  // visible for testing.
  // We need to call this from the MetricCollectorsTest because otherwise tests clobber each
  // others metric data. We also need it from the KsqlEngineMetricsTest
  public static void initialize() {
    MetricConfig metricConfig = new MetricConfig()
        .samples(100)
        .timeWindow(
            1000,
            TimeUnit.MILLISECONDS
        );
    List<MetricsReporter> reporters = new ArrayList<>();
    reporters.add(new JmxReporter("io.confluent.ksql.metrics"));
    // Replace all static contents other than Time to ensure they are cleaned for tests that are
    // not aware of the need to initialize/cleanup this test, in case test processes are reused.
    // Tests aware of the class clean everything up properly to get the state into a clean state,
    // a full, fresh instantiation here ensures something like KsqlEngineMetricsTest running after
    // another test that used MetricsCollector without running cleanUp will behave correctly.
    metrics = new Metrics(metricConfig, reporters, new SystemTime());
    collectorMap = new ConcurrentHashMap<>();
  }

  // visible for testing.
  // needs to be called from the tear down method of MetricCollectorsTest so that the tests don't
  // clobber each other. We also need to call it from the KsqlEngineMetrics test for the same
  // reason.
  public static void cleanUp() {
    if (metrics != null) {
      metrics.close();
    }
    collectorMap.clear();
  }

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

  public static String getStatsFor(final String topic, boolean isError) {

    ArrayList<TopicSensors.Stat> allStats = new ArrayList<>();
    collectorMap.values().forEach(c -> allStats.addAll(c.stats(topic.toLowerCase(), isError)));

    Map<String, TopicSensors.Stat> aggregateStats = getAggregateMetrics(allStats);

    return format(aggregateStats.values(), isError ? "last-failed" : "last-message");
  }

  public static void recordError(String topic) {
    collectorMap.values().iterator().next().recordError(topic);
  }


  static Map<String, TopicSensors.Stat> getAggregateMetrics(
      final List<TopicSensors.Stat> allStats
  ) {
    Map<String, TopicSensors.Stat> results = new TreeMap<>();
    allStats.forEach(stat -> {
      results.computeIfAbsent(
          stat.name(),
          k -> new TopicSensors.Stat(stat.name(), 0, stat.getTimestamp())
      );
      results.get(stat.name()).aggregate(stat.getValue());
    });
    return results;
  }

  private static String format(Collection<TopicSensors.Stat> stats, String lastEventTimestampMsg) {
    StringBuilder results = new StringBuilder();
    stats.forEach(stat -> results.append(stat.formatted()).append(" "));
    if (stats.size() > 0) {
      results
          .append(String.format("%16s: ", lastEventTimestampMsg))
          .append(String.format("%9s", stats.iterator().next().timestamp()));
    }
    return results.toString();
  }

  public static Collection<Double> currentConsumptionRateByQuery() {
    return collectorMap.values()
        .stream()
        .filter(collector -> collector.getGroupId() != null)
        .collect(
            Collectors.groupingBy(
              MetricCollector::getGroupId,
              Collectors.summingDouble(
                  MetricCollector::currentMessageConsumptionRate
              )
          )
        )
        .values();
  }

  public static double currentProductionRate() {
    return collectorMap.values().stream()
        .mapToDouble(MetricCollector::currentMessageProductionRate)
        .sum();
  }

  public static double currentConsumptionRate() {
    return collectorMap.values().stream()
        .mapToDouble(MetricCollector::currentMessageConsumptionRate)
        .sum();
  }

  public static double totalMessageConsumption() {
    return collectorMap.values().stream()
        .mapToDouble(MetricCollector::totalMessageConsumption)
        .sum();
  }

  public static double totalBytesConsumption() {
    return collectorMap.values().stream()
        .mapToDouble(MetricCollector::totalBytesConsumption)
        .sum();
  }

  public static double currentErrorRate() {
    return collectorMap.values().stream()
        .mapToDouble(MetricCollector::errorRate)
        .sum();
  }

  public static Metrics getMetrics() {
    return metrics;
  }

  public static Time getTime() {
    return time;
  }

}
