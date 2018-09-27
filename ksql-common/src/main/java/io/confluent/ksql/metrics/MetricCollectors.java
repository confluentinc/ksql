/*
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
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.kafka.common.metrics.JmxReporter;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.common.utils.SystemTime;

/**
 * Topic based collectors for producer/consumer related statistics that can be mapped on to
 * streams/tables/queries for ksql entities (Stream, Table, Query)
 */
public final class MetricCollectors {
  public static final String PRODUCER_MESSAGES_PER_SEC = "messages-per-sec";
  public static final String PRODUCER_TOTAL_MESSAGES = "total-messages";
  public static final String CONSUMER_MESSAGES_PER_SEC = "consumer-messages-per-sec";
  public static final String CONSUMER_TOTAL_MESSAGES = "consumer-total-messages";
  public static final String CONSUMER_TOTAL_BYTES = "consumer-total-bytes";
  public static final String CONSUMER_FAILED_MESSAGES = "consumer-failed-messages";
  public static final String CONSUMER_FAILED_MESSAGES_PER_SEC
      = "consumer-failed-messages-per-sec";

  private static Map<String, MetricCollector> collectorMap;
  private static Metrics metrics;

  static {
    initialize();
  }

  private static final Time time = new io.confluent.common.utils.SystemTime();

  private MetricCollectors() {
  }

  // visible for testing.
  // We need to call this from the MetricCollectorsTest because otherwise tests clobber each
  // others metric data. We also need it from the KsqlEngineMetricsTest
  public static void initialize() {
    final MetricConfig metricConfig = new MetricConfig()
        .samples(100)
        .timeWindow(
            1000,
            TimeUnit.MILLISECONDS
        );
    final List<MetricsReporter> reporters = new ArrayList<>();
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

  static String addCollector(final String id, final MetricCollector collector) {
    final StringBuilder builtId = new StringBuilder(id);
    while (collectorMap.containsKey(builtId.toString())) {
      builtId.append("-").append(collectorMap.size());
    }

    final String finalId = builtId.toString();
    collectorMap.put(finalId, collector);
    return finalId;
  }

  static void remove(final String id) {
    collectorMap.remove(id);
  }

  static Map<String, TopicSensors.Stat> getStatsFor(
      final String topic, final boolean isError) {
    return getAggregateMetrics(
        collectorMap.values().stream()
            .flatMap(c -> c.stats(topic.toLowerCase(), isError).stream())
            .collect(Collectors.toList())
    );
  }

  public static String getAndFormatStatsFor(final String topic, final boolean isError) {
    return format(
        getStatsFor(topic, isError).values(),
        isError ? "last-failed" : "last-message");
  }

  static Map<String, TopicSensors.Stat> getAggregateMetrics(
      final List<TopicSensors.Stat> allStats
  ) {
    final Map<String, TopicSensors.Stat> results = new TreeMap<>();
    allStats.forEach(stat -> {
      results.computeIfAbsent(
          stat.name(),
          k -> new TopicSensors.Stat(stat.name(), 0, stat.getTimestamp())
      );
      results.get(stat.name()).aggregate(stat.getValue());
    });
    return results;
  }

  private static String format(
      final Collection<TopicSensors.Stat> stats,
      final String lastEventTimestampMsg) {
    final StringBuilder results = new StringBuilder();
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
                  m -> m.aggregateStat(CONSUMER_MESSAGES_PER_SEC, false)
              )
          )
        )
        .values();
  }

  public static double aggregateStat(final String name, final boolean isError) {
    return collectorMap.values().stream()
        .mapToDouble(m -> m.aggregateStat(name, isError))
        .sum();
  }

  public static double currentProductionRate() {
    return aggregateStat(PRODUCER_MESSAGES_PER_SEC, false);
  }

  public static double currentConsumptionRate() {
    return aggregateStat(CONSUMER_MESSAGES_PER_SEC, false);
  }

  public static double totalMessageConsumption() {
    return aggregateStat(CONSUMER_TOTAL_MESSAGES, false);
  }

  public static double totalBytesConsumption() {
    return aggregateStat(CONSUMER_TOTAL_BYTES, false);
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
