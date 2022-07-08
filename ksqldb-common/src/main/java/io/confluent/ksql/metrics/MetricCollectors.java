/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.ksql.metrics;

import static com.google.common.collect.ImmutableMap.toImmutableMap;

import com.google.common.base.Functions;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.common.utils.Time;
import io.confluent.ksql.util.AppInfo;
import io.confluent.ksql.util.KsqlConfig;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.metrics.JmxReporter;
import org.apache.kafka.common.metrics.KafkaMetricsContext;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.MetricsContext;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.common.utils.SystemTime;

/**
 * Topic based collectors for producer/consumer related statistics that can be mapped on to
 * streams/tables/queries for ksql entities (Stream, Table, Query)
 */
@SuppressWarnings("ClassDataAbstractionCoupling")
public final class MetricCollectors {

  public static final String RESOURCE_LABEL_PREFIX =
      CommonClientConfigs.METRICS_CONTEXT_PREFIX + "resource.";
  public static final String RESOURCE_LABEL_TYPE =
      RESOURCE_LABEL_PREFIX + "type";
  public static final String RESOURCE_LABEL_VERSION =
      RESOURCE_LABEL_PREFIX + "version";
  public static final String RESOURCE_LABEL_COMMIT_ID =
      RESOURCE_LABEL_PREFIX + "commit.id";
  public static final String RESOURCE_LABEL_CLUSTER_ID =
      RESOURCE_LABEL_PREFIX + "cluster.id";
  public static final String RESOURCE_LABEL_KAFKA_CLUSTER_ID =
      RESOURCE_LABEL_PREFIX + "kafka.cluster.id";
  private static final String KSQL_JMX_PREFIX = "io.confluent.ksql.metrics";
  private static final String KSQL_RESOURCE_TYPE = "ksql";
  private static final Time time = new io.confluent.common.utils.SystemTime();
  private static Map<String, MetricCollector> collectorMap;
  private static Metrics metrics;

  static {
    initialize();
  }

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
    reporters.add(new JmxReporter());
    final MetricsContext metricsContext = new KafkaMetricsContext(KSQL_JMX_PREFIX);
    // Replace all static contents other than Time to ensure they are cleaned for tests that are
    // not aware of the need to initialize/cleanup this test, in case test processes are reused.
    // Tests aware of the class clean everything up properly to get the state into a clean state,
    // a full, fresh instantiation here ensures something like KsqlEngineMetricsTest running after
    // another test that used MetricsCollector without running cleanUp will behave correctly.
    metrics = new Metrics(metricConfig, reporters, new SystemTime(), metricsContext);
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

  public static void addConfigurableReporter(
      final KsqlConfig ksqlConfig
  ) {
    final String ksqlServiceId = ksqlConfig.getString(KsqlConfig.KSQL_SERVICE_ID_CONFIG);

    final List<MetricsReporter> reporters = ksqlConfig.getConfiguredInstances(
        KsqlConfig.METRIC_REPORTER_CLASSES_CONFIG,
        MetricsReporter.class,
        Collections.singletonMap(
            KsqlConfig.KSQL_SERVICE_ID_CONFIG,
            ksqlServiceId));

    if (reporters.size() > 0) {
      final MetricsContext metricsContext = new KafkaMetricsContext(
          KSQL_JMX_PREFIX,
          ksqlConfig.originalsWithPrefix(
              CommonClientConfigs.METRICS_CONTEXT_PREFIX));
      for (final MetricsReporter reporter : reporters) {
        reporter.contextChange(metricsContext);
        metrics.addReporter(reporter);
      }
    }
  }

  public static Map<String, Object> addConfluentMetricsContextConfigs(
      final String ksqlServiceId,
      final String kafkaClusterId
  ) {
    final Map<String, Object> updatedProps = new HashMap<>();
    updatedProps.put(RESOURCE_LABEL_TYPE, KSQL_RESOURCE_TYPE);
    updatedProps.put(RESOURCE_LABEL_CLUSTER_ID, ksqlServiceId);
    updatedProps.put(RESOURCE_LABEL_KAFKA_CLUSTER_ID, kafkaClusterId);
    updatedProps.put(RESOURCE_LABEL_VERSION, AppInfo.getVersion());
    updatedProps.put(RESOURCE_LABEL_COMMIT_ID, AppInfo.getCommitId());
    return updatedProps;
  }

  static void remove(final String id) {
    collectorMap.remove(id);
  }

  public static Collection<TopicSensors.Stat> getStatsFor(
      final String topic, final boolean isError) {
    return getAggregateMetrics(
        collectorMap.values().stream()
            .flatMap(c -> c.stats(topic.toLowerCase(), isError).stream())
            .collect(Collectors.toList())
    );
  }

  public static String getAndFormatStatsFor(final String topic, final boolean isError) {
    return format(
        getStatsFor(topic, isError),
        isError ? "last-failed" : "last-message");
  }

  static Collection<TopicSensors.Stat> getAggregateMetrics(
      final List<TopicSensors.Stat> allStats
  ) {
    return allStats.stream().collect(toImmutableMap(
        TopicSensors.Stat::name,
        Functions.identity(),
        (first, other) -> first.aggregate(other.getValue())
    )).values();
  }

  public static String format(
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
                  m -> m.aggregateStat(ConsumerCollector.CONSUMER_MESSAGES_PER_SEC, false)
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
    return aggregateStat(ProducerCollector.PRODUCER_MESSAGES_PER_SEC, false);
  }

  public static double currentConsumptionRate() {
    return aggregateStat(ConsumerCollector.CONSUMER_MESSAGES_PER_SEC, false);
  }

  public static double totalMessageConsumption() {
    return aggregateStat(ConsumerCollector.CONSUMER_TOTAL_MESSAGES, false);
  }

  public static double totalBytesConsumption() {
    return aggregateStat(ConsumerCollector.CONSUMER_TOTAL_BYTES, false);
  }

  public static double currentErrorRate() {
    return collectorMap.values().stream()
        .mapToDouble(MetricCollector::errorRate)
        .sum();
  }

  @SuppressFBWarnings(value = "MS_EXPOSE_REP", justification = "should be mutable")
  public static Metrics getMetrics() {
    return metrics;
  }

  public static Time getTime() {
    return time;
  }

}
