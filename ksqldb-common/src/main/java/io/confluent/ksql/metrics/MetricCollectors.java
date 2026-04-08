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
import static java.util.Collections.singletonList;

import com.google.common.base.Functions;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.common.utils.Time;
import io.confluent.ksql.util.AppInfo;
import io.confluent.ksql.util.KsqlConfig;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
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

/**
 * Topic based collectors for producer/consumer related statistics that can be mapped on to
 * streams/tables/queries for ksql entities (Stream, Table, Query)
 */
@SuppressFBWarnings(
    value = "EI_EXPOSE_REP2",
    justification = "should be mutable"
)
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
  private final Time time = new io.confluent.common.utils.SystemTime();
  private Map<String, MetricCollector> collectorMap;
  private Metrics metrics;

  public MetricCollectors() {
    this(
        new Metrics(
            new MetricConfig()
                .samples(100)
                .timeWindow(
                    1000,
                    TimeUnit.MILLISECONDS
                ),
            // must be a mutable list to add configured reporters later
            new LinkedList<>(singletonList(new JmxReporter())),
            org.apache.kafka.common.utils.Time.SYSTEM,
            new KafkaMetricsContext(KSQL_JMX_PREFIX)
        )
    );
  }

  @SuppressFBWarnings(
      value = "EI_EXPOSE_REP2",
      justification = "should be mutable"
  )
  public MetricCollectors(final Metrics metrics) {
    this.metrics = metrics;
    collectorMap = new ConcurrentHashMap<>();
  }

  String addCollector(final String id, final MetricCollector collector) {
    final StringBuilder builtId = new StringBuilder(id);
    while (collectorMap.containsKey(builtId.toString())) {
      builtId.append("-").append(collectorMap.size());
    }

    final String finalId = builtId.toString();
    collectorMap.put(finalId, collector);
    return finalId;
  }

  public void addConfigurableReporter(
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

  public Map<String, Object> addConfluentMetricsContextConfigs(
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

  void remove(final String id) {
    collectorMap.remove(id);
  }

  public Collection<TopicSensors.Stat> getStatsFor(
      final String topic, final boolean isError) {
    return getAggregateMetrics(
        collectorMap.values().stream()
            .flatMap(c -> c.stats(topic.toLowerCase(), isError).stream())
            .collect(Collectors.toList())
    );
  }

  public String getAndFormatStatsFor(final String topic, final boolean isError) {
    return format(
        getStatsFor(topic, isError),
        isError ? "last-failed" : "last-message");
  }

  Collection<TopicSensors.Stat> getAggregateMetrics(
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

  public Collection<Double> currentConsumptionRateByQuery() {
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

  public double aggregateStat(final String name, final boolean isError) {
    return collectorMap.values().stream()
        .mapToDouble(m -> m.aggregateStat(name, isError))
        .sum();
  }

  public double currentProductionRate() {
    return aggregateStat(ProducerCollector.PRODUCER_MESSAGES_PER_SEC, false);
  }

  public double currentConsumptionRate() {
    return aggregateStat(ConsumerCollector.CONSUMER_MESSAGES_PER_SEC, false);
  }

  public double totalMessageConsumption() {
    return aggregateStat(ConsumerCollector.CONSUMER_TOTAL_MESSAGES, false);
  }

  public double totalBytesConsumption() {
    return aggregateStat(ConsumerCollector.CONSUMER_TOTAL_BYTES, false);
  }

  public double currentErrorRate() {
    return collectorMap.values().stream()
        .mapToDouble(MetricCollector::errorRate)
        .sum();
  }

  @SuppressFBWarnings(
      value = {"MS_EXPOSE_REP", "EI_EXPOSE_REP"},
      justification = "should be mutable"
  )
  public Metrics getMetrics() {
    return metrics;
  }

  public Time getTime() {
    return time;
  }

}
