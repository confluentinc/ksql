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

package io.confluent.ksql.internal;

import static io.confluent.ksql.internal.MetricsTagUtils.KSQL_CONSUMER_GROUP_MEMBER_ID_TAG;
import static io.confluent.ksql.internal.MetricsTagUtils.KSQL_QUERY_ID_TAG;
import static io.confluent.ksql.internal.MetricsTagUtils.KSQL_TOPIC_TAG;
import static io.confluent.ksql.internal.MetricsTagUtils.SHARED_RUNTIME_THREAD_PATTERN;
import static io.confluent.ksql.internal.MetricsTagUtils.UNSHARED_RUNTIME_THREAD_PATTERN;
import static java.util.Objects.requireNonNull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.MetricsContext;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.common.metrics.stats.CumulativeSum;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ThroughputMetricsReporter implements MetricsReporter {
  private static final Logger LOGGER = LoggerFactory.getLogger(ThroughputMetricsReporter.class);
  private static final String THROUGHPUT_METRICS_GROUP = "ksql-query-throughput-metrics";
  private static final String RECORDS_CONSUMED = "records-consumed-total";
  private static final String BYTES_CONSUMED = "bytes-consumed-total";
  private static final String RECORDS_PRODUCED = "records-produced-total";
  private static final String BYTES_PRODUCED = "bytes-produced-total";
  private static final Set<String> THROUGHPUT_METRIC_NAMES =
      Set.of(RECORDS_CONSUMED, BYTES_CONSUMED, RECORDS_PRODUCED, BYTES_PRODUCED);

  private static final Map<String, Map<String, Map<MetricName, ThroughputTotalMetric>>> metrics =
      new HashMap<>();
  private static final Map<String, String> customTags = new HashMap<>();
  private Metrics metricRegistry;

  @Override
  public void init(final List<KafkaMetric> initial) {
  }

  @VisibleForTesting
  static void reset() {
    metrics.clear();
  }

  @Override
  public synchronized void configure(final Map<String, ?> configMap) {
    this.metricRegistry = (Metrics) requireNonNull(
      configMap.get(KsqlConfig.KSQL_INTERNAL_METRICS_CONFIG)
    );
    customTags.putAll(KsqlConfig.getStringAsMap(KsqlConfig.KSQL_CUSTOM_METRICS_TAGS, configMap));
  }

  @Override
  public void metricChange(final KafkaMetric metric) {
    if (!THROUGHPUT_METRIC_NAMES.contains(metric.metricName().name())
          || !StreamsMetricsImpl.TOPIC_LEVEL_GROUP.equals(metric.metricName().group())) {
      return;
    }
    addMetric(
        metric,
        getQueryId(metric),
        getTopic(metric)
    );
  }

  private synchronized void addMetric(
      final KafkaMetric metric,
      final String queryId,
      final String topic
  ) {
    final MetricName throughputTotalMetricName =
          getThroughputTotalMetricName(queryId, topic, metric.metricName());
    LOGGER.debug("Adding metric {}", throughputTotalMetricName);
    if (!metrics.containsKey(queryId)) {
      metrics.put(queryId, new HashMap<>());
    }
    if (!metrics.get(queryId).containsKey(topic)) {
      metrics.get(queryId).put(topic, new HashMap<>());
    }

    final ThroughputTotalMetric existingThroughputMetric =
          metrics.get(queryId).get(topic).get(throughputTotalMetricName);

    if (existingThroughputMetric == null) {
      final ThroughputTotalMetric newThroughputMetric = new ThroughputTotalMetric(metric);

      metrics.get(queryId).get(topic)
          .put(throughputTotalMetricName, newThroughputMetric);
      metricRegistry.addMetric(
          throughputTotalMetricName,
          newThroughputMetric
      );
    } else {
      existingThroughputMetric.add(metric);
    }
  }

  @Override
  public void metricRemoval(final KafkaMetric metric) {
    if (!THROUGHPUT_METRIC_NAMES.contains(metric.metricName().name())
          || !StreamsMetricsImpl.TOPIC_LEVEL_GROUP.equals(metric.metricName().group())) {
      return;
    }

    removeMetric(
        metric,
        getQueryId(metric),
        getTopic(metric)
    );
  }

  private synchronized void removeMetric(
      final KafkaMetric metric,
      final String queryId,
      final String topic
  ) {
    final MetricName throughputTotalMetricName =
        getThroughputTotalMetricName(queryId, topic, metric.metricName());

    LOGGER.debug("Removing metric {}", throughputTotalMetricName);

    if (metrics.containsKey(queryId)
          && metrics.get(queryId).containsKey(topic)
          && metrics.get(queryId).get(topic).containsKey(throughputTotalMetricName)) {

      final ThroughputTotalMetric throughputTotalMetric =
          metrics.get(queryId).get(topic).get(throughputTotalMetricName);

      throughputTotalMetric.remove(metric.metricName());
      
      if (throughputTotalMetric.throughputTotalMetrics.isEmpty()) {
        metrics.get(queryId).get(topic).remove(throughputTotalMetricName);
        metricRegistry.removeMetric(throughputTotalMetricName);
        if (metrics.get(queryId).get(topic).isEmpty()) {
          metrics.get(queryId).remove(topic);
          if (metrics.get(queryId).isEmpty()) {
            metrics.remove(queryId);
          }
        }
      }
    }
  }

  @Override
  public void close() {
  }

  @Override
  public Set<String> reconfigurableConfigs() {
    return null;
  }

  @Override
  public void validateReconfiguration(final Map<String, ?> configs) throws ConfigException {
  }

  @Override
  public void reconfigure(final Map<String, ?> configs) {
  }

  @Override
  public void contextChange(final MetricsContext metricsContext) {
  }

  private MetricName getThroughputTotalMetricName(
      final String queryId,
      final String topic,
      final MetricName metricName
  ) {
    return new MetricName(
      metricName.name(),
      THROUGHPUT_METRICS_GROUP,
      metricName.description() + " by this query",
      getThroughputTotalMetricTags(queryId, topic, metricName.tags())
    );
  }

  private String getQueryId(final KafkaMetric metric) {
    if (metric.metricName().tags().containsKey(KSQL_QUERY_ID_TAG)) {
      return metric.metricName().tags().get(KSQL_QUERY_ID_TAG);
    }

    final String taskName =
        metric.metricName().tags().getOrDefault(StreamsMetricsImpl.TASK_ID_TAG, "");
    final Matcher namedTopologyMatcher = SHARED_RUNTIME_THREAD_PATTERN.matcher(taskName);
    if (namedTopologyMatcher.find()) {
      return namedTopologyMatcher.group(1);
    }

    final String queryIdTag =
        metric.metricName().tags().getOrDefault(StreamsMetricsImpl.THREAD_ID_TAG, "");
    final Matcher matcher = UNSHARED_RUNTIME_THREAD_PATTERN.matcher(queryIdTag);

    if (matcher.find()) {
      return matcher.group(1);
    } else {
      LOGGER.error("Can't parse query id from metric {}", metric.metricName());
      throw new KsqlException("Missing query ID when reporting total throughput metrics");
    }
  }

  private String getTopic(final KafkaMetric metric) {
    if (metric.metricName().tags().containsKey(KSQL_TOPIC_TAG)) {
      return metric.metricName().tags().get(KSQL_TOPIC_TAG);
    }

    final String topic =
        metric.metricName().tags().getOrDefault(StreamsMetricsImpl.TOPIC_NAME_TAG, "");
    if (topic.equals("")) {
      LOGGER.error("Can't parse topic name from metric {}", metric);
      throw new KsqlException("Missing topic name when reporting total throughput metrics");
    } else {
      return topic;
    }
  }

  private Map<String, String> getThroughputTotalMetricTags(
      final String queryId,
      final String topic,
      final Map<String, String> originalStreamsMetricTags
  ) {
    final Map<String, String> queryMetricTags = new HashMap<>(customTags);
    queryMetricTags.putAll(originalStreamsMetricTags);
    // Remove the taskId and processorNodeId tags as the throughput total metric sums over them
    queryMetricTags.remove(StreamsMetricsImpl.TASK_ID_TAG);
    queryMetricTags.remove(StreamsMetricsImpl.PROCESSOR_NODE_ID_TAG);

    // Replace thread id with consumer group member id to match existing client tag/Druid label
    final String threadId = queryMetricTags.remove(StreamsMetricsImpl.THREAD_ID_TAG);
    queryMetricTags.put(KSQL_CONSUMER_GROUP_MEMBER_ID_TAG, threadId);

    queryMetricTags.put(KSQL_QUERY_ID_TAG, queryId);
    return ImmutableMap.copyOf(queryMetricTags);
  }

  // This is a hack to get the ce-metrics to correctly identify these metrics as a cumulative sum
  // and take the diff to compute the delta so that it can be accumulated downstream.
  private static class ThroughputTotalMetric extends CumulativeSum {
    final Map<MetricName, KafkaMetric> throughputTotalMetrics = new HashMap<>();

    ThroughputTotalMetric(final KafkaMetric metric) {
      add(metric);
    }

    private void add(final KafkaMetric metric) {
      throughputTotalMetrics.put(metric.metricName(), metric);
    }

    private void remove(final MetricName metric) {
      throughputTotalMetrics.remove(metric);
    }

    @Override
    public double measure(final MetricConfig metricConfig, final long now) {
      return throughputTotalMetrics
        .values()
         .stream()
         .map(m -> (Double) m.metricValue())
        .reduce(Double::sum)
        .orElse(0D);
    }
  }

}
