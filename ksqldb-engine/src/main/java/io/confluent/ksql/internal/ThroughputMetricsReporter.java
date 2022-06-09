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
import java.util.regex.Pattern;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.MetricsContext;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.kafka.common.utils.Utils.mkSet;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.PROCESSOR_NODE_ID_TAG;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.TASK_ID_TAG;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.THREAD_ID_TAG;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.TOPIC_NAME_TAG;

public class ThroughputMetricsReporter implements MetricsReporter {

    private static final Logger LOGGER = LoggerFactory.getLogger(ThroughputMetricsReporter.class);
    private static final String QUERY_ID_TAG = "query-id";
    private static final String RECORDS_CONSUMED = "records-consumed-total";
    private static final String BYTES_CONSUMED = "bytes-consumed-total";
    private static final String RECORDS_PRODUCED = "records-produced-total";
    private static final String BYTES_PRODUCED = "bytes-produced-total";
    private static final Set<String> THROUGHPUT_METRIC_NAMES =
        mkSet(RECORDS_CONSUMED, BYTES_CONSUMED, RECORDS_PRODUCED, BYTES_PRODUCED);
    private static final Pattern NAMED_TOPOLOGY_PATTERN = Pattern.compile("(.*?)__\\d*_\\d*");
    private static final Pattern QUERY_ID_PATTERN =
        Pattern.compile("(?<=query-|transient_)(.*?)(?=-)");

    // CHECKSTYLE_RULES.OFF: LineLength
    private static final Map<String, Map<String, Map<MetricName, ThroughputTotalMetric>>> registeredMetrics = new HashMap<>();
    // CHECKSTYLE_RULES.ON: LineLength
    private static final Map<String, String> customTags = new HashMap<>();
    private Metrics metricRegistry;

    @Override
    public void init(final List<KafkaMetric> initial) {
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
        if (!THROUGHPUT_METRIC_NAMES.contains(metric.metricName().name())) {
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
            getThroughputTotalMetricName(queryId, metric.metricName());

        LOGGER.debug("Adding metric {}", throughputTotalMetricName);

        if (!registeredMetrics.containsKey(queryId)) {
            registeredMetrics.put(queryId, new HashMap<>());
        }
        if (!registeredMetrics.get(queryId).containsKey(topic)) {
            registeredMetrics.get(queryId).put(topic, new HashMap<>());
        }

        final ThroughputTotalMetric throughputMetric =
            registeredMetrics.get(queryId).get(topic).get(throughputTotalMetricName);

        if (throughputMetric == null) {
            final ThroughputTotalMetric newThroughputMetric =
                new ThroughputTotalMetric(throughputTotalMetricName, metric);

            registeredMetrics
                .get(queryId)
                .get(topic)
                .put(throughputTotalMetricName, newThroughputMetric);
            metricRegistry.addMetric(
                throughputTotalMetricName,
                (config, now) -> newThroughputMetric.getValue()
            );
        } else {
            throughputMetric.add(metric);
        }
    }

    @Override
    public void metricRemoval(final KafkaMetric metric) {
        if (!THROUGHPUT_METRIC_NAMES.contains(metric.metricName().name())) {
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
            getThroughputTotalMetricName(queryId, metric.metricName());

        LOGGER.debug("Removing metric {}", throughputTotalMetricName);

        if (registeredMetrics.containsKey(queryId) &&
            registeredMetrics.get(queryId).containsKey(topic) &&
            registeredMetrics.get(queryId).get(topic).containsKey(throughputTotalMetricName)) {

            final ThroughputTotalMetric throughputTotalMetric =
                registeredMetrics.get(queryId).get(topic).get(throughputTotalMetricName);

            metricRegistry.removeMetric(throughputTotalMetricName);
            throughputTotalMetric.remove(throughputTotalMetricName);

            if (throughputTotalMetric.metrics.isEmpty()) {
                registeredMetrics.get(queryId).get(topic).remove(throughputTotalMetricName);
                if (registeredMetrics.get(queryId).get(topic).isEmpty()) {
                    registeredMetrics.get(queryId).remove(topic);
                    if (registeredMetrics.get(queryId).isEmpty()) {
                        registeredMetrics.remove(queryId);
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
        final MetricName metricName
    ) {
        return new MetricName(
            metricName.name(),
            StreamsMetricsImpl.TOPIC_LEVEL_GROUP,
            metricName.description() + " by this query",
            getThroughputTotalMetricTags(queryId, metricName.tags())
        );
    }

    private String getQueryId(final KafkaMetric metric) {
        final String taskName = metric.metricName().tags().getOrDefault(TASK_ID_TAG, "");
        final Matcher namedTopologyMatcher = NAMED_TOPOLOGY_PATTERN.matcher(taskName);
        if (namedTopologyMatcher.find()) {
            return namedTopologyMatcher.group(1);
        }

        final String queryIdTag = metric.metricName().tags().getOrDefault(THREAD_ID_TAG, "");
        final Matcher matcher = QUERY_ID_PATTERN.matcher(queryIdTag);
        if (matcher.find()) {
            return matcher.group(1);
        } else {
            LOGGER.error("Can't parse query id from metric {}", metric);
            throw new KsqlException("Missing query ID when reporting total throughput metrics");
        }
    }

    private String getTopic(final KafkaMetric metric) {
        return metric.metricName().tags().getOrDefault(TOPIC_NAME_TAG, "");
    }

    private Map<String, String> getThroughputTotalMetricTags(
        final String queryId,
        final Map<String, String> originalStreamsMetricTags
    ) {
        final Map<String, String> queryMetricTags = new HashMap<>(customTags);
        queryMetricTags.putAll(originalStreamsMetricTags);
        // Remove the taskId and processorNodeId tags as the throughput total metric sums over them
        queryMetricTags.remove(TASK_ID_TAG);
        queryMetricTags.remove(PROCESSOR_NODE_ID_TAG);
        queryMetricTags.put(QUERY_ID_TAG, queryId);
        return ImmutableMap.copyOf(queryMetricTags);
    }

    @VisibleForTesting
    static void setTags(final Map<String, String> tags) {
        customTags.clear();
        customTags.putAll(tags);
    }

    private static class ThroughputTotalMetric {
        final MetricName aggregatedTotalMetric;
        final Map<MetricName, KafkaMetric> metrics = new HashMap<>();

        ThroughputTotalMetric(final MetricName aggregatedTotalMetric, final KafkaMetric metric) {
            this.aggregatedTotalMetric = aggregatedTotalMetric;
            add(metric);
        }

        private void add(final KafkaMetric metric) {
            metrics.put(metric.metricName(), metric);
        }

        private void remove(final MetricName metric) {
            metrics.remove(metric);
        }

        public Double getValue() {
            return metrics
                .values()
                .stream()
                .map(m -> (Double) m.metricValue())
                .reduce(Double::sum)
                .orElse(0D);
        }
    }

}
