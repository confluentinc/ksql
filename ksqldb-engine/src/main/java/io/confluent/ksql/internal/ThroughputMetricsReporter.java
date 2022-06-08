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

public class ThroughputMetricsReporter implements MetricsReporter {
    private static final Logger LOGGER = LoggerFactory.getLogger(ThroughputMetricsReporter.class);
    private static final String RECORDS_CONSUMED = "records-consumed";
    private static final String BYTES_CONSUMED = "bytes-consumed";
    private static final String RECORDS_PRODUCED = "records-produced";
    private static final String BYTES_PRODUCED = "bytes-produced";
    private static final Set<String> THROUGHPUT_METRIC_NAMES =
        mkSet(RECORDS_CONSUMED, BYTES_CONSUMED, RECORDS_PRODUCED, BYTES_PRODUCED);
    private static final Pattern NAMED_TOPOLOGY_PATTERN = Pattern.compile("(.*?)__\\d*_\\d*");
    private static final Pattern QUERY_ID_PATTERN =
        Pattern.compile("(?<=query_|transient_)(.*?)(?=-)");

    // CHECKSTYLE_RULES.OFF: LineLength
    //
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
        if (!metric.metricName().group().equals(StreamsMetricsImpl.TOPIC_LEVEL_GROUP)) {
            return;
        }
        addMetric(
            metric,
            getQueryId(metric),
            getTopicName(metric)
        );
    }

    @Override
    public void metricRemoval(final KafkaMetric metric) {
        final MetricName metricName = metric.metricName();
        if (!THROUGHPUT_METRIC_NAMES.contains(metricName.name())) {
            return;
        }

        removeMetric(
            metric,
            getQueryId(metric),
            getTopicName(metric)
        );
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

    private synchronized void addMetric(
        final KafkaMetric metric,
        final String queryId,
        final String topicName
        ) {
        final MetricName metricName = metric.metricName();
        final Map<String, String> queryMetricTags = getQueryMetricTags(queryId, metric.metricName().tags());
        final Map<String, String> topicMetricTags = getTopicMetricTags(queryMetricTags, topicName);

        LOGGER.debug("Adding {} metric for query={} and topic={}", metricName.name(), queryId, topicName);

        if (!registeredMetrics.containsKey(queryId)) {
            registeredMetrics.put(queryId, new HashMap<>());
        }
        if (!registeredMetrics.get(queryId).containsKey(topicName)) {
            registeredMetrics.get(queryId).put(topicName, new HashMap<>());
        }

        if (registeredMetrics.get(queryId).get(topicName).containsKey(metricName)) {
            LOGGER.error("The {} metric has already been added for query={} and topic={}",
                         metricName.name(), queryId, topicName);
        }

        final ThroughputTotalMetric newMetric = new ThroughputTotalMetric(
            metricRegistry.metricName(
                metricName.name(),
                StreamsMetricsImpl.TOPIC_LEVEL_GROUP,
                topicMetricTags),
            metric);
        registeredMetrics.get(queryId).get(topicName).put(metricName, newMetric);

        metricRegistry.addMetric(
            new MetricName(
                metricName.name(),
                StreamsMetricsImpl.TOPIC_LEVEL_GROUP,
                metricName.description(),
                queryMetricTags),
            (config, now) -> registeredMetrics.get(queryId).get(topicName).get(metricName).getValue()
        );
    }

    private synchronized void removeMetric(
        final KafkaMetric metric,
        final String queryId,
        final String topicName
    ) {
        if (registeredMetrics.containsKey(queryId)) {
            if (!registeredMetrics.get(queryId).containsKey(topicName)) {
                metricRegistry.removeMetric(metric.metricName());
                registeredMetrics.get(queryId).get(topicName).remove(metric.metricName());

                if (registeredMetrics.get(queryId).get(topicName).size() == 0) {

                }
            }
        }
    }

    private MetricName getMetricNameWithQueryIdTag(
        final String queryId,
        final String topicName,
        final MetricName metricName
    ) {
        return new MetricName(
            metricName.name(),
            StreamsMetricsImpl.TOPIC_LEVEL_GROUP,
            metricName.description(),
            getTopicMetricTags(getQueryMetricTags(queryId, metricName.tags()), topicName)
        );
    }

    private String getQueryId(final KafkaMetric metric) {
        final String taskName = metric.metricName().tags().getOrDefault("task-id", "");
        final Matcher namedTopologyMatcher = NAMED_TOPOLOGY_PATTERN.matcher(taskName);
        if (namedTopologyMatcher.find()) {
            return namedTopologyMatcher.group(1);
        }

        final String queryIdTag = metric.metricName().tags().getOrDefault("thread-id", "");
        final Matcher matcher = QUERY_ID_PATTERN.matcher(queryIdTag);
        if (matcher.find()) {
            return matcher.group(1);
        } else {
            throw new KsqlException("Missing query ID when reporting total throughput metrics");
        }
    }

    private String getTopicName(final KafkaMetric metric) {
        return metric.metricName().tags().getOrDefault("topic", "");
    }

    private Map<String, String> getQueryMetricTags(
        final String queryId,
        final Map<String, String> metricTags
    ) {
        final Map<String, String> queryMetricTags = new HashMap<>(customTags);
        queryMetricTags.putAll(metricTags);
        queryMetricTags.put("query-id", queryId);
        return ImmutableMap.copyOf(queryMetricTags);
    }

    private Map<String, String> getTopicMetricTags(
        final Map<String, String> taskTags,
        final String topic
    ) {
        final Map<String, String> topicMetricTags = new HashMap<>(taskTags);
        topicMetricTags.put("topic", topic);
        return ImmutableMap.copyOf((topicMetricTags));
    }

    @VisibleForTesting
    static void setTags(final Map<String, String> tags) {
        customTags.clear();
        customTags.putAll(tags);
    }

    private static class ThroughputTotalMetric {
        final MetricName metricName;
        final KafkaMetric metric;

        ThroughputTotalMetric(final MetricName metricName, final KafkaMetric metric) {
            this.metricName = metricName;
            this.metric = metric;
        }

        public double getValue() {
            return (double) metric.metricValue();
        }
    }

}
