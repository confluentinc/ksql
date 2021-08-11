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

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.metrics.MetricCollectors;
import java.io.File;
import java.math.BigInteger;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.metrics.Gauge;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.MetricsContext;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.streams.StreamsConfig;

public class StorageUtilizationMetrics implements MetricsReporter {
  private final Map<String, Map<String, TaskStorageMetric>> metricsSeen;
  private final Metrics metricRegistry;
  static AtomicBoolean registeredNodeMetrics = new AtomicBoolean(false);

  public StorageUtilizationMetrics() {
    this(MetricCollectors.getMetrics());
  }

  public StorageUtilizationMetrics(final Metrics metricRegistry) {
    this.metricsSeen = new HashMap<>();
    this.metricRegistry = metricRegistry;
  }

  // for testing
  public StorageUtilizationMetrics(
      final Metrics metricRegistry,
      final Map<String, Map<String, TaskStorageMetric>> taskMetrics) {
    this.metricsSeen = taskMetrics;
    this.metricRegistry = metricRegistry;
  }

  @Override
  public void init(final List<KafkaMetric> list) {

  }

  @Override
  public void configure(final Map<String, ?> map) {
    final String dir;
    if (map.containsKey(StreamsConfig.STATE_DIR_CONFIG)) {
      dir = map.get(StreamsConfig.STATE_DIR_CONFIG).toString();
    } else {
      dir =  StreamsConfig
        .configDef()
        .defaultValues()
        .get(StreamsConfig.STATE_DIR_CONFIG)
        .toString();
    }
    configureShared(new File(dir), this.metricRegistry);
  }

  private static void configureShared(final File baseDir, final Metrics metricRegistry) {
    if (registeredNodeMetrics.getAndSet(true)) {
      return;
    }
    final MetricName nodeAvailable =
        metricRegistry.metricName("node-storage-available", "ksql-utilization-metrics");
    final MetricName nodeTotal =
        metricRegistry.metricName("node-storage-total", "ksql-utilization-metrics");
    final MetricName nodeUsed =
        metricRegistry.metricName("node-storage-used", "ksql-utilization-metrics");

    metricRegistry.addMetric(
        nodeAvailable,
        (Gauge<Long>) (config, now) -> baseDir.getFreeSpace()
    );
    metricRegistry.addMetric(
        nodeTotal,
        (Gauge<Long>) (config, now) -> baseDir.getFreeSpace()
    );
    metricRegistry.addMetric(
        nodeUsed,
        (Gauge<Long>) (config, now) -> baseDir.getFreeSpace()
    );
  }

  @Override
  public void metricChange(final KafkaMetric metric) {
    if (!metric.metricName().name().equals("total-sst-files-size")) {
      return;
    }
    final String taskId = metric.metricName().tags().getOrDefault("task-id", "");
    final String queryId = getQueryId(metric);

    // if we haven't seen a task for this query yet
    synchronized (metricsSeen) {
      if (!metricsSeen.containsKey(queryId)) {
        metricRegistry.addMetric(
            metricRegistry.metricName(
            "query-storage-usage",
            "ksql-utilization-metrics",
            ImmutableMap.of("query-id", queryId)),
            (Gauge<BigInteger>) (config, now) -> computeQueryMetric(queryId)
        );
        metricsSeen.put(queryId, new HashMap<>());
      }
      final TaskStorageMetric newMetric;
      // We haven't seen a metric for this query's task before
      if (!metricsSeen.get(queryId).containsKey(taskId)) {
        // create a new task level metric to track state store storage usage
        newMetric = new TaskStorageMetric(
          metricRegistry.metricName(
            "task-storage-usage",
            "ksql-utilization-metrics",
            ImmutableMap.of("task-id", taskId, "query-id", queryId)
          ));
        // add to list of seen task metrics for this query
        metricsSeen.get(queryId).put(taskId, newMetric);
        // create gauge for task level storage usage
        metricRegistry.addMetric(
            newMetric.metricName,
            (Gauge<BigInteger>) (config, now) -> newMetric.getValue()
        );
      } else {
        // We have this metric already
        newMetric = metricsSeen.get(queryId).get(taskId);
      }
      newMetric.add(metric);
    }
  }

  @Override
  public void metricRemoval(final KafkaMetric metric) {
    final MetricName metricName = metric.metricName();
    if (!metricName.name().equals("total-sst-files-size")) {
      return;
    }

    final String queryId = getQueryId(metric);
    final String taskId = metric.metricName().tags().getOrDefault("task-id", "");
    final TaskStorageMetric taskMetric = metricsSeen.get(queryId).get(taskId);

    // remove storage metric for this task
    taskMetric.remove(metric);
    if (taskMetric.metrics.size() == 0) {
      // no more storage metrics for this task, can remove task gauge
      metricRegistry.removeMetric(taskMetric.metricName);
      metricsSeen.get(queryId).remove(taskId);
      if (metricsSeen.get(queryId).size() == 0) {
        // we've removed all the task metrics for this query, don't need the query metrics anymore
        metricRegistry.removeMetric(metricRegistry.metricName(
            "query-storage-usage",
            "ksql-utilization-metrics",
            ImmutableMap.of("query-id", queryId))
        );
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

  private BigInteger computeQueryMetric(final String queryId) {
    BigInteger queryMetricSum = BigInteger.ZERO;
    for (Map.Entry<String, TaskStorageMetric> entry : metricsSeen.get(queryId).entrySet()) {
      queryMetricSum = queryMetricSum.add(entry.getValue().getValue());
    }
    return queryMetricSum;
  }

  private String getQueryId(final KafkaMetric metric) {
    final String queryIdTag = metric.metricName().tags().getOrDefault("thread-id", "");
    final Pattern pattern = Pattern.compile("(?<=query_)(.*?)(?=-)");
    final Matcher matcher = pattern.matcher(queryIdTag);
    return matcher.find() ? matcher.group(1) : "";
  }

  public static class TaskStorageMetric {
    final MetricName metricName;
    private final Map<MetricName, KafkaMetric> metrics = new ConcurrentHashMap<>();

    TaskStorageMetric(final MetricName metricName) {
      this.metricName = metricName;
    }

    private void add(final KafkaMetric metric) {
      metrics.put(metric.metricName(), metric);
    }

    private void remove(final KafkaMetric metric) {
      metrics.remove(metric.metricName());
    }

    public BigInteger getValue() {
      BigInteger newValue = BigInteger.ZERO;
      for (KafkaMetric metric : metrics.values()) {
        final BigInteger value = (BigInteger) metric.metricValue();
        newValue = newValue.add(value);
      }
      return newValue;
    }

    @Override
    public boolean equals(final Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      final TaskStorageMetric storageMetric = (TaskStorageMetric) o;
      return Objects.equals(metricName, storageMetric.metricName);
    }
  }

}
