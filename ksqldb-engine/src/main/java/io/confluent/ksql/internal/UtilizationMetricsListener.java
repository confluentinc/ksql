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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
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

public class UtilizationMetricsListener implements MetricsReporter {
  private final Map<String, List<TaskStorageMetric>> metricsSeen;
  private final Metrics metricRegistry;
  private static final Object lock = new Object();
  private static List<MetricName> registeredNodeMetrics = null;


  public UtilizationMetricsListener() {
    this(MetricCollectors.getMetrics());
  }

  public UtilizationMetricsListener(final Metrics metricRegistry) {
    this.metricsSeen = new HashMap<>();
    this.metricRegistry = metricRegistry;
  }

  // for testing
  public UtilizationMetricsListener(
      final Metrics metricRegistry,
      final Map<String, List<TaskStorageMetric>> taskMetrics) {
    this.metricsSeen = taskMetrics;
    this.metricRegistry = metricRegistry;
  }

  @Override
  public void init(List<KafkaMetric> list) {

  }


  @Override
  public void configure(Map<String, ?> map) {
    String dir;
    if (map.containsKey(StreamsConfig.STATE_DIR_CONFIG)) {
      dir = map.get(StreamsConfig.STATE_DIR_CONFIG).toString();
    } else {
      dir =  StreamsConfig.configDef().defaultValues().get(StreamsConfig.STATE_DIR_CONFIG).toString();
    }
    configureShared(new File(dir), this.metricRegistry);
  }

  private static void configureShared(final File baseDir, final Metrics metricRegistry) {
    synchronized (lock) {
      if (registeredNodeMetrics != null) {
        return;
      }
      registeredNodeMetrics = new ArrayList<>();

      final MetricName nodeAvailable = metricRegistry.metricName("node-storage-available", "ksql-utilization-metrics");
      final MetricName nodeTotal = metricRegistry.metricName("node-storage-total", "ksql-utilization-metrics");
      final MetricName nodeUsed = metricRegistry.metricName("node-storage-used", "ksql-utilization-metrics");
      registeredNodeMetrics.add(nodeAvailable);
      registeredNodeMetrics.add(nodeTotal);
      registeredNodeMetrics.add(nodeUsed);

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
  }

  @Override
  public void metricChange(final KafkaMetric metric) {
    if (!metric.metricName().name().equals("total-sst-files-size")) {
      return;
    }
    final String taskId = metric.metricName().tags().getOrDefault("task-id", "");
    final String queryIdTag = metric.metricName().tags().getOrDefault("thread-id", "");
    Pattern pattern = Pattern.compile("(?<=query_)(.*?)(?=-)");
    Matcher matcher = pattern.matcher(queryIdTag);
    final String queryId = matcher.find() ? matcher.group(1) : "";
    // if we haven't seen a task for this query yet
    if (!metricsSeen.containsKey(queryId)) {
      metricRegistry.addMetric(
        metricRegistry.metricName(
          "query-storage-usage",
          "ksql-utilization-metrics",
          ImmutableMap.of("query-id", queryId)),
        (Gauge<BigInteger>) (config, now) -> computeQueryMetric(queryId)
      );
      metricsSeen.put(queryId, new ArrayList<>());
    }
    final TaskStorageMetric newMetric = new TaskStorageMetric(
      metricRegistry.metricName(
        "task-storage-usage",
        "ksql-utilization-metrics",
        ImmutableMap.of("task-id", taskId, "query-id", queryId)
      ));
    // We've seen a metric for this query's task before
    if (metricsSeen.get(queryId).contains(newMetric)) {
      // we have this task metric already, just need to update its value
      resetAndUpdateMetric(metric, taskId, queryId);
    } else {
      // create a new task level metric to track state store storage usage
      newMetric.add(metric);
      // create gauge for task level storage usage
      metricRegistry.addMetric(newMetric.metricName, (Gauge<BigInteger>) (config, now) -> newMetric.getValue());

      // add to list of seen task metrics for this query
      metricsSeen.get(queryId).add(newMetric);
    }
  }

  @Override
  public void metricRemoval(final KafkaMetric metric) {
    final MetricName metricName = metric.metricName();
    if (!metricName.name().equals("total-sst-files-size")) {
      return;
    }

    final String queryId = metric.metricName().tags().getOrDefault("app-id", "");
    final List<TaskStorageMetric> taskMetrics = metricsSeen.get(queryId);
    // will delete task / query gauges if we don't have any more metrics for them
    for (TaskStorageMetric m : taskMetrics) {
      // if don't have any store metrics for this task
      m.remove(metric);
      if (m.metrics.size() == 0) {
        // we can get rid of task level metric, there are no sst-metrics anymore
        metricRegistry.removeMetric(m.metricName);
        metricsSeen.get(queryId).remove(m);
        // if there are no task level metrics for this query
        if (taskMetrics.size() == 0){
          // we don't have more task metrics for the query, remove query gauge
          metricRegistry.removeMetric(metricRegistry.metricName(
            "query-storage-usage",
            "ksql-utilization-metrics",
            ImmutableMap.of("query-id", queryId))
          );
          metricsSeen.remove(queryId);
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
  public void validateReconfiguration(Map<String, ?> configs) throws ConfigException {

  }

  @Override
  public void reconfigure(Map<String, ?> configs) {

  }

  @Override
  public void contextChange(MetricsContext metricsContext) {

  }

  public void resetAndUpdateMetric(final KafkaMetric metric, final String taskId, final String queryId) {
    final List<TaskStorageMetric> taskMetrics = metricsSeen.get(queryId);
    taskMetrics.forEach(r -> r.remove(metric));
    for (TaskStorageMetric storageMetric : taskMetrics) {
      if (storageMetric.getTaskId().equals(taskId)) {
        storageMetric.add(metric);
      }
    }
  }

  final BigInteger computeQueryMetric(final String queryId) {
    BigInteger queryMetricSum = BigInteger.ZERO;
    for (TaskStorageMetric m : metricsSeen.get(queryId)) {
      queryMetricSum = queryMetricSum.add(m.getValue());
    }
    return queryMetricSum;
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

    public String getTaskId() {
      return metricName.tags().getOrDefault("task-id", "");
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
