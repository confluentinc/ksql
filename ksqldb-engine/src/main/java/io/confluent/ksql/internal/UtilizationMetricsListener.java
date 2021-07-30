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
import io.confluent.ksql.util.KsqlConfig;

import java.io.File;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.metrics.Gauge;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.MetricsContext;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.streams.StreamsConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UtilizationMetricsListener implements MetricsReporter {
  private final ConcurrentHashMap<String, Long> queryStorageMetrics;
  private final Map<String, List<String>> metrics;
  private final Metrics metricRegistry;
  private final File baseDir;
  private final Collection<TaskStorageMetric> registeredMetrics;

  public UtilizationMetricsListener(final KsqlConfig config, final Metrics metricRegistry) {
    this.queryStorageMetrics = new ConcurrentHashMap<>();
    this.metrics = new HashMap<>();
    this.baseDir = new File(config.getKsqlStreamConfigProps()
      .getOrDefault(
        StreamsConfig.STATE_DIR_CONFIG,
        StreamsConfig.configDef().defaultValues().get(StreamsConfig.STATE_DIR_CONFIG))
      .toString());
    this.metricRegistry = metricRegistry;
    this.registeredMetrics = new ArrayList<>();

    final MetricName nodeAvailable = this.metricRegistry.metricName("node-storage-available", "ksql-utilization-metrics");
    final MetricName nodeTotal = this.metricRegistry.metricName("node-storage-total", "ksql-utilization-metrics");
    final MetricName nodeUsed = this.metricRegistry.metricName("node-storage-used", "ksql-utilization-metrics");


    this.metricRegistry.addMetric(
      nodeAvailable,
      (Gauge<Long>) (metricConfig, now) -> baseDir.getFreeSpace()
    );
    this.metricRegistry.addMetric(
      nodeTotal,
      (Gauge<Long>) (metricConfig, now) -> baseDir.getFreeSpace()
    );
    this.metricRegistry.addMetric(
      nodeUsed,
      (Gauge<Long>) (metricConfig, now) -> baseDir.getFreeSpace()
    );
  }

  // for testing
  public UtilizationMetricsListener(
      final ConcurrentHashMap<String, Long> kafkaStreams,
      final File baseDir,
      final Metrics metricRegistry,
      final Map<String, List<String>> taskMetrics) {
    this.queryStorageMetrics = kafkaStreams;
    this.metrics = taskMetrics;
    this.baseDir = baseDir;
    this.metricRegistry = metricRegistry;
    this.registeredMetrics = new ArrayList<>();

    final MetricName nodeAvailable = this.metricRegistry.metricName("node-storage-available", "ksql-utilization-metrics");
    final MetricName nodeTotal = this.metricRegistry.metricName("node-storage-total", "ksql-utilization-metrics");
    final MetricName nodeUsed = this.metricRegistry.metricName("node-storage-used", "ksql-utilization-metrics");


    this.metricRegistry.addMetric(
      nodeAvailable,
      (Gauge<Long>) (config, now) -> baseDir.getFreeSpace()
    );
    this.metricRegistry.addMetric(
      nodeTotal,
      (Gauge<Long>) (config, now) -> baseDir.getFreeSpace()
    );
    this.metricRegistry.addMetric(
      nodeUsed,
      (Gauge<Long>) (config, now) -> baseDir.getFreeSpace()
    );
  }

  @Override
  public void init(List<KafkaMetric> list) {

  }

  @Override
  public void metricChange(final KafkaMetric metric) {
    if (!metric.metricName().name().equals("total-sst-files-size")) {
      return;
    }
    final String taskId = metric.metricName().tags().getOrDefault("task-id", "");

    final String queryId = "";
    // if we haven't seen a task for this query yet
    if (!metrics.containsKey(queryId)) {
      metricRegistry.addMetric(
        metricRegistry.metricName(
          "query-storage-usage",
          "ksql-utilization-metrics",
          ImmutableMap.of("query-id", queryId)),
        (Gauge<Object>) (config, now) -> queryStorageMetrics.get(queryId)
      );
      metrics.put(queryId, new ArrayList<>());
    }
    // We've seen metric for this query before
    if (metrics.get(queryId).contains(taskId)) {
      // we have this task metric already, just need to update it
      resetMetric(metric);
      for (TaskStorageMetric storageMetric : registeredMetrics) {
        if (storageMetric.getTaskId().equals(taskId)) {
          storageMetric.add(metric);
        }
      }
    } else {
      // create a new task level metric to track state stores
      final TaskStorageMetric newMetric = new TaskStorageMetric(
        metricRegistry.metricName(
          "task-storage-usage",
          "ksql-utilization-metrics",
          ImmutableMap.of("task-id", taskId, "query-id", "query-id")
        ));
      newMetric.add(metric);
      registeredMetrics.add(newMetric);
      // create gauge for task / query
      metricRegistry.addMetric(newMetric.metricName, (Gauge<BigInteger>) (config, now) -> newMetric.getValue());

      // add to list of seen metrics
      metrics.get(queryId).add(taskId);
    }
  }

  @Override
  public void metricRemoval(final KafkaMetric metric) {
    final MetricName metricName = metric.metricName();
    if (!metricName.name().equals("total-sst-files-size")) {
      return;
    }

    registeredMetrics.forEach(r -> r.remove(metricName));
    // will maybe delete task / query gauges here
    cleanMetrics();
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

  public void cleanMetrics() {
    for (TaskStorageMetric m : registeredMetrics) {
      // if don't have any store metrics for this task
      if (m.metrics.size() == 0) {
        // we can get rid fo task level metric
        metricRegistry.removeMetric(m.metricName);
        registeredMetrics.remove(m);
        final List<String> taskMetrics = metrics.get("queryId");
        taskMetrics.remove("task-id");
        // if there are no task level metrics for this query
        if (taskMetrics.size() == 0){
          // we don't have more task metrics for the query, remove query gauge
          metricRegistry.removeMetric(metricRegistry.metricName(
            "query-storage-usage",
            "ksql-utilization-metrics",
            ImmutableMap.of("query-id", "query-id")));
        }
      }
    }
  }

  public void resetMetric(final KafkaMetric metric) {
    registeredMetrics.forEach(r -> r.remove(metric.metricName()));
  }

  @Override
  public void configure(Map<String, ?> map) {

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

    private void remove(final MetricName name) {
      metrics.remove(name);
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
