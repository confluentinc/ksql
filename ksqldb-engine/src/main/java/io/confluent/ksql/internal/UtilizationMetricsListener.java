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
import io.confluent.ksql.engine.QueryEventListener;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.QueryMetadata;
import java.io.File;
import java.math.BigInteger;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.Gauge;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UtilizationMetricsListener implements Runnable, QueryEventListener {
  private final ConcurrentHashMap<QueryId, KafkaStreams> kafkaStreams;
  private final Logger logger = LoggerFactory.getLogger(UtilizationMetricsListener.class);
  private final Set<TaskStorageMetric> metrics;
  private final Metrics metricRegistry;
  private final File baseDir;

  public UtilizationMetricsListener(final KsqlConfig config, final Metrics metricRegistry) {
    this.kafkaStreams = new ConcurrentHashMap<>();
    this.metrics = new HashSet<>();
    this.baseDir = new File(config.getKsqlStreamConfigProps()
      .getOrDefault(
        StreamsConfig.STATE_DIR_CONFIG,
        StreamsConfig.configDef().defaultValues().get(StreamsConfig.STATE_DIR_CONFIG))
      .toString());
    this.metricRegistry = metricRegistry;

    this.metricRegistry.addMetric(
      metricRegistry.metricName("node-storage-usage", "ksql-utilization-metrics"),
      (Gauge<Long>) (metricsConfig, now) -> baseDir.getFreeSpace()
    );
  }

  // for testing
  public UtilizationMetricsListener(
      final ConcurrentHashMap<QueryId, KafkaStreams> kafkaStreams,
      final File baseDir,
      final Metrics metrics) {
    this.kafkaStreams = kafkaStreams;
    this.metrics = new HashSet<>();
    this.baseDir = baseDir;
    metricRegistry = metrics;

    final MetricName nodeAvailable = metricRegistry.metricName("node-storage-available", "ksql-utilization-metrics");
    final MetricName nodeTotal = metricRegistry.metricName("node-storage-total", "ksql-utilization-metrics");
    final MetricName nodeUsed = metricRegistry.metricName("node-storage-used", "ksql-utilization-metrics");


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
  public void onCreate(
      final ServiceContext serviceContext,
      final MetaStore metaStore,
      final QueryMetadata queryMetadata) {
    kafkaStreams.put(queryMetadata.getQueryId(), queryMetadata.getKafkaStreams());
  }

  @Override
  public void onDeregister(final QueryMetadata query) {
    kafkaStreams.remove(query.getQueryId());
  }

  @Override
  public void run() {
    logger.info("Reporting Observability Metrics");
    logger.info("Reporting storage metrics");
    taskDiskUsage();
  }

  public void taskDiskUsage() {
    final Set<TaskStorageMetric> allMetricsSeen = new HashSet<>();

    for (Map.Entry<QueryId, KafkaStreams> streams : kafkaStreams.entrySet()) {
      final List<Metric> fileSizePerTask = streams.getValue().metrics().values().stream()
          .filter(m -> m.metricName().name().equals("total-sst-files-size"))
          .collect(Collectors.toList());

      long queryDiskUsage = 0L;
      final Set<TaskStorageMetric> updatedTaskLevelMetrics = new HashSet<>();
      for (Metric m : fileSizePerTask) {
        final BigInteger usage = (BigInteger) m.metricValue();
        final String taskId = m.metricName().tags().getOrDefault("task-id", "");
        final TaskStorageMetric newMetric = new TaskStorageMetric(
          metricRegistry.metricName(
            "task-storage-usage",
            "ksql-utilization-metrics",
            ImmutableMap.of("task-id", taskId, "query-id", streams.getKey().toString())
          ));

        if (!updatedTaskLevelMetrics.add(newMetric)) {
          newMetric.updateValue((long) newMetric.value + usage.longValue());
        }
        queryDiskUsage += usage.longValue();
      }

      final TaskStorageMetric queryStorageMetric = new TaskStorageMetric(
        metricRegistry.metricName(
          "query-storage-usage",
          "ksql-utilization-metrics",
          ImmutableMap.of("query-id", streams.getKey().toString())
        ));

      if (!updatedTaskLevelMetrics.add(queryStorageMetric)) {
        queryStorageMetric.updateValue((long) queryStorageMetric.value + queryDiskUsage);
      }
      updatedTaskLevelMetrics.removeAll(metrics);

      // create gauges for all the task level metrics we haven't seen before, add them to our master list
      for (TaskStorageMetric entry : updatedTaskLevelMetrics) {
        metricRegistry.addMetric(entry.metricName, (Gauge<Object>) (config, now) -> entry.getValue());
        metrics.add(entry);
      }

      logger.info("Total storage usage for query {} is {}", streams.getKey(), queryDiskUsage);

      allMetricsSeen.addAll(updatedTaskLevelMetrics);
    }

    final Set<TaskStorageMetric> toRemove = metrics;
    toRemove.removeAll(allMetricsSeen);

    for (TaskStorageMetric entry : toRemove) {
      metricRegistry.removeMetric(entry.metricName);
    }
  }

  private class TaskStorageMetric {
    final MetricName metricName;
    Object value;

    private TaskStorageMetric(final MetricName metricName) {
      this.metricName = metricName;
    }

    private void updateValue(final Object value) {
      this.value = value;
    }

    public Object getValue() {
      return this.value;
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
