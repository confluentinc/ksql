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
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import org.apache.kafka.common.Metric;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UtilizationMetricsListener implements Runnable, QueryEventListener {
  private final ConcurrentHashMap<QueryId, KafkaStreams> kafkaStreams;
  private final List<File> streamsDirectories;
  private final Logger logger = LoggerFactory.getLogger(UtilizationMetricsListener.class);
  private final List<String> metrics;
  private Instant lastSampleTime;
  private final File baseDir;

  public UtilizationMetricsListener(final KsqlConfig config) {
    this.kafkaStreams = new ConcurrentHashMap<>();
    this.metrics = new ArrayList<>();
    this.streamsDirectories = new ArrayList<>();
    this.baseDir = new File(config.getKsqlStreamConfigProps()
      .getOrDefault(
        StreamsConfig.STATE_DIR_CONFIG,
        StreamsConfig.configDef().defaultValues().get(StreamsConfig.STATE_DIR_CONFIG))
      .toString());
  }

  // for testing
  public UtilizationMetricsListener(
      final ConcurrentHashMap<QueryId, KafkaStreams> kafkaStreams,
      final List<File> streamsDirectories,
      final File baseDir) {
    this.kafkaStreams = kafkaStreams;
    this.metrics = new ArrayList<>();
    this.streamsDirectories = streamsDirectories;
    this.baseDir = baseDir;
  }

  @Override
  public void onCreate(
      final ServiceContext serviceContext,
      final MetaStore metaStore,
      final QueryMetadata queryMetadata) {
    kafkaStreams.put(queryMetadata.getQueryId(), queryMetadata.getKafkaStreams());
    streamsDirectories.add(new File(baseDir, queryMetadata.getQueryApplicationId()));
  }

  @Override
  public void onDeregister(final QueryMetadata query) {
    kafkaStreams.remove(query.getQueryId());
    streamsDirectories.remove(new File(baseDir, query.getQueryApplicationId()));
  }

  @Override
  public void run() {
    logger.info("Reporting Observability Metrics");
    final Instant currentTime = Instant.now();
    logger.info("Reporting CSU thread level metrics");
    final List<MetricsReporter.DataPoint> dataPoints = new ArrayList<>();
    nodeDiskUsage(dataPoints, currentTime);
    taskDiskUsage(dataPoints, currentTime);
    // here is where we would call report() for telemetry reporter
    lastSampleTime = currentTime;
  }

  public void nodeDiskUsage(
      final List<MetricsReporter.DataPoint> dataPoints,
      final Instant sampleTime) {
    for (File f : streamsDirectories) {
      final double freeSpace = f.getFreeSpace();
      final long totalSpace = f.getTotalSpace();
      final double percFree = percentage(freeSpace, (double) totalSpace);
      dataPoints.add(new MetricsReporter.DataPoint(sampleTime,"storage-usage", freeSpace));
      dataPoints.add(new MetricsReporter.DataPoint(sampleTime,"storage-usage-perc", percFree));
      logger.info("The disk usage for {} is {}", f.getName(), freeSpace);
      logger.info("The % disk space free for {} is {}%", f.getName(), percFree);
    }
  }

  public void taskDiskUsage(
      final List<MetricsReporter.DataPoint> dataPoints,
      final Instant sampleTime) {
    for (Map.Entry<QueryId, KafkaStreams> streams : kafkaStreams.entrySet()) {
      final List<Metric> fileSizePerTask = streams.getValue().metrics().values().stream()
          .filter(m -> m.metricName().name().equals("total-sst-files-size"))
          .collect(Collectors.toList());
      double totalDiskUsage = 0.0;
      for (Metric m : fileSizePerTask) {
        logger.info("Recording task level storage usage for {}", streams.getKey());
        final BigInteger usage = (BigInteger) m.metricValue();
        dataPoints.add(new MetricsReporter.DataPoint(
            sampleTime,
            "task-storage-usage",
            usage.doubleValue(),
            ImmutableMap.of(
            "task-id", m.metricName().tags().getOrDefault("task-id", ""),
            "query-id", streams.getKey().toString()
          ))
        );
        logger.info(
            "Storage usage for task {} is {}",
            m.metricName().tags().getOrDefault("task-id", ""),
            usage
        );
        totalDiskUsage += usage.doubleValue();
      }
      logger.info("Total storage usage for query {} is {}",streams.getKey(), totalDiskUsage);
      dataPoints.add(new MetricsReporter.DataPoint(
          sampleTime,
          "query-storage-usage",
          totalDiskUsage,
          ImmutableMap.of("query-id", streams.getKey().toString()))
      );
    }
  }

  private double percentage(final double b, final double w) {
    return Math.round((b / w) * 100);
  }
}
