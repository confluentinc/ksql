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

import io.confluent.ksql.engine.QueryEventListener;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.QueryMetadata;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class UtilizationMetricsListener implements Runnable, QueryEventListener {
  private final Map<QueryId, KafkaStreams> kafkaStreams;
  private final List<File> streamsDirectories;
  private final Logger logger = LoggerFactory.getLogger(UtilizationMetricsListener.class);
  private final List<String> metrics;
  private final Time time;
  private long lastSampleTime;
  private final File baseDir;

  public UtilizationMetricsListener(final KsqlConfig config) {
    this.kafkaStreams = new HashMap<>();
    this.metrics = new ArrayList<>();
    this.streamsDirectories = new ArrayList<>();
    this.baseDir = new File(config.getKsqlStreamConfigProps()
      .getOrDefault(
        StreamsConfig.STATE_DIR_CONFIG,
        StreamsConfig.configDef().defaultValues().get(StreamsConfig.STATE_DIR_CONFIG))
      .toString());
    time = Time.SYSTEM;
  }

    // for testing
  public UtilizationMetricsListener(final HashMap<QueryId, KafkaStreams> kafkaStreams, final List<File> streamsDirectories, final File baseDir) {
    this.kafkaStreams = kafkaStreams;
    this.metrics = new ArrayList<>();
    this.streamsDirectories = streamsDirectories;
    this.baseDir = baseDir;
    time = Time.SYSTEM;
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
    final Long currentTime = time.milliseconds();
    // here is where we would report metrics
    logger.info("Reporting CSU thread level metrics");
    nodeDiskUsage();
    taskDiskUsage();
    lastSampleTime = currentTime;
  }

  private double percentage(final double b, final double w) {
    return Math.round((b / w) * 100);
  }

  public void nodeDiskUsage() {
    for (File f : streamsDirectories) {
      final long freeSpace = f.getFreeSpace();
      final long totalSpace = f.getTotalSpace();
      final double percFree = percentage((double) freeSpace, (double) totalSpace);
      logger.info("The disk usage for {} is {}", f.getName(), freeSpace);
      logger.info("The % disk space free for {} is {}%", f.getName(), percFree);
    }
  }

  public void taskDiskUsage() {
    for (Map.Entry<QueryId, KafkaStreams> streams : kafkaStreams.entrySet()) {
      final List<Metric> fileSizePerTask = streams.getValue().metrics().values().stream()
        .filter(m -> m.metricName().name().equals("total-sst-files-size"))
        .collect(Collectors.toList());
      BigInteger totalDiskUsage = BigInteger.valueOf(0);
      for (Metric m : fileSizePerTask) {
        logger.info("Recording task level disk usage for {}", streams.getKey());
        final BigInteger usage = (BigInteger) m.metricValue();
        logger.info("Disk usage for task {} is {}", m.metricName().tags().getOrDefault("task-id", ""), usage);
        totalDiskUsage.add(usage);
      }
      logger.info("Total disk usage for query {} is {}",streams.getKey(), totalDiskUsage);
    }
  }
}
