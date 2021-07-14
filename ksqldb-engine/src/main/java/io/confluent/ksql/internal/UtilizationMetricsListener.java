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
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.util.QueryMetadata;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.KafkaStreams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UtilizationMetricsListener implements Runnable, QueryEventListener {

  private final List<KafkaStreams> kafkaStreams;
  private final Logger logger = LoggerFactory.getLogger(UtilizationMetricsListener.class);
  private final List<String> metrics;
  private final Time time;
  private long lastSampleTime;

  public UtilizationMetricsListener() {
    this.kafkaStreams = new ArrayList<>();
    this.metrics = new LinkedList<>();
    time = Time.SYSTEM;
    lastSampleTime = time.milliseconds();
  }

  @Override
  public void onCreate(
      final ServiceContext serviceContext,
      final MetaStore metaStore,
      final QueryMetadata queryMetadata) {
    kafkaStreams.add(queryMetadata.getKafkaStreams());
  }

  @Override
  public void onDeregister(final QueryMetadata query) {
    final KafkaStreams streams = query.getKafkaStreams();
    kafkaStreams.remove(streams);
  }

  @Override
  public void run() {
    logger.info("Reporting Observability Metrics");
    final Long currentTime = time.milliseconds();
    // here is where we would report metrics
    lastSampleTime = currentTime;
  }
}
