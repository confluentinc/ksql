/**
 * Copyright 2017 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.confluent.ksql.util;

import io.confluent.ksql.serde.DataSource;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.planner.plan.OutputNode;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.Topology;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;

public class QueuedQueryMetadata extends QueryMetadata {

  private final BlockingQueue<KeyValue<String, GenericRow>> rowQueue;

  public QueuedQueryMetadata(
      final String statementString,
      final KafkaStreams kafkaStreams,
      final OutputNode outputNode,
      final String executionPlan,
      final BlockingQueue<KeyValue<String, GenericRow>> rowQueue,
      final DataSource.DataSourceType dataSourceType,
      final String queryApplicationId,
      final KafkaTopicClient kafkaTopicClient,
      final Topology topology,
      final Map<String, Object> overriddenProperties) {
    super(statementString, kafkaStreams, outputNode, executionPlan, dataSourceType,
          queryApplicationId, kafkaTopicClient, topology, overriddenProperties);
    this.rowQueue = rowQueue;
  }

  public BlockingQueue<KeyValue<String, GenericRow>> getRowQueue() {
    return rowQueue;
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof QueuedQueryMetadata)) {
      return false;
    }

    QueuedQueryMetadata that = (QueuedQueryMetadata) o;

    return Objects.equals(this.rowQueue, that.rowQueue) && super.equals(o);
  }

  @Override
  public int hashCode() {
    return Objects.hash(rowQueue, super.hashCode());
  }
}
