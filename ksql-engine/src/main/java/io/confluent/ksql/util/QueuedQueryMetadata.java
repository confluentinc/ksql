/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.util;

import io.confluent.ksql.GenericRow;
import io.confluent.ksql.planner.plan.OutputNode;
import io.confluent.ksql.serde.DataSource;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.Topology;

public class QueuedQueryMetadata extends QueryMetadata {

  private final BlockingQueue<KeyValue<String, GenericRow>> rowQueue;
  private final AtomicBoolean isRunning = new AtomicBoolean(true);

  public QueuedQueryMetadata(
      final String statementString,
      final KafkaStreams kafkaStreams,
      final OutputNode outputNode,
      final String executionPlan,
      final BlockingQueue<KeyValue<String, GenericRow>> rowQueue,
      final DataSource.DataSourceType dataSourceType,
      final String queryApplicationId,
      final Topology topology,
      final Map<String, Object> overriddenProperties,
      final Consumer<QueryMetadata> closeCallback
  ) {
    super(statementString, kafkaStreams, outputNode, executionPlan, dataSourceType,
          queryApplicationId, topology, overriddenProperties, closeCallback);
    this.rowQueue = Objects.requireNonNull(rowQueue, "rowQueue");
  }

  public boolean isRunning() {
    return isRunning.get();
  }

  public BlockingQueue<KeyValue<String, GenericRow>> getRowQueue() {
    return rowQueue;
  }

  @Override
  public boolean equals(final Object o) {
    if (!(o instanceof QueuedQueryMetadata)) {
      return false;
    }

    final QueuedQueryMetadata that = (QueuedQueryMetadata) o;

    return Objects.equals(this.rowQueue, that.rowQueue) && super.equals(o);
  }

  @Override
  public int hashCode() {
    return Objects.hash(rowQueue, super.hashCode());
  }

  public void setLimitHandler(final OutputNode.LimitHandler limitHandler) {
    getOutputNode().setLimitHandler(limitHandler);
  }

  @Override
  public void close() {
    super.close();
    isRunning.set(false);
  }

}
