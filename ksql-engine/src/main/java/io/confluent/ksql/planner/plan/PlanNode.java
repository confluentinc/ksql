/*
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

package io.confluent.ksql.planner.plan;

import static java.util.Objects.requireNonNull;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.serde.DataSource.DataSourceType;
import io.confluent.ksql.structured.SchemaKStream;
import io.confluent.ksql.util.KafkaTopicClient;
import io.confluent.ksql.util.KsqlConfig;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.streams.StreamsBuilder;


public abstract class PlanNode {

  private final PlanNodeId id;
  private final DataSourceType nodeOutputType;

  protected PlanNode(final PlanNodeId id, final DataSourceType nodeOutputType) {
    requireNonNull(id, "id is null");
    requireNonNull(nodeOutputType, "nodeOutputType is null");
    this.id = id;
    this.nodeOutputType = nodeOutputType;
  }

  @JsonProperty("id")
  public PlanNodeId getId() {
    return id;
  }

  public DataSourceType getNodeOutputType() {
    return nodeOutputType;
  }

  public abstract Schema getSchema();

  public abstract Field getKeyField();

  public abstract List<PlanNode> getSources();

  public <C, R> R accept(final PlanVisitor<C, R> visitor, final C context) {
    return visitor.visitPlan(this, context);
  }

  public StructuredDataSourceNode getTheSourceNode() {
    if (this instanceof StructuredDataSourceNode) {
      return (StructuredDataSourceNode) this;
    } else if (this.getSources() != null && !this.getSources().isEmpty()) {
      return this.getSources().get(0).getTheSourceNode();
    }
    return null;
  }

  protected abstract int getPartitions(KafkaTopicClient kafkaTopicClient);

  public abstract SchemaKStream<?> buildStream(
      StreamsBuilder builder,
      KsqlConfig ksqlConfig,
      KafkaTopicClient kafkaTopicClient,
      FunctionRegistry functionRegistry,
      Map<String, Object> props,
      Supplier<SchemaRegistryClient> schemaRegistryClientFactory);
}
