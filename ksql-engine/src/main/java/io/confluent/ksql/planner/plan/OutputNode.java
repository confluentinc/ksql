/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.ksql.planner.plan;

import static java.util.Objects.requireNonNull;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.util.KafkaTopicClient;
import io.confluent.ksql.util.QueryIdGenerator;
import io.confluent.ksql.util.timestamp.TimestampExtractionPolicy;
import java.util.List;
import java.util.Optional;
import javax.annotation.concurrent.Immutable;
import org.apache.kafka.connect.data.Schema;

@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
@Immutable
public abstract class OutputNode
    extends PlanNode {

  private final PlanNode source;
  private final Schema schema;
  private final Optional<Integer> limit;
  private final TimestampExtractionPolicy timestampExtractionPolicy;

  @JsonCreator
  protected OutputNode(
      @JsonProperty("id") final PlanNodeId id,
      @JsonProperty("source") final PlanNode source,
      @JsonProperty("schema") final Schema schema,
      @JsonProperty("limit") final Optional<Integer> limit,
      @JsonProperty("timestamp_policy") final TimestampExtractionPolicy timestampExtractionPolicy) {
    super(id, source.getNodeOutputType());
    requireNonNull(source, "source is null");
    requireNonNull(schema, "schema is null");
    requireNonNull(timestampExtractionPolicy, "timestampExtractionPolicy is null");

    this.source = source;
    this.schema = schema;
    this.limit = limit;
    this.timestampExtractionPolicy = timestampExtractionPolicy;
  }

  @Override
  public Schema getSchema() {
    return this.schema;
  }

  @Override
  public List<PlanNode> getSources() {
    return ImmutableList.of(source);
  }

  public Optional<Integer> getLimit() {
    return limit;
  }

  @JsonProperty
  public PlanNode getSource() {
    return source;
  }

  @Override
  protected int getPartitions(final KafkaTopicClient kafkaTopicClient) {
    return source.getPartitions(kafkaTopicClient);
  }

  @Override
  public <C, R> R accept(final PlanVisitor<C, R> visitor, final C context) {
    return visitor.visitOutput(this, context);
  }

  public TimestampExtractionPolicy getTimestampExtractionPolicy() {
    return timestampExtractionPolicy;
  }

  public abstract QueryId getQueryId(QueryIdGenerator queryIdGenerator);
}
