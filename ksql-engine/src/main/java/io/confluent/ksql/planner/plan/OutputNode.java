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

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.execution.plan.SelectExpression;
import io.confluent.ksql.execution.timestamp.TimestampColumn;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.query.id.QueryIdGenerator;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.services.KafkaTopicClient;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import javax.annotation.concurrent.Immutable;

@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
@Immutable
public abstract class OutputNode
    extends PlanNode {

  private final PlanNode source;
  private final LogicalSchema schema;
  private final OptionalInt limit;
  private final Optional<TimestampColumn> timestampColumn;

  protected OutputNode(
      final PlanNodeId id,
      final PlanNode source,
      final LogicalSchema schema,
      final OptionalInt limit,
      final Optional<TimestampColumn> timestampColumn
  ) {
    super(id, source.getNodeOutputType());

    this.source = requireNonNull(source, "source");
    this.schema = requireNonNull(schema, "schema");
    this.limit = requireNonNull(limit, "limit");
    this.timestampColumn =
        requireNonNull(timestampColumn, "timestampColumn");
  }

  @Override
  public LogicalSchema getSchema() {
    return schema;
  }

  @Override
  public List<PlanNode> getSources() {
    return ImmutableList.of(source);
  }

  public OptionalInt getLimit() {
    return limit;
  }

  public PlanNode getSource() {
    return source;
  }

  @Override
  protected int getPartitions(final KafkaTopicClient kafkaTopicClient) {
    return source.getPartitions(kafkaTopicClient);
  }

  @Override
  public List<SelectExpression> getSelectExpressions() {
    return Collections.emptyList();
  }

  @Override
  public <C, R> R accept(final PlanVisitor<C, R> visitor, final C context) {
    return visitor.visitOutput(this, context);
  }

  public Optional<TimestampColumn> getTimestampColumn() {
    return timestampColumn;
  }

  public abstract QueryId getQueryId(QueryIdGenerator queryIdGenerator);
}
