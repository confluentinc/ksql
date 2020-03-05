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
import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.execution.timestamp.TimestampColumn;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.query.id.QueryIdGenerator;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.services.KafkaTopicClient;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
@Immutable
public abstract class OutputNode
    extends PlanNode {

  private final PlanNode source;
  private final OptionalInt limit;
  private final Optional<TimestampColumn> timestampColumn;

  protected OutputNode(
      final PlanNodeId id,
      final PlanNode source,
      final LogicalSchema schema,
      final OptionalInt limit,
      final Optional<TimestampColumn> timestampColumn
  ) {
    super(id, source.getNodeOutputType(), schema, source.getSelectExpressions());

    this.source = requireNonNull(source, "source");
    this.limit = requireNonNull(limit, "limit");
    this.timestampColumn =
        requireNonNull(timestampColumn, "timestampColumn");
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
  public <C, R> R accept(final PlanVisitor<C, R> visitor, final C context) {
    return visitor.visitOutput(this, context);
  }

  public Optional<TimestampColumn> getTimestampColumn() {
    return timestampColumn;
  }

  public abstract QueryId getQueryId(QueryIdGenerator queryIdGenerator);
}
