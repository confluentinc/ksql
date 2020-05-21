/*
 * Copyright 2019 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"; you may not use
 * this file except in compliance with the License. You may obtain a copy of the
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

import static io.confluent.ksql.util.GrammaticalJoiner.and;
import static java.util.Objects.requireNonNull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import io.confluent.ksql.execution.builder.KsqlQueryBuilder;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.expression.tree.UnqualifiedColumnReferenceExp;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.planner.Projection;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.services.KafkaTopicClient;
import io.confluent.ksql.structured.SchemaKStream;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

public class RepartitionNode extends PlanNode {

  private final PlanNode source;
  private final Expression originalPartitionBy;
  private final Expression partitionBy;
  private final LogicalSchema schema;
  private final boolean internal;

  public RepartitionNode(
      final PlanNodeId id,
      final PlanNode source,
      final LogicalSchema schema,
      final Expression originalPartitionBy,
      final Expression partitionBy,
      final boolean internal
  ) {
    super(id, source.getNodeOutputType(), source.getSourceName());
    this.schema = requireNonNull(schema, "schema");
    this.source = requireNonNull(source, "source");
    this.originalPartitionBy = requireNonNull(originalPartitionBy, "originalPartitionBy");
    this.partitionBy = requireNonNull(partitionBy, "partitionBy");
    this.internal = internal;
  }

  @Override
  public LogicalSchema getSchema() {
    return schema;
  }

  @Override
  public List<PlanNode> getSources() {
    return ImmutableList.of(source);
  }

  @Override
  protected int getPartitions(final KafkaTopicClient kafkaTopicClient) {
    return source.getPartitions(kafkaTopicClient);
  }

  @Override
  public SchemaKStream<?> buildStream(final KsqlQueryBuilder builder) {
    return source.buildStream(builder)
        .selectKey(
            partitionBy,
            builder.buildNodeContext(getId().toString())
        );
  }

  @VisibleForTesting
  public Expression getPartitionBy() {
    return partitionBy;
  }

  public Expression resolveSelect(final int idx, final Expression expression) {
    return partitionBy.equals(expression)
        ? new UnqualifiedColumnReferenceExp(Iterables.getOnlyElement(getSchema().key()).name())
        : expression;
  }

  @Override
  public Stream<ColumnName> resolveSelectStar(
      final Optional<SourceName> sourceName
  ) {
    if (sourceName.isPresent() && !sourceName.equals(getSourceName())) {
      throw new IllegalArgumentException("Expected sourceName of " + getSourceName()
          + ", but was " + sourceName.get());
    }

    if (internal) {
      // An internal repartition is an impl detail, so should not change the set of columns:
      return source.resolveSelectStar(sourceName);
    }

    // Note: the 'value' columns include the key columns at this point:
    return orderColumns(getSchema().value(), getSchema());
  }

  @Override
  void validateKeyPresent(final SourceName sinkName, final Projection projection) {
    if (!projection.containsExpression(partitionBy)) {
      final ImmutableList<Expression> keys = ImmutableList.of(originalPartitionBy);
      throwKeysNotIncludedError(sinkName, "partitioning expression", keys, and());
    }
  }
}
