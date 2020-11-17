/*
 * Copyright 2020 Confluent Inc.
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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import io.confluent.ksql.execution.builder.KsqlQueryBuilder;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.expression.tree.NullLiteral;
import io.confluent.ksql.execution.expression.tree.UnqualifiedColumnReferenceExp;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.planner.Projection;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.serde.ValueFormat;
import io.confluent.ksql.structured.SchemaKStream;
import java.util.Optional;
import java.util.stream.Stream;

public class UserRepartitionNode extends SingleSourcePlanNode {

  private final Expression partitionBy;
  private final LogicalSchema schema;
  private final Expression originalPartitionBy;
  private final ValueFormat valueFormat;

  public UserRepartitionNode(
      final PlanNodeId id,
      final PlanNode source,
      final LogicalSchema schema,
      final Expression originalPartitionBy,
      final Expression partitionBy
  ) {
    super(id, source.getNodeOutputType(), source.getSourceName(), source);
    this.schema = requireNonNull(schema, "schema");
    this.partitionBy = requireNonNull(partitionBy, "partitionBy");
    this.originalPartitionBy = requireNonNull(originalPartitionBy, "originalPartitionBy");
    this.valueFormat = getLeftmostSourceNode()
        .getDataSource()
        .getKsqlTopic()
        .getValueFormat();
  }

  @Override
  public LogicalSchema getSchema() {
    return schema;
  }

  @Override
  public SchemaKStream<?> buildStream(final KsqlQueryBuilder builder) {
    return getSource().buildStream(builder)
        .selectKey(
            valueFormat.getFormatInfo(),
            partitionBy,
            Optional.empty(),
            builder.buildNodeContext(getId().toString()),
            false
        );
  }

  @VisibleForTesting
  public Expression getPartitionBy() {
    return partitionBy;
  }

  @Override
  public Expression resolveSelect(final int idx, final Expression expression) {
    // after issuing a PARTITION BY, there will only be one key column because we
    // do not support PARTITION BY multiple columns
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

    // Note: the 'value' columns include the key columns at this point:
    return orderColumns(getSchema().value(), getSchema());
  }

  @Override
  void validateKeyPresent(final SourceName sinkName, final Projection projection) {
    if (!(partitionBy instanceof NullLiteral) && !projection.containsExpression(partitionBy)) {
      final ImmutableList<Expression> keys = ImmutableList.of(originalPartitionBy);
      throwKeysNotIncludedError(sinkName, "partitioning expression", keys);
    }
  }
}
