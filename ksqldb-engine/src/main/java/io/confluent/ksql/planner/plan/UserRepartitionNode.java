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
import io.confluent.ksql.execution.streams.PartitionByParamsFactory;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.planner.Projection;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.serde.ValueFormat;
import io.confluent.ksql.structured.SchemaKStream;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

public class UserRepartitionNode extends SingleSourcePlanNode {

  private final List<Expression> partitionBy;
  private final LogicalSchema schema;
  private final List<Expression> originalPartitionBy;
  private final ValueFormat valueFormat;

  public UserRepartitionNode(
      final PlanNodeId id,
      final PlanNode source,
      final LogicalSchema schema,
      final List<Expression> originalPartitionBy, // TODO: rename?
      final List<Expression> partitionBy
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
  public List<Expression> getPartitionBy() {
    return partitionBy;
  }

  @Override
  public Expression resolveSelect(final int idx, final Expression expression) {
    for (int i = 0; i < partitionBy.size(); i++) {
      if (partitionBy.get(i).equals(expression)) {
        return new UnqualifiedColumnReferenceExp(getSchema().key().get(i).name());
      }
    }

    return expression;
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
    if (!PartitionByParamsFactory.isPartitionByNull(partitionBy)
        && !containsExpressions(projection, partitionBy)) {
      final ImmutableList<Expression> keys = ImmutableList.copyOf(originalPartitionBy);
      // TODO: fix error message to only mention the ones that are missing
      throwKeysNotIncludedError(sinkName, "partitioning expression", keys);
    }
  }

  // TODO: clean this up
  private static boolean containsExpressions(
      final Projection projection,
      final List<Expression> expressions
  ) {
    return expressions.stream().allMatch(projection::containsExpression);
  }
}
