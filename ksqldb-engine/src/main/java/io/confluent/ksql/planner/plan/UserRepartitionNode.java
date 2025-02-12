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
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.execution.expression.tree.Expression;
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

  private final ImmutableList<Expression> partitionBys;
  private final LogicalSchema schema;
  private final ImmutableList<Expression> originalPartitionBys;
  private final ValueFormat valueFormat;

  @SuppressFBWarnings("EI_EXPOSE_REP2")
  public UserRepartitionNode(
      final PlanNodeId id,
      final PlanNode source,
      final LogicalSchema schema,
      final List<Expression> originalPartitionBys,
      final List<Expression> partitionBys
  ) {
    super(id, source.getNodeOutputType(), source.getSourceName(), source);
    this.schema = requireNonNull(schema, "schema");
    this.partitionBys = ImmutableList.copyOf(requireNonNull(partitionBys, "partitionBys"));
    this.originalPartitionBys = ImmutableList.copyOf(
        requireNonNull(originalPartitionBys, "originalPartitionBys")
    );
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
  public SchemaKStream<?> buildStream(final PlanBuildContext buildContext) {
    return getSource().buildStream(buildContext)
        .selectKey(
            valueFormat.getFormatInfo(),
            partitionBys,
            Optional.empty(),
            buildContext.buildNodeContext(getId().toString()),
            false
        );
  }

  @VisibleForTesting
  @SuppressFBWarnings(value = "EI_EXPOSE_REP", justification = "partitionBys is ImmutableList")
  public List<Expression> getPartitionBys() {
    return partitionBys;
  }

  @Override
  public Expression resolveSelect(final int idx, final Expression expression) {
    for (int i = 0; i < partitionBys.size(); i++) {
      if (partitionBys.get(i).equals(expression)) {
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
    if (!PartitionByParamsFactory.isPartitionByNull(partitionBys)
        && !containsExpressions(projection, partitionBys)) {
      throwKeysNotIncludedError(sinkName, "partitioning expression", originalPartitionBys);
    }
  }

  private static boolean containsExpressions(
      final Projection projection,
      final List<Expression> expressions
  ) {
    return expressions.stream().allMatch(projection::containsExpression);
  }
}
