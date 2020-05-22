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

import com.google.common.collect.ImmutableSet;
import io.confluent.ksql.execution.expression.tree.ColumnReferenceExp;
import io.confluent.ksql.execution.expression.tree.UnqualifiedColumnReferenceExp;
import io.confluent.ksql.execution.plan.SelectExpression;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.parser.tree.SelectItem;
import io.confluent.ksql.planner.Projection;
import io.confluent.ksql.planner.RequiredColumns;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.LogicalSchema.Builder;
import io.confluent.ksql.schema.ksql.SystemColumns;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * The user supplied projection.
 *
 * <p>Used by all plans except those with GROUP BY, which has its own custom projection handling
 * in {@link AggregateNode}.
 */
public class FinalProjectNode extends ProjectNode implements VerifiableNode {

  private final Projection projection;
  private final boolean persistent;
  private Optional<LogicalSchema> schema = Optional.empty();
  private Optional<List<SelectExpression>> selectExpressions = Optional.empty();

  public FinalProjectNode(
      final PlanNodeId id,
      final PlanNode source,
      final List<SelectItem> selectItems,
      final boolean persistent
  ) {
    super(id, source);
    this.projection = Projection.of(selectItems);
    this.persistent = persistent;
  }

  @Override
  public LogicalSchema getSchema() {
    return schema.orElseThrow(IllegalStateException::new);
  }

  @Override
  public List<SelectExpression> getSelectExpressions() {
    return selectExpressions.orElseThrow(IllegalStateException::new);
  }

  @Override
  public void validateKeyPresent(final SourceName sinkName) {
    getSource().validateKeyPresent(sinkName, projection);
  }

  @Override
  public Set<ColumnReferenceExp> validateColumns(
      final FunctionRegistry functionRegistry
  ) {
    final RequiredColumns requiredColumns = RequiredColumns.builder()
        .addAll(projection.singleExpressions())
        .build();

    final Set<ColumnReferenceExp> unknown = getSource().validateColumns(requiredColumns);

    if (!unknown.isEmpty()) {
      return unknown;
    }

    bake(functionRegistry);
    return ImmutableSet.of();
  }

  private void bake(final FunctionRegistry functionRegistry) {
    final LogicalSchema parentSchema = getSource().getSchema();

    final List<SelectExpression> selectExpressions = SelectionUtil
        .buildSelectExpressions(getSource(), projection.selectItems());

    final LogicalSchema schema =
        SelectionUtil.buildProjectionSchema(parentSchema, selectExpressions, functionRegistry);

    if (persistent) {
      // Persistent queries have key columns as key columns - so final projection can exclude them:
      selectExpressions.removeIf(se -> {
        if (se.getExpression() instanceof UnqualifiedColumnReferenceExp) {
          final ColumnName columnName = ((UnqualifiedColumnReferenceExp) se.getExpression())
              .getColumnName();

          // Window bounds columns are currently removed if not aliased:
          if (SystemColumns.isWindowBound(columnName) && se.getAlias().equals(columnName)) {
            return true;
          }

          return parentSchema.isKeyColumn(columnName);
        }
        return false;
      });
    }

    final LogicalSchema nodeSchema;
    if (persistent) {
      nodeSchema = schema.withoutPseudoAndKeyColsInValue();
    } else {
      // Transient queries return key columns in the value, so the projection includes them, and
      // the schema needs to include them too:
      final Builder builder = LogicalSchema.builder();

      builder.keyColumns(parentSchema.key());

      schema.columns()
          .forEach(builder::valueColumn);

      nodeSchema = builder.build();
    }

    this.schema = Optional.of(nodeSchema);
    this.selectExpressions = Optional.of(selectExpressions);

    validate();
  }
}
