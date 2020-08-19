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

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.execution.expression.tree.ColumnReferenceExp;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.expression.tree.UnqualifiedColumnReferenceExp;
import io.confluent.ksql.execution.plan.SelectExpression;
import io.confluent.ksql.execution.util.ExpressionTypeManager;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.parser.tree.AllColumns;
import io.confluent.ksql.parser.tree.SelectItem;
import io.confluent.ksql.parser.tree.SingleColumn;
import io.confluent.ksql.schema.ksql.Column;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.LogicalSchema.Builder;
import io.confluent.ksql.schema.ksql.types.SqlType;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public final class SelectionUtil {

  private SelectionUtil() {
  }

  public static LogicalSchema buildProjectionSchema(
      final LogicalSchema parentSchema,
      final List<SelectExpression> projection,
      final FunctionRegistry functionRegistry
  ) {
    final ExpressionTypeManager expressionTypeManager = new ExpressionTypeManager(
        parentSchema,
        functionRegistry
    );

    final Builder builder = LogicalSchema.builder();

    final ImmutableMap.Builder<ColumnName, SqlType> keys = ImmutableMap.builder();

    for (final SelectExpression select : projection) {
      final Expression expression = select.getExpression();

      final SqlType expressionType = expressionTypeManager
          .getExpressionSqlType(expression);

      final boolean keyColumn = expression instanceof ColumnReferenceExp
          && parentSchema.isKeyColumn(((ColumnReferenceExp) expression).getColumnName());

      if (keyColumn) {
        builder.keyColumn(select.getAlias(), expressionType);
        keys.put(select.getAlias(), expressionType);
      } else {
        builder.valueColumn(select.getAlias(), expressionType);
      }
    }

    return builder.build();
  }

  public static List<SelectExpression> buildSelectExpressions(
      final PlanNode parentNode,
      final List<? extends SelectItem> selectItems,
      final Optional<LogicalSchema> targetSchema
  ) {
    return IntStream.range(0, selectItems.size())
        .boxed()
        .flatMap(idx -> resolveSelectItem(idx, selectItems, parentNode, targetSchema))
        .collect(Collectors.toList());
  }

  private static Stream<SelectExpression> resolveSelectItem(
      final int idx,
      final List<? extends SelectItem> selectItems,
      final PlanNode parentNode,
      final Optional<LogicalSchema> targetSchema
  ) {
    final SelectItem selectItem = selectItems.get(idx);

    if (selectItem instanceof SingleColumn) {
      final SingleColumn column = (SingleColumn) selectItem;
      final Optional<Column> targetColumn = targetSchema.map(schema -> schema.columns().get(idx));

      return resolveSingleColumn(idx, parentNode, column, targetColumn);
    }

    if (selectItem instanceof AllColumns) {
      return resolveAllColumns(parentNode, (AllColumns) selectItem);
    }

    throw new IllegalArgumentException(
        "Unsupported SelectItem type: " + selectItem.getClass().getName());
  }

  private static Stream<SelectExpression> resolveSingleColumn(
      final int idx,
      final PlanNode parentNode,
      final SingleColumn column,
      final Optional<Column> targetColumn
  ) {
    final Expression expression = parentNode.resolveSelect(idx, column.getExpression());
    final ColumnName alias = column.getAlias()
        .orElseThrow(() -> new IllegalStateException("Alias should be present by this point"));

    return Stream.of(SelectExpression.of(
        alias,
        targetColumn
            .map(col -> ImplicitlyCastResolver.resolve(expression, col.type()))
            .orElse(expression))
    );
  }

  private static Stream<SelectExpression> resolveAllColumns(
      final PlanNode parentNode,
      final AllColumns allColumns
  ) {
    final Stream<ColumnName> columns = parentNode
        .resolveSelectStar(allColumns.getSource());

    return columns
        .map(name -> SelectExpression.of(name, new UnqualifiedColumnReferenceExp(
            allColumns.getLocation(),
            name
        )));
  }
}
