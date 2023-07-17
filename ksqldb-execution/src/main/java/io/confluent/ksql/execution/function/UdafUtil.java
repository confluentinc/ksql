/*
 * Copyright 2019 Confluent Inc.
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

package io.confluent.ksql.execution.function;

import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.expression.tree.FunctionCall;
import io.confluent.ksql.execution.expression.tree.Literal;
import io.confluent.ksql.execution.expression.tree.UnqualifiedColumnReferenceExp;
import io.confluent.ksql.execution.util.ExpressionTypeManager;
import io.confluent.ksql.function.AggregateFunctionInitArguments;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.function.KsqlAggregateFunction;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.schema.ksql.Column;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public final class UdafUtil {

  private UdafUtil() {
  }

  public static KsqlAggregateFunction<?, ?, ?> resolveAggregateFunction(
      final FunctionRegistry functionRegistry,
      final FunctionCall functionCall,
      final LogicalSchema schema,
      final KsqlConfig config
  ) {
    try {
      final ExpressionTypeManager expressionTypeManager =
          new ExpressionTypeManager(schema, functionRegistry);

      final SqlType argumentType =
          expressionTypeManager.getExpressionSqlType(functionCall.getArguments().get(0));

      // UDAFs only support one non-constant argument, and that argument must be a column reference
      final Expression arg = functionCall.getArguments().get(0);

      final Optional<Column> possibleValueColumn = arg instanceof UnqualifiedColumnReferenceExp
          ? schema.findValueColumn(((UnqualifiedColumnReferenceExp) arg).getColumnName())
          // assume that it is a column reference with no alias
          : schema.findValueColumn(ColumnName.of(arg.toString()));

      final Column valueColumn = possibleValueColumn
          .orElseThrow(() -> new KsqlException("Could not find column for expression: " + arg));

      final AggregateFunctionInitArguments aggregateFunctionInitArguments =
          createAggregateFunctionInitArgs(valueColumn.index(), functionCall, config);

      return functionRegistry.getAggregateFunction(
          functionCall.getName(),
          argumentType,
          aggregateFunctionInitArguments
      );
    } catch (final Exception e) {
      throw new KsqlException("Failed to create aggregate function: " + functionCall, e);
    }
  }

  public static AggregateFunctionInitArguments createAggregateFunctionInitArgs(
      final int udafIndex,
      final FunctionCall functionCall,
      final KsqlConfig config
  ) {
    final List<Expression> args = functionCall.getArguments();

    final List<Object> initArgs = new ArrayList<>(Math.max(0, args.size() - 1));

    // args for index > 0 must be literals:
    for (int idx = 1; idx < args.size(); idx++) {
      final Expression param = args.get(idx);
      if (!(param instanceof Literal)) {
        throw new KsqlException("Parameter " + (idx + 1) + " passed to function "
            + functionCall.getName().text()
            + " must be a literal constant, but was expression: '" + param + "'");
      }

      initArgs.add(((Literal) param).getValue());
    }

    final Map<String, Object> functionConfig = config
        .getKsqlFunctionsConfigProps(functionCall.getName().text());

    return new AggregateFunctionInitArguments(udafIndex, functionConfig, initArgs);
  }
}
