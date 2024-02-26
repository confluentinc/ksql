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
import io.confluent.ksql.function.AggregateFunctionFactory;
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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

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

      final List<SqlType> args = functionCall.getArguments().stream()
              .map(expressionTypeManager::getExpressionSqlType)
              .collect(Collectors.toList());

      final AggregateFunctionFactory.FunctionSource func = functionRegistry
              .getAggregateFactory(functionCall.getName())
              .getFunction(args);

      final int totalArgs = functionCall.getArguments().size();

      // All non-constant UDAF arguments must be column references
      final List<Integer> argIndices = functionCall.getArguments().stream()
              .limit(totalArgs - func.initArgs)
              .map((arg) -> {
                final Optional<Column> column;

                if (arg instanceof UnqualifiedColumnReferenceExp) {
                  final UnqualifiedColumnReferenceExp colRef =
                          (UnqualifiedColumnReferenceExp) arg;
                  column = schema.findValueColumn(colRef.getColumnName());
                } else {
                  // assume that it is a column reference with no alias
                  column = schema.findValueColumn(ColumnName.of(arg.toString()));
                }

                return column.orElseThrow(
                        () -> new KsqlException("Could not find column for expression: " + arg)
                );
              }).map(Column::index).collect(Collectors.toList());

      return func.source.apply(createAggregateFunctionInitArgs(
              func.initArgs,
              argIndices,
              functionCall,
              config
      ));
    } catch (final Exception e) {
      throw new KsqlException("Failed to create aggregate function: " + functionCall, e);
    }
  }

  /**
   * Creates the initial arguments for a dummy aggregate function that will not
   * actually be called. For example, this is useful for retrieving the return type
   * in the {@link ExpressionTypeManager}.
   * @param numInitArgs     number of initial arguments. This is necessary as there
   *                        is the potential for the {@link FunctionRegistry} to
   *                        resolve different functions based on the number of initial
   *                        arguments.
   * @param functionCall    the call to the function whose initial arguments should be
   *                        created
   * @return initial arguments for a dummy aggregate function
   */
  public static AggregateFunctionInitArguments createAggregateFunctionInitArgs(
          final int numInitArgs,
          final FunctionCall functionCall
  ) {
    return createAggregateFunctionInitArgs(
            numInitArgs,
            Collections.emptyList(),
            functionCall,
            KsqlConfig.empty()
    );
  }

  public static AggregateFunctionInitArguments createAggregateFunctionInitArgs(
          final int numInitArgs,
          final List<Integer> udafIndices,
          final FunctionCall functionCall,
          final KsqlConfig config
  ) {
    final List<Expression> args = functionCall.getArguments();

    final List<Object> initArgs = new ArrayList<>(numInitArgs);

    // args for index > 0 must be literals:
    for (int idx = args.size() - numInitArgs; idx < args.size(); idx++) {
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

    return new AggregateFunctionInitArguments(udafIndices, functionConfig, initArgs);
  }
}
