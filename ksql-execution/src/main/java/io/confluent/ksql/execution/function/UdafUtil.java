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

import io.confluent.ksql.execution.expression.tree.ColumnReferenceExp;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.expression.tree.FunctionCall;
import io.confluent.ksql.execution.expression.tree.Literal;
import io.confluent.ksql.execution.util.ExpressionTypeManager;
import io.confluent.ksql.function.AggregateFunctionInitArguments;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.function.KsqlAggregateFunction;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.schema.ksql.ColumnRef;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.util.KsqlException;
import java.util.List;
import java.util.OptionalInt;
import java.util.stream.Collectors;

public final class UdafUtil {

  private UdafUtil() {
  }

  public static KsqlAggregateFunction<?, ?, ?> resolveAggregateFunction(
      FunctionRegistry functionRegistry, FunctionCall functionCall, LogicalSchema schema
  ) {
    try {
      ExpressionTypeManager expressionTypeManager =
          new ExpressionTypeManager(schema, functionRegistry);

      SqlType argumentType =
          expressionTypeManager.getExpressionSqlType(functionCall.getArguments().get(0));

      // UDAFs only support one non-constant argument, and that argument must be a column reference
      Expression arg = functionCall.getArguments().get(0);
      OptionalInt udafIndex;
      if (arg instanceof ColumnReferenceExp) {
        udafIndex = schema.valueColumnIndex(((ColumnReferenceExp) arg).getReference());
      } else {
        // assume that it is a column reference with no alias
        udafIndex = schema.valueColumnIndex(ColumnRef.withoutSource(ColumnName.of(arg.toString())));
      }
      if (!udafIndex.isPresent()) {
        throw new KsqlException("Could not find column for expression: " + arg);
      }

      AggregateFunctionInitArguments aggregateFunctionInitArguments =
          createAggregateFunctionInitArgs(udafIndex.getAsInt(), functionCall);

      return functionRegistry.getAggregateFunction(
          functionCall.getName().name(),
          argumentType,
          aggregateFunctionInitArguments
      );
    } catch (Exception e) {
      throw new KsqlException("Failed to create aggregate function: " + functionCall, e);
    }
  }

  public static AggregateFunctionInitArguments createAggregateFunctionInitArgs(
      int udafIndex, FunctionCall functionCall
  ) {
    // args from index > 0 are all literals
    List<Object> args = functionCall.getArguments()
        .stream()
        .skip(1)
        .map(expr -> {
          if (expr instanceof Literal) {
            return (Literal) expr;
          } else {
            throw new KsqlException(
                "Aggregate function initialisation arguments must be literals"
            );
          }
        })
        .map(Literal::getValue)
        .collect(Collectors.toList());

    return new AggregateFunctionInitArguments(udafIndex, args);
  }


}
