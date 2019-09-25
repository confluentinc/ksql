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
import io.confluent.ksql.execution.util.ExpressionTypeManager;
import io.confluent.ksql.function.AggregateFunctionArguments;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.function.KsqlAggregateFunction;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.util.KsqlException;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.kafka.connect.data.Schema;

public final class UdafUtil {
  private UdafUtil() {
  }

  @SuppressWarnings("deprecation") // Need to migrate away from Connect Schema use.
  public static KsqlAggregateFunction resolveAggregateFunction(
      final FunctionRegistry functionRegistry,
      final FunctionCall functionCall,
      final LogicalSchema schema
  ) {
    try {
      final ExpressionTypeManager expressionTypeManager =
          new ExpressionTypeManager(schema, functionRegistry);
      final List<Expression> functionArgs = functionCall.getArguments();
      final Schema expressionType = expressionTypeManager.getExpressionSchema(functionArgs.get(0));
      final KsqlAggregateFunction aggregateFunctionInfo = functionRegistry.getAggregate(
          functionCall.getName().name(),
          expressionType
      );

      final List<String> args = functionArgs.stream()
          .map(Expression::toString)
          .collect(Collectors.toList());

      final int udafIndex = schema.valueColumnIndex(args.get(0)).getAsInt();

      return aggregateFunctionInfo.getInstance(new AggregateFunctionArguments(udafIndex, args));
    } catch (final Exception e) {
      throw new KsqlException("Failed to create aggregate function: " + functionCall, e);
    }
  }
}
