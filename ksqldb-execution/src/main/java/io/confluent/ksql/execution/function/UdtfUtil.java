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

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.expression.tree.FunctionCall;
import io.confluent.ksql.execution.util.ExpressionTypeManager;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.function.KsqlTableFunction;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.SqlArgument;
import java.util.List;
import java.util.stream.Collectors;

public final class UdtfUtil {

  private UdtfUtil() {
  }

  public static KsqlTableFunction resolveTableFunction(
      final FunctionRegistry functionRegistry,
      final FunctionCall functionCall,
      final LogicalSchema schema
  ) {
    final ExpressionTypeManager expressionTypeManager =
        new ExpressionTypeManager(schema, functionRegistry);

    final List<Expression> functionArgs = functionCall.getArguments();

    final List<SqlArgument> argTypes = functionArgs.isEmpty()
        ? ImmutableList.of(
            SqlArgument.of(FunctionRegistry.DEFAULT_FUNCTION_ARG_SCHEMA))
        : functionArgs.stream()
            .map(expressionTypeManager::getExpressionSqlType)
            .map(SqlArgument::of)
            .collect(Collectors.toList());

    return functionRegistry.getTableFunction(
        functionCall.getName(),
        argTypes
    );
  }
}
