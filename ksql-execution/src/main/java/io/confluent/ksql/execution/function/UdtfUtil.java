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
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.function.KsqlTableFunction;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.util.KsqlException;
import java.util.List;
import org.apache.kafka.connect.data.Schema;

public final class UdtfUtil {

  private UdtfUtil() {
  }

  @SuppressWarnings("deprecation") // Need to migrate away from Connect Schema use.
  public static KsqlTableFunction<?, ?> resolveTableFunction(
      final FunctionRegistry functionRegistry,
      final FunctionCall functionCall,
      final LogicalSchema schema
  ) {
    try {
      final ExpressionTypeManager expressionTypeManager =
          new ExpressionTypeManager(schema, functionRegistry);
      final List<Expression> functionArgs = functionCall.getArguments();
      final Schema expressionType = expressionTypeManager.getExpressionSchema(functionArgs.get(0));
      return functionRegistry.getTableFunction(
          functionCall.getName().name(),
          expressionType
      );
    } catch (final Exception e) {
      throw new KsqlException("Failed to create table function: " + functionCall, e);
    }
  }
}
