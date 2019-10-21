/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.ksql.analyzer;

import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.expression.tree.FunctionCall;
import io.confluent.ksql.execution.expression.tree.TraversalExpressionVisitor;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.util.KsqlException;
import java.util.Objects;
import java.util.Optional;

/**
 * Analyses a query and extracts data related to table functions - the data is returned in an
 * instance of {@code TableFunctionAnalysis}
 */
class TableFunctionAnalyzer {

  private final TableFunctionAnalysis tableFunctionAnalysis;
  private final FunctionRegistry functionRegistry;

  TableFunctionAnalyzer(
      final TableFunctionAnalysis tableFunctionAnalysis,
      final FunctionRegistry functionRegistry
  ) {
    this.tableFunctionAnalysis = Objects.requireNonNull(tableFunctionAnalysis, "aggregateAnalysis");
    this.functionRegistry = Objects.requireNonNull(functionRegistry, "functionRegistry");
  }

  void processSelect(final Expression expression) {
    final TableFunctionVisitor visitor = new TableFunctionVisitor();
    visitor.process(expression, null);
  }

  private final class TableFunctionVisitor extends TraversalExpressionVisitor<Void> {

    private Optional<String> tableFunctionName = Optional.empty();

    @Override
    public Void visitFunctionCall(final FunctionCall functionCall, final Void context) {
      final String functionName = functionCall.getName().name();
      final boolean isTableFunction = functionRegistry.isTableFunction(functionName);

      if (isTableFunction) {
        if (tableFunctionName.isPresent()) {
          throw new KsqlException("Table functions cannot be nested: "
              + tableFunctionName.get() + "(" + functionName + "())");
        }

        tableFunctionName = Optional.of(functionName);

        tableFunctionAnalysis.addTableFunction(functionCall);
      }

      super.visitFunctionCall(functionCall, context);

      if (isTableFunction) {
        tableFunctionName = Optional.empty();
      }

      return null;
    }
  }
}
