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

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.execution.expression.tree.ColumnReferenceExp;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.expression.tree.FunctionCall;
import io.confluent.ksql.execution.expression.tree.TraversalExpressionVisitor;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.util.KsqlException;
import java.util.Objects;
import java.util.Optional;

class TableFunctionAnalyzer {

  private final TableFunctionAnalysis tableFunctionAnalysis;
  private final ColumnReferenceExp defaultArgument;
  private final FunctionRegistry functionRegistry;

  TableFunctionAnalyzer(
      final TableFunctionAnalysis tableFunctionAnalysis,
      final ColumnReferenceExp defaultArgument,
      final FunctionRegistry functionRegistry
  ) {
    this.tableFunctionAnalysis = Objects.requireNonNull(tableFunctionAnalysis, "aggregateAnalysis");
    this.defaultArgument = Objects.requireNonNull(defaultArgument, "defaultArgument");
    this.functionRegistry = Objects.requireNonNull(functionRegistry, "functionRegistry");
  }

  void processSelect(final Expression expression) {
    final AggregateVisitor visitor = new AggregateVisitor();
    visitor.process(expression, null);
  }

  private final class AggregateVisitor extends TraversalExpressionVisitor<Void> {

    private Optional<String> tableFunctionName = Optional.empty();

    @Override
    public Void visitFunctionCall(final FunctionCall node, final Void context) {
      final String functionName = node.getName().name();
      final boolean isTableFunction = functionRegistry.isTableFunction(functionName);

      final FunctionCall functionCall = isTableFunction && node.getArguments().isEmpty()
          ? new FunctionCall(node.getLocation(), node.getName(), ImmutableList.of(defaultArgument))
          : node;

      if (isTableFunction) {
        if (tableFunctionName.isPresent()) {
          throw new KsqlException("Aggregate functions can not be nested: "
              + tableFunctionName.get() + "(" + functionName + "())");
        }

        tableFunctionName = Optional.of(functionName);

        //functionCall.getArguments().forEach(tableFunctionAnalysis::addAggregateFunctionArgument);
        tableFunctionAnalysis.addTableFunction(functionCall);
      }

      super.visitFunctionCall(functionCall, context);

      if (isTableFunction) {
        tableFunctionName = Optional.empty();
      }

      return null;
    }

    @Override
    public Void visitColumnReference(
        final ColumnReferenceExp node,
        final Void context
    ) {
      tableFunctionAnalysis.addColumn(node);
      return null;
    }
  }
}
