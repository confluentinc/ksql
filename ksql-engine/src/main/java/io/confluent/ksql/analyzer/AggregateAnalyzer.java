/*
 * Copyright 2017 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.confluent.ksql.analyzer;

import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.parser.DefaultTraversalVisitor;
import io.confluent.ksql.parser.tree.DereferenceExpression;
import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.parser.tree.FunctionCall;
import io.confluent.ksql.parser.tree.Node;
import java.util.Objects;

public class AggregateAnalyzer {

  private final AggregateAnalysis aggregateAnalysis;
  private final DereferenceExpression defaultArgument;
  private final FunctionRegistry functionRegistry;

  public AggregateAnalyzer(
      final AggregateAnalysis aggregateAnalysis,
      final DereferenceExpression defaultArgument,
      final FunctionRegistry functionRegistry
  ) {
    this.aggregateAnalysis = Objects.requireNonNull(aggregateAnalysis, "aggregateAnalysis");
    this.defaultArgument = Objects.requireNonNull(defaultArgument, "defaultArgument");
    this.functionRegistry = Objects.requireNonNull(functionRegistry, "functionRegistry");
  }

  public void process(final Expression expression, final boolean isGroupByExpression) {
    final AggregateVisitor visitor = new AggregateVisitor(isGroupByExpression);

    visitor.process(expression, new AnalysisContext());
  }

  private final class AggregateVisitor extends DefaultTraversalVisitor<Node, AnalysisContext> {

    private final boolean isGroupByExpression;
    private boolean hasAggregateFunction = false;

    private AggregateVisitor(final boolean isGroupByExpression) {
      this.isGroupByExpression = isGroupByExpression;
    }

    @Override
    protected Node visitFunctionCall(final FunctionCall node, final AnalysisContext context) {
      final String functionName = node.getName().getSuffix();
      if (functionRegistry.isAggregate(functionName)) {
        if (node.getArguments().isEmpty()) {
          node.getArguments().add(defaultArgument);
        }

        node.getArguments().forEach(aggregateAnalysis::addAggregateFunctionArgument);
        aggregateAnalysis.addFunction(node);
        hasAggregateFunction = true;
      }

      return super.visitFunctionCall(node, context);
    }

    @Override
    protected Node visitDereferenceExpression(
        final DereferenceExpression node,
        final AnalysisContext context
    ) {
      final String name = node.toString();
      if (isGroupByExpression) {
        aggregateAnalysis.addGroupByColumn(name, node);
      } else if (!hasAggregateFunction) {
        aggregateAnalysis.addNonAggregateSelectColumn(name, node);
      }

      aggregateAnalysis.addRequiredColumn(name, node);
      return null;
    }
  }
}
