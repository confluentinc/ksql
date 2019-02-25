/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.analyzer;

import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.parser.DefaultTraversalVisitor;
import io.confluent.ksql.parser.tree.DereferenceExpression;
import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.parser.tree.FunctionCall;
import io.confluent.ksql.parser.tree.Node;
import io.confluent.ksql.util.KsqlException;
import java.util.HashSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.Stack;
import java.util.function.BiConsumer;

class AggregateAnalyzer {

  private final MutableAggregateAnalysis aggregateAnalysis;
  private final DereferenceExpression defaultArgument;
  private final FunctionRegistry functionRegistry;

  AggregateAnalyzer(
      final MutableAggregateAnalysis aggregateAnalysis,
      final DereferenceExpression defaultArgument,
      final FunctionRegistry functionRegistry
  ) {
    this.aggregateAnalysis = Objects.requireNonNull(aggregateAnalysis, "aggregateAnalysis");
    this.defaultArgument = Objects.requireNonNull(defaultArgument, "defaultArgument");
    this.functionRegistry = Objects.requireNonNull(functionRegistry, "functionRegistry");
  }

  void processSelect(final Expression expression) {
    final Set<DereferenceExpression> nonAggFields = new HashSet<>();
    final AggregateVisitor visitor = new AggregateVisitor((aggFuncName, node) -> {
      if (!aggFuncName.isPresent()) {
        nonAggFields.add(node);
      }
    });

    visitor.process(expression, new AnalysisContext());

    if (!visitor.visitedAggFunction) {
      aggregateAnalysis.addNonAggregateSelectExpression(expression, nonAggFields);
    }
  }

  void processGroupBy(final Expression expression) {
    final AggregateVisitor visitor = new AggregateVisitor((aggFuncName, node) -> {
      if (aggFuncName.isPresent()) {
        throw new KsqlException("GROUP BY does not support aggregate functions: "
            + aggFuncName.get() + " is an aggregate function.");
      }
    });

    visitor.process(expression, new AnalysisContext());
  }

  void processHaving(final Expression expression) {
    final AggregateVisitor visitor = new AggregateVisitor((aggFuncName, node) -> {
      if (!aggFuncName.isPresent()) {
        aggregateAnalysis.addNonAggregateHavingField(node);
      }
    });
    visitor.process(expression, new AnalysisContext());
  }

  private final class AggregateVisitor extends DefaultTraversalVisitor<Node, AnalysisContext> {

    private final BiConsumer<Optional<String>, DereferenceExpression> dereferenceCollector;
    private final Stack<String> aggregateFunctionStack = new Stack<>();
    private boolean visitedAggFunction = false;

    private AggregateVisitor(
        final BiConsumer<Optional<String>, DereferenceExpression> dereferenceCollector
    ) {
      this.dereferenceCollector =
          Objects.requireNonNull(dereferenceCollector, "dereferenceCollector");
    }

    @Override
    protected Node visitFunctionCall(final FunctionCall node, final AnalysisContext context) {
      final String functionName = node.getName().getSuffix();
      final boolean aggregateFunc = functionRegistry.isAggregate(functionName);
      if (aggregateFunc) {
        visitedAggFunction = true;
        if (node.getArguments().isEmpty()) {
          node.getArguments().add(defaultArgument);
        }

        node.getArguments().forEach(aggregateAnalysis::addAggregateFunctionArgument);
        aggregateAnalysis.addAggFunction(node);
        aggregateFunctionStack.push(functionName);
      }

      final Node result = super.visitFunctionCall(node, context);

      if (aggregateFunc) {
        aggregateFunctionStack.pop();
      }

      return result;
    }

    @Override
    protected Node visitDereferenceExpression(
        final DereferenceExpression node,
        final AnalysisContext context
    ) {
      final Optional<String> aggFunctionName = aggregateFunctionStack.isEmpty()
          ? Optional.empty()
          : Optional.of(aggregateFunctionStack.peek());

      dereferenceCollector.accept(aggFunctionName, node);

      aggregateAnalysis.addRequiredColumn(node);
      return null;
    }
  }
}
