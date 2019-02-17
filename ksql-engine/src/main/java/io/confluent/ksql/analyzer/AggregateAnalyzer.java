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
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

class AggregateAnalyzer {

  private final AggregateAnalysis aggregateAnalysis;
  private final DereferenceExpression defaultArgument;
  private final FunctionRegistry functionRegistry;

  AggregateAnalyzer(
      final AggregateAnalysis aggregateAnalysis,
      final DereferenceExpression defaultArgument,
      final FunctionRegistry functionRegistry
  ) {
    this.aggregateAnalysis = Objects.requireNonNull(aggregateAnalysis, "aggregateAnalysis");
    this.defaultArgument = Objects.requireNonNull(defaultArgument, "defaultArgument");
    this.functionRegistry = Objects.requireNonNull(functionRegistry, "functionRegistry");
  }

  void processSelect(final Expression expression) {
    final AggregateVisitor visitor = new AggregateVisitor((inAggregateFunction, node) -> {
      if (!inAggregateFunction) {
        aggregateAnalysis.addNonAggregateSelectColumn(node);
      }
    });

    visitor.process(expression, new AnalysisContext());
  }

  void processGroupBy(final Expression expression) {
    final AggregateVisitor visitor = new AggregateVisitor((ignored, node) ->
        aggregateAnalysis.addGroupByColumn(node));

    visitor.process(expression, new AnalysisContext());
  }

  void processHaving(final Expression expression) {
    final AggregateVisitor visitor = new AggregateVisitor((inAggregateFunction, node) -> {
      if (!inAggregateFunction) {
        aggregateAnalysis.addNonAggregateHavingColumn(node);
      }
    });
    visitor.process(expression, new AnalysisContext());
  }

  private final class AggregateVisitor extends DefaultTraversalVisitor<Node, AnalysisContext> {

    private final BiConsumer<Boolean, DereferenceExpression> dereferenceCollector;
    private final AtomicInteger aggregateFunctionDepth = new AtomicInteger(0);

    private AggregateVisitor(
        final BiConsumer<Boolean, DereferenceExpression> dereferenceCollector
    ) {
      this.dereferenceCollector =
          Objects.requireNonNull(dereferenceCollector, "dereferenceCollector");
    }

    @Override
    protected Node visitFunctionCall(final FunctionCall node, final AnalysisContext context) {
      final String functionName = node.getName().getSuffix();
      final boolean aggregateFunc = functionRegistry.isAggregate(functionName);
      if (aggregateFunc) {
        if (node.getArguments().isEmpty()) {
          node.getArguments().add(defaultArgument);
        }

        node.getArguments().forEach(aggregateAnalysis::addAggregateFunctionArgument);
        aggregateAnalysis.addFunction(node);
        aggregateFunctionDepth.incrementAndGet();
      }

      final Node result = super.visitFunctionCall(node, context);

      if (aggregateFunc) {
        aggregateFunctionDepth.decrementAndGet();
      }

      return result;
    }

    @Override
    protected Node visitDereferenceExpression(
        final DereferenceExpression node,
        final AnalysisContext context
    ) {
      dereferenceCollector.accept(aggregateFunctionDepth.get() != 0, node);

      aggregateAnalysis.addRequiredColumn(node);
      return null;
    }
  }
}
