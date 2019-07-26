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
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.parser.tree.DereferenceExpression;
import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.parser.tree.FunctionCall;
import io.confluent.ksql.parser.tree.TraversalExpressionVisitor;
import io.confluent.ksql.util.KsqlException;
import java.util.HashSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
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
    final Set<DereferenceExpression> nonAggParams = new HashSet<>();
    final AggregateVisitor visitor = new AggregateVisitor((aggFuncName, node) -> {
      if (!aggFuncName.isPresent()) {
        nonAggParams.add(node);
      }
    });

    visitor.process(expression, null);

    if (visitor.visitedAggFunction) {
      aggregateAnalysis.addAggregateSelectField(nonAggParams);
    } else {
      aggregateAnalysis.addNonAggregateSelectExpression(expression, nonAggParams);
    }
  }

  void processGroupBy(final Expression expression) {
    final AggregateVisitor visitor = new AggregateVisitor((aggFuncName, node) -> {
      if (aggFuncName.isPresent()) {
        throw new KsqlException("GROUP BY does not support aggregate functions: "
            + aggFuncName.get() + " is an aggregate function.");
      }
    });

    visitor.process(expression, null);
  }

  void processHaving(final Expression expression) {
    final AggregateVisitor visitor = new AggregateVisitor((aggFuncName, node) -> {
      if (!aggFuncName.isPresent()) {
        aggregateAnalysis.addNonAggregateHavingField(node);
      }
    });
    visitor.process(expression, null);
  }

  private final class AggregateVisitor extends TraversalExpressionVisitor<Void> {

    private final BiConsumer<Optional<String>, DereferenceExpression> dereferenceCollector;
    private Optional<String> aggFunctionName = Optional.empty();
    private boolean visitedAggFunction = false;

    private AggregateVisitor(
        final BiConsumer<Optional<String>, DereferenceExpression> dereferenceCollector
    ) {
      this.dereferenceCollector =
          Objects.requireNonNull(dereferenceCollector, "dereferenceCollector");
    }

    @Override
    public Void visitFunctionCall(final FunctionCall node, final Void context) {
      final String functionName = node.getName().getSuffix();
      final boolean aggregateFunc = functionRegistry.isAggregate(functionName);

      final FunctionCall functionCall = aggregateFunc && node.getArguments().isEmpty()
          ? new FunctionCall(node.getLocation(), node.getName(), ImmutableList.of(defaultArgument))
          : node;

      if (aggregateFunc) {
        if (aggFunctionName.isPresent()) {
          throw new KsqlException("Aggregate functions can not be nested: "
              + aggFunctionName.get() + "(" + functionName + "())");
        }

        visitedAggFunction = true;
        aggFunctionName = Optional.of(functionName);

        functionCall.getArguments().forEach(aggregateAnalysis::addAggregateFunctionArgument);
        aggregateAnalysis.addAggFunction(functionCall);
      }

      super.visitFunctionCall(functionCall, context);

      if (aggregateFunc) {
        aggFunctionName = Optional.empty();
      }

      return null;
    }

    @Override
    public Void visitDereferenceExpression(
        final DereferenceExpression node,
        final Void context
    ) {
      dereferenceCollector.accept(aggFunctionName, node);
      aggregateAnalysis.addRequiredColumn(node);
      return null;
    }


  }
}
