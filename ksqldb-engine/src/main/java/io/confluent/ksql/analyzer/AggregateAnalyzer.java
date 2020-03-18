/*
 * Copyright 2020 Confluent Inc.
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

import static java.util.Objects.requireNonNull;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.collect.Sets.SetView;
import io.confluent.ksql.execution.expression.tree.ColumnReferenceExp;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.expression.tree.FunctionCall;
import io.confluent.ksql.execution.expression.tree.QualifiedColumnReferenceExp;
import io.confluent.ksql.execution.expression.tree.TraversalExpressionVisitor;
import io.confluent.ksql.execution.expression.tree.UnqualifiedColumnReferenceExp;
import io.confluent.ksql.execution.plan.SelectExpression;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.name.FunctionName;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.SchemaUtil;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

public class AggregateAnalyzer {

  private final FunctionRegistry functionRegistry;

  public AggregateAnalyzer(final FunctionRegistry functionRegistry) {
    this.functionRegistry = requireNonNull(functionRegistry, "functionRegistry");
  }

  public AggregateAnalysisResult analyze(
      final ImmutableAnalysis analysis,
      final List<SelectExpression> finalProjection
  ) {
    if (analysis.getGroupByExpressions().isEmpty()) {
      throw new IllegalArgumentException("Not an aggregate query");
    }

    final Context context = new Context(analysis);

    finalProjection.stream()
        .map(SelectExpression::getExpression)
        .forEach(exp -> processSelect(exp, context));

    analysis.getWhereExpression()
        .ifPresent(exp -> processWhere(exp, context));

    analysis.getGroupByExpressions()
        .forEach(exp -> processGroupBy(exp, context));

    analysis.getHavingExpression()
        .ifPresent(exp -> processHaving(exp, context));

    enforceAggregateRules(context);

    return context.aggregateAnalysis;
  }

  private void processSelect(final Expression expression, final Context context) {
    final Set<ColumnReferenceExp> nonAggParams = new HashSet<>();
    final AggregateVisitor visitor = new AggregateVisitor(context, (aggFuncName, node) -> {
      if (aggFuncName.isPresent()) {
        throwOnWindowBoundColumnIfWindowedAggregate(node, context);
      } else {
        nonAggParams.add(node);
      }
    });

    visitor.process(expression, null);

    if (visitor.visitedAggFunction) {
      context.aggregateAnalysis.addAggregateSelectField(nonAggParams);
    } else {
      context.aggregateAnalysis.addNonAggregateSelectExpression(expression, nonAggParams);
    }

    context.aggregateAnalysis.addFinalSelectExpression(expression);
  }

  private void processGroupBy(final Expression expression, final Context context) {
    final AggregateVisitor visitor = new AggregateVisitor(context, (aggFuncName, node) -> {
      if (aggFuncName.isPresent()) {
        throw new KsqlException("GROUP BY does not support aggregate functions: "
            + aggFuncName.get().text() + " is an aggregate function.");
      }
      throwOnWindowBoundColumnIfWindowedAggregate(node, context);
    });

    visitor.process(expression, null);
  }

  private void processWhere(final Expression expression, final Context context) {
    final AggregateVisitor visitor = new AggregateVisitor(context, (aggFuncName, node) ->
        throwOnWindowBoundColumnIfWindowedAggregate(node, context));

    visitor.process(expression, null);
  }

  private void processHaving(final Expression expression, final Context context) {
    final AggregateVisitor visitor = new AggregateVisitor(context, (aggFuncName, node) -> {
      throwOnWindowBoundColumnIfWindowedAggregate(node, context);
      if (!aggFuncName.isPresent()) {
        context.aggregateAnalysis.addNonAggregateHavingField(node);
      }
    });
    visitor.process(expression, null);

    context.aggregateAnalysis.setHavingExpression(expression);
  }

  private static void throwOnWindowBoundColumnIfWindowedAggregate(
      final ColumnReferenceExp node,
      final Context context
  ) {
    // Window bounds are supported for operations on windowed sources
    if (!context.analysis.getWindowExpression().isPresent()) {
      return;
    }

    // For non-windowed sources, with a windowed GROUP BY, they are only supported in selects:
    if (SchemaUtil.isWindowBound(node.getColumnName())) {
      throw new KsqlException(
          "Window bounds column " + node + " can only be used in the SELECT clause of "
              + "windowed aggregations and can not be passed to aggregate functions."
              + System.lineSeparator()
              + "See https://github.com/confluentinc/ksql/issues/4397"
      );
    }
  }

  private static void enforceAggregateRules(
      final Context context
  ) {
    if (context.aggregateAnalysis.getAggregateFunctions().isEmpty()) {
      throw new KsqlException(
          "GROUP BY requires columns using aggregate functions in SELECT clause.");
    }

    final Set<Expression> groupByExprs = getGroupByExpressions(context.analysis);

    final List<String> unmatchedSelects = context.aggregateAnalysis
        .getNonAggregateSelectExpressions()
        .entrySet()
        .stream()
        // Remove any that exactly match a group by expression:
        .filter(e -> !groupByExprs.contains(e.getKey()))
        // Remove any that are constants,
        // or expressions where all params exactly match a group by expression:
        .filter(e -> !Sets.difference(e.getValue(), groupByExprs).isEmpty())
        .map(Map.Entry::getKey)
        .map(Expression::toString)
        .sorted()
        .collect(Collectors.toList());

    if (!unmatchedSelects.isEmpty()) {
      throw new KsqlException(
          "Non-aggregate SELECT expression(s) not part of GROUP BY: " + unmatchedSelects);
    }

    final SetView<ColumnReferenceExp> unmatchedSelectsAgg = Sets
        .difference(context.aggregateAnalysis.getAggregateSelectFields(), groupByExprs);
    if (!unmatchedSelectsAgg.isEmpty()) {
      throw new KsqlException(
          "Column used in aggregate SELECT expression(s) "
              + "outside of aggregate functions not part of GROUP BY: " + unmatchedSelectsAgg);
    }

    final Set<ColumnReferenceExp> havingColumns = context.aggregateAnalysis
        .getNonAggregateHavingFields().stream()
        .map(ref -> new UnqualifiedColumnReferenceExp(ref.getColumnName()))
        .collect(Collectors.toSet());

    final Set<ColumnReferenceExp> havingOnly = Sets.difference(havingColumns, groupByExprs);
    if (!havingOnly.isEmpty()) {
      throw new KsqlException(
          "Non-aggregate HAVING expression not part of GROUP BY: " + havingOnly);
    }
  }

  private static Set<Expression> getGroupByExpressions(
      final ImmutableAnalysis analysis
  ) {
    if (!analysis.getWindowExpression().isPresent()) {
      return ImmutableSet.copyOf(analysis.getGroupByExpressions());
    }

    // Add in window bounds columns as implicit group by columns:
    final Set<UnqualifiedColumnReferenceExp> windowBoundColumnRefs =
        SchemaUtil.windowBoundsColumnNames().stream()
            .map(UnqualifiedColumnReferenceExp::new)
            .collect(Collectors.toSet());

    return ImmutableSet.<Expression>builder()
        .addAll(analysis.getGroupByExpressions())
        .addAll(windowBoundColumnRefs)
        .build();
  }

  private final class AggregateVisitor extends TraversalExpressionVisitor<Void> {

    private final BiConsumer<Optional<FunctionName>, ColumnReferenceExp>
        dereferenceCollector;
    private final ColumnReferenceExp defaultArgument;
    private final MutableAggregateAnalysis aggregateAnalysis;

    private Optional<FunctionName> aggFunctionName = Optional.empty();
    private boolean visitedAggFunction = false;

    private AggregateVisitor(
        final Context context,
        final BiConsumer<Optional<FunctionName>, ColumnReferenceExp> dereferenceCollector
    ) {
      this.defaultArgument = context.analysis.getDefaultArgument();
      this.aggregateAnalysis = context.aggregateAnalysis;
      this.dereferenceCollector = requireNonNull(dereferenceCollector, "dereferenceCollector");
    }

    @Override
    public Void visitFunctionCall(final FunctionCall node, final Void context) {
      final FunctionName functionName = node.getName();
      final boolean aggregateFunc = functionRegistry.isAggregate(functionName);

      final FunctionCall functionCall = aggregateFunc && node.getArguments().isEmpty()
          ? new FunctionCall(node.getLocation(), node.getName(), ImmutableList.of(defaultArgument))
          : node;

      if (aggregateFunc) {
        if (aggFunctionName.isPresent()) {
          throw new KsqlException("Aggregate functions can not be nested: "
              + aggFunctionName.get().text() + "(" + functionName.text() + "())");
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
    public Void visitColumnReference(
        final UnqualifiedColumnReferenceExp node,
        final Void context
    ) {
      dereferenceCollector.accept(aggFunctionName, node);

      if (!SchemaUtil.isWindowBound(node.getColumnName())) {
        aggregateAnalysis.addRequiredColumn(node);
      }
      return null;
    }

    @Override
    public Void visitQualifiedColumnReference(
        final QualifiedColumnReferenceExp node,
        final Void context
    ) {
      throw new UnsupportedOperationException("Should of been converted to unqualified");
    }
  }

  private static final class Context {

    final ImmutableAnalysis analysis;
    final MutableAggregateAnalysis aggregateAnalysis = new MutableAggregateAnalysis();

    Context(final ImmutableAnalysis analysis) {
      this.analysis = requireNonNull(analysis, "analysis");
    }
  }
}
