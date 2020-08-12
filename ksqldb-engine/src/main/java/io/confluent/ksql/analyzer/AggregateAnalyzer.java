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
import io.confluent.ksql.execution.expression.tree.ColumnReferenceExp;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.expression.tree.FunctionCall;
import io.confluent.ksql.execution.expression.tree.QualifiedColumnReferenceExp;
import io.confluent.ksql.execution.expression.tree.SubscriptExpression;
import io.confluent.ksql.execution.expression.tree.TraversalExpressionVisitor;
import io.confluent.ksql.execution.expression.tree.UnqualifiedColumnReferenceExp;
import io.confluent.ksql.execution.plan.SelectExpression;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.name.FunctionName;
import io.confluent.ksql.parser.tree.GroupBy;
import io.confluent.ksql.schema.ksql.SystemColumns;
import io.confluent.ksql.util.KsqlException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
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
    if (!analysis.getGroupBy().isPresent()) {
      throw new IllegalArgumentException("Not an aggregate query");
    }

    final AggAnalyzer aggAnalyzer = new AggAnalyzer(analysis, functionRegistry);
    aggAnalyzer.process(finalProjection);
    return aggAnalyzer.result();
  }

  private static final class AggAnalyzer {

    private final ImmutableAnalysis analysis;
    private final MutableAggregateAnalysis aggregateAnalysis = new MutableAggregateAnalysis();
    private final FunctionRegistry functionRegistry;
    private final Set<Expression> groupBy;
    private boolean foundExpressionInGroupBy = false;


    // The list of columns from the source schema that are used in aggregate columns, but not as
    // parameters to the aggregate functions and which are not part of the GROUP BY clause:
    private final List<Expression> aggSelectsNotPartOfGroupBy = new ArrayList<>();

    // The list of non-aggregate select expression which are not part of the GROUP BY clause:
    private final List<Expression> nonAggSelectsNotPartOfGroupBy = new ArrayList<>();

    // The list of columns from the source schema that are used in the HAVING clause outside
    // of aggregate functions which are not part of the GROUP BY clause:
    private final List<Expression> nonAggHavingNotPartOfGroupBy = new ArrayList<>();

    AggAnalyzer(
        final ImmutableAnalysis analysis,
        final FunctionRegistry functionRegistry
    ) {
      this.analysis = requireNonNull(analysis, "analysis");
      this.functionRegistry = requireNonNull(functionRegistry, "functionRegistry");
      this.groupBy = getGroupByExpressions(analysis);
    }

    public void process(final List<SelectExpression> finalProjection) {
      finalProjection.stream()
          .map(SelectExpression::getExpression)
          .forEach(this::processSelect);

      analysis.getWhereExpression()
          .ifPresent(this::processWhere);

      analysis.getGroupBy()
          .map(GroupBy::getGroupingExpressions)
          .orElseGet(ImmutableList::of)
          .forEach(this::processGroupBy);

      analysis.getHavingExpression()
          .ifPresent(this::processHaving);
    }

    private void processSelect(final Expression expression) {
      final Set<Expression> nonAggParams = new HashSet<>();
      final AggregateVisitor visitor = new AggregateVisitor(
          this,
          groupBy,
          foundExpressionInGroupBy,
          (aggFuncName, node) -> {
            if (aggFuncName.isPresent()) {
              throwOnWindowBoundColumnIfWindowedAggregate(node);
            } else {
              if (!groupBy.contains(node)) {
                nonAggParams.add(node);
              }
            }
          });

      visitor.process(expression, null);

      if (visitor.visitedAggFunction) {
        captureAggregateSelectNotPartOfGroupBy(nonAggParams);
      } else {
        captureNonAggregateSelectNotPartOfGroupBy(expression, nonAggParams);
      }

      aggregateAnalysis.addFinalSelectExpression(expression);
    }

    private void processGroupBy(final Expression expression) {
      final AggregateVisitor visitor = new AggregateVisitor(
          this,
          groupBy,
          foundExpressionInGroupBy,
          (aggFuncName, node) -> {
            if (aggFuncName.isPresent()) {
              throw new KsqlException("GROUP BY does not support aggregate functions: "
                  + aggFuncName.get().text() + " is an aggregate function.");
            }
            throwOnWindowBoundColumnIfWindowedAggregate(node);
          });

      visitor.process(expression, null);
    }

    private void processWhere(final Expression expression) {
      final AggregateVisitor visitor = new AggregateVisitor(
          this,
          groupBy,
          foundExpressionInGroupBy,
          (aggFuncName, node) ->
            throwOnWindowBoundColumnIfWindowedAggregate(node));

      visitor.process(expression, null);
    }

    private void processHaving(final Expression expression) {
      final AggregateVisitor visitor = new AggregateVisitor(
          this,
          groupBy,
          foundExpressionInGroupBy,
          (aggFuncName, node) -> {
            throwOnWindowBoundColumnIfWindowedAggregate(node);

            if (!aggFuncName.isPresent()) {
              captureNoneAggregateHavingNotPartOfGroupBy(node);
            }
          });

      visitor.process(expression, null);

      aggregateAnalysis.setHavingExpression(expression);
    }

    private void throwOnWindowBoundColumnIfWindowedAggregate(final Expression node) {
      // Window bounds are supported for operations on windowed sources
      if (!analysis.getWindowExpression().isPresent()) {
        return;
      }

      // For non-windowed sources, with a windowed GROUP BY, they are only supported in selects:
      if (!(node instanceof ColumnReferenceExp)) {
        return;
      }
      if (SystemColumns.isWindowBound(((ColumnReferenceExp)node).getColumnName())) {
        throw new KsqlException(
            "Window bounds column " + node + " can only be used in the SELECT clause of "
                + "windowed aggregations and can not be passed to aggregate functions."
                + System.lineSeparator()
                + "See https://github.com/confluentinc/ksql/issues/4397"
        );
      }
    }

    private static Set<Expression> getGroupByExpressions(
        final ImmutableAnalysis analysis
    ) {
      final List<Expression> groupByExpressions = analysis.getGroupBy()
          .map(GroupBy::getGroupingExpressions)
          .orElseGet(ImmutableList::of);

      if (!analysis.getWindowExpression().isPresent()) {
        return ImmutableSet.copyOf(groupByExpressions);
      }

      // Add in window bounds columns as implicit group by columns:
      final Set<UnqualifiedColumnReferenceExp> windowBoundColumnRefs =
          SystemColumns.windowBoundsColumnNames().stream()
              .map(UnqualifiedColumnReferenceExp::new)
              .collect(Collectors.toSet());

      return ImmutableSet.<Expression>builder()
          .addAll(groupByExpressions)
          .addAll(windowBoundColumnRefs)
          .build();
    }

    private void captureNonAggregateSelectNotPartOfGroupBy(
        final Expression expression,
        final Set<Expression> nonAggParams
    ) {

      boolean matchesGroupBy = false;
      // If the non-Agg expression is a function, then all its arguments must be part of the
      // grouping columns. Note, they may be nested inside other functions.
      /*if (expression instanceof FunctionCall) {
        matchesGroupBy = functionContainsOnlyGroupingColumns(expression, true);
      } else {
        matchesGroupBy = groupBy.contains(expression);
      }*/
      matchesGroupBy = groupBy.contains(expression);
      if (matchesGroupBy) {
        return;
      }

      final boolean onlyReferencesColumnsInGroupBy = Sets
          .difference(nonAggParams, groupBy).isEmpty();
      if (onlyReferencesColumnsInGroupBy) {
        return;
      }

      nonAggSelectsNotPartOfGroupBy.add(expression);
    }

    private void captureAggregateSelectNotPartOfGroupBy(
        final Set<Expression> nonAggParams
    ) {
      nonAggParams.stream()
          .filter(param -> !groupBy.contains(param))
          .forEach(aggSelectsNotPartOfGroupBy::add);
    }

    private void captureNoneAggregateHavingNotPartOfGroupBy(final Expression nonAggColumn) {
      if (groupBy.contains(nonAggColumn)) {
        return;
      }

      nonAggHavingNotPartOfGroupBy.add(nonAggColumn);
    }

    public AggregateAnalysisResult result() {
      enforceAggregateRules();
      return aggregateAnalysis;
    }

    private void enforceAggregateRules() {
      if (aggregateAnalysis.getAggregateFunctions().isEmpty()) {
        throw new KsqlException(
            "GROUP BY requires aggregate functions in either the SELECT or HAVING clause.");
      }

      final String unmatchedSelects = nonAggSelectsNotPartOfGroupBy.stream()
          .map(Objects::toString)
          .collect(Collectors.joining(", "));

      if (!unmatchedSelects.isEmpty()) {
        throw new KsqlException(
            "Non-aggregate SELECT expression(s) not part of GROUP BY: "
                + unmatchedSelects
                + System.lineSeparator()
                + "Either add the column to the GROUP BY or remove it from the SELECT."
        );
      }

      final String unmatchedSelectsAgg = aggSelectsNotPartOfGroupBy.stream()
          .map(Objects::toString)
          .collect(Collectors.joining(", "));

      if (!unmatchedSelectsAgg.isEmpty()) {
        throw new KsqlException(
            "Column used in aggregate SELECT expression(s) outside of aggregate functions "
                + "not part of GROUP BY: "
                + unmatchedSelectsAgg
                + System.lineSeparator()
                + "Either add the column to the GROUP BY or remove it from the SELECT."
        );
      }

      final String havingOnly = nonAggHavingNotPartOfGroupBy.stream()
          .map(Objects::toString)
          .collect(Collectors.joining(", "));

      if (!havingOnly.isEmpty()) {
        throw new KsqlException(
            "Non-aggregate HAVING expression not part of GROUP BY: " + havingOnly
                + System.lineSeparator()
                + "Consider switching the HAVING clause to a WHERE clause before the GROUP BY."
        );
      }
    }
  }

  private static final class AggregateVisitor extends TraversalExpressionVisitor<Void> {

    private final BiConsumer<Optional<FunctionName>, Expression> dereferenceCollector;
    private final ColumnReferenceExp defaultArgument;
    private final MutableAggregateAnalysis aggregateAnalysis;
    private final FunctionRegistry functionRegistry;
    private final Set<Expression> groupBy;
    private boolean foundExpressionInGroupBy;

    private Optional<FunctionName> aggFunctionName = Optional.empty();
    private boolean visitedAggFunction = false;

    private AggregateVisitor(
        final AggAnalyzer aggAnalyzer,
        final Set<Expression> groupBy,
        final boolean foundExpressionInGroupBy,
        final BiConsumer<Optional<FunctionName>, Expression> dereferenceCollector
    ) {
      this.defaultArgument = aggAnalyzer.analysis.getDefaultArgument();
      this.aggregateAnalysis = aggAnalyzer.aggregateAnalysis;
      this.functionRegistry = aggAnalyzer.functionRegistry;
      this.groupBy = groupBy;
      this.foundExpressionInGroupBy = foundExpressionInGroupBy;
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

      if (groupBy.contains(functionCall)) {
        foundExpressionInGroupBy = true;
      }
      super.visitFunctionCall(functionCall, context);

      if (aggregateFunc) {
        aggFunctionName = Optional.empty();
      }

      return null;
    }

    @Override
    public Void visitUnqualifiedColumnReference(
        final UnqualifiedColumnReferenceExp node,
        final Void context
    ) {
      if (!foundExpressionInGroupBy || visitedAggFunction) {
        dereferenceCollector.accept(aggFunctionName, node);
      }
      foundExpressionInGroupBy = false;
      if (!SystemColumns.isWindowBound(node.getColumnName())) {
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

    @Override
    public Void visitSubscriptExpression(final SubscriptExpression node, final Void context) {
      if (groupBy.contains(node)) {
        foundExpressionInGroupBy = true;
      }
      super.visitSubscriptExpression(node, context);
      return null;
    }
  }
}
