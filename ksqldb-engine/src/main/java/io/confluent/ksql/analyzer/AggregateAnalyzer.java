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

    // The list of expressions that appear in the SELECT clause outside of aggregate functions.
    // Used for throwing an error if these columns are not part of the GROUP BY clause.
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
      captureNonAggregateSelectNotPartOfGroupBy(expression, nonAggParams);
      aggregateAnalysis.addFinalSelectExpression(expression);
    }

    private void processGroupBy(final Expression expression) {
      final AggregateVisitor visitor = new AggregateVisitor(
          this,
          groupBy,
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
          (aggFuncName, node) ->
            throwOnWindowBoundColumnIfWindowedAggregate(node));

      visitor.process(expression, null);
    }

    private void processHaving(final Expression expression) {
      final AggregateVisitor visitor = new AggregateVisitor(
          this,
          groupBy,
          (aggFuncName, node) -> {
            throwOnWindowBoundColumnIfWindowedAggregate(node);

            if (!aggFuncName.isPresent()) {
              captureNonAggregateHavingNotPartOfGroupBy(node);
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

      if (!(node instanceof ColumnReferenceExp)) {
        return;
      }
      // For non-windowed sources, with a windowed GROUP BY, they are only supported in selects:
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
      final boolean matchesGroupBy = groupBy.contains(expression);
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

    private void captureNonAggregateHavingNotPartOfGroupBy(final Expression nonAggColumn) {
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
                + "Either add the column(s) to the GROUP BY or remove them from the SELECT."
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

  /**
   * This visitor performs two tasks: Create the input schema to the AggregateNode and validations.
   *
   * <p>For creating the input schema, it checks if any expression along the path from the root
   * expression to the leaf (UnqualifiedColumnReference) is part of the groupBy. If at least one is,
   * then the UnqualifiedColumnReference is added to the schema.
   *
   * <p>For validation, the visitor checks that:
   * <ol>
   *  <li> expressions not in aggregate functions are part of the grouping clause </li>
   *  <li> aggregate functions are not nested </li>
   *  <li> window clauses (windowstart, windowend) don't appear in aggregate functions or
   *  groupBy </li>
   *  <li> aggregate functions don't appear in the groupBy clause </li>
   *  <li> expressions in the having clause are either aggregate functions or grouping keys </li>
   * </ol>
   */
  private static final class AggregateVisitor extends TraversalExpressionVisitor<Void> {

    private final BiConsumer<Optional<FunctionName>, Expression> dereferenceCollector;
    private final ColumnReferenceExp defaultArgument;
    private final MutableAggregateAnalysis aggregateAnalysis;
    private final FunctionRegistry functionRegistry;
    private final Set<Expression> groupBy;
    private Expression currentlyInExpressionPartOfGroupBy;

    private Optional<FunctionName> aggFunctionName = Optional.empty();
    private boolean currentlyInAggregateFunction = false;

    private AggregateVisitor(
        final AggAnalyzer aggAnalyzer,
        final Set<Expression> groupBy,
        final BiConsumer<Optional<FunctionName>, Expression> dereferenceCollector
    ) {
      this.defaultArgument = aggAnalyzer.analysis.getDefaultArgument();
      this.aggregateAnalysis = aggAnalyzer.aggregateAnalysis;
      this.functionRegistry = aggAnalyzer.functionRegistry;
      this.groupBy = groupBy;
      this.dereferenceCollector = requireNonNull(dereferenceCollector, "dereferenceCollector");
    }

    @Override
    public Void process(final Expression node, final Void context) {
      if (groupBy.contains(node) && currentlyInExpressionPartOfGroupBy == null) {
        currentlyInExpressionPartOfGroupBy = node;
      }
      super.process(node, context);
      if (currentlyInExpressionPartOfGroupBy != null
          && currentlyInExpressionPartOfGroupBy == node) {
        currentlyInExpressionPartOfGroupBy = null;
      }
      return null;
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

        currentlyInAggregateFunction = true;
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
    public Void visitUnqualifiedColumnReference(
        final UnqualifiedColumnReferenceExp node,
        final Void context
    ) {
      if (currentlyInExpressionPartOfGroupBy == null
          || currentlyInAggregateFunction
          || SystemColumns.isWindowBound(node.getColumnName())) {
        dereferenceCollector.accept(aggFunctionName, node);
      }
      if (!SystemColumns.isWindowBound(node.getColumnName())) {
        // Used to infer the required columns in the INPUT schema of the aggregate node
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
}
