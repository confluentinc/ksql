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

import static java.util.Objects.requireNonNull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.collect.Sets.SetView;
import io.confluent.ksql.engine.rewrite.ExpressionTreeRewriter;
import io.confluent.ksql.execution.expression.tree.ColumnReferenceExp;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.expression.tree.FunctionCall;
import io.confluent.ksql.execution.plan.SelectExpression;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.name.FunctionName;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.parser.tree.Sink;
import io.confluent.ksql.serde.SerdeOption;
import io.confluent.ksql.util.AggregateExpressionRewriter;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.TableFunctionExpressionRewriter;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

// CHECKSTYLE_RULES.OFF: ClassDataAbstractionCoupling
public class QueryAnalyzer {
  // CHECKSTYLE_RULES.ON: ClassDataAbstractionCoupling

  private final Analyzer analyzer;
  private final MetaStore metaStore;
  private final QueryValidator continuousValidator;
  private final QueryValidator staticValidator;

  public QueryAnalyzer(
      final MetaStore metaStore,
      final String outputTopicPrefix,
      final Set<SerdeOption> defaultSerdeOptions
  ) {
    this(
        metaStore,
        new Analyzer(metaStore, outputTopicPrefix, defaultSerdeOptions),
        new ContinuousQueryValidator(),
        new StaticQueryValidator()
    );
  }

  @VisibleForTesting
  QueryAnalyzer(
      final MetaStore metaStore,
      final Analyzer analyzer,
      final QueryValidator continuousValidator,
      final QueryValidator staticValidator
  ) {
    this.metaStore = requireNonNull(metaStore, "metaStore");
    this.analyzer = requireNonNull(analyzer, "analyzer");
    this.continuousValidator = requireNonNull(continuousValidator, "continuousValidator");
    this.staticValidator = requireNonNull(staticValidator, "staticValidator");
  }

  public Analysis analyze(
      final Query query,
      final Optional<Sink> sink
  ) {
    final Analysis analysis = analyzer.analyze(query, sink);

    if (query.isStatic()) {
      staticValidator.validate(analysis);
    } else {
      continuousValidator.validate(analysis);
    }

    return analysis;
  }

  public AggregateAnalysis analyzeAggregate(final Query query, final Analysis analysis) {
    final MutableAggregateAnalysis aggregateAnalysis = new MutableAggregateAnalysis();
    final ColumnReferenceExp defaultArgument = analysis.getDefaultArgument();
    final AggregateAnalyzer aggregateAnalyzer =
        new AggregateAnalyzer(aggregateAnalysis, defaultArgument, metaStore);
    final AggregateExpressionRewriter aggregateExpressionRewriter =
        new AggregateExpressionRewriter(metaStore);

    processSelectExpressions(
        analysis,
        aggregateAnalysis,
        aggregateAnalyzer,
        aggregateExpressionRewriter
    );

    if (!aggregateAnalysis.getAggregateFunctions().isEmpty()
        && analysis.getGroupByExpressions().isEmpty()) {
      final String aggFuncs = aggregateAnalysis.getAggregateFunctions().stream()
          .map(FunctionCall::getName)
          .map(FunctionName::name)
          .collect(Collectors.joining(", "));
      throw new KsqlException("Use of aggregate functions requires a GROUP BY clause. "
          + "Aggregate function(s): " + aggFuncs);
    }

    processGroupByExpression(
        analysis,
        aggregateAnalyzer
    );

    analysis.getHavingExpression().ifPresent(having ->
        processHavingExpression(
            having,
            aggregateAnalysis,
            aggregateAnalyzer,
            aggregateExpressionRewriter
        )
    );

    enforceAggregateRules(query, analysis, aggregateAnalysis);
    return aggregateAnalysis;
  }

  public TableFunctionAnalysis analyzeTableFunctions(final Query query, final Analysis analysis) {
    final TableFunctionAnalysis tableFunctionAnalysis = new TableFunctionAnalysis();
    final ColumnReferenceExp defaultArgument = analysis.getDefaultArgument();
    final TableFunctionAnalyzer tableFunctionAnalyzer =
        new TableFunctionAnalyzer(tableFunctionAnalysis, defaultArgument, metaStore);
    final TableFunctionExpressionRewriter tableFunctionExpressionRewriter =
        new TableFunctionExpressionRewriter(metaStore);

    processSelectExpressionsForTableFunctionAnalysis(
        analysis,
        tableFunctionAnalysis,
        tableFunctionAnalyzer,
        tableFunctionExpressionRewriter
    );

    return tableFunctionAnalysis;
  }

  private static void processHavingExpression(
      final Expression having,
      final MutableAggregateAnalysis aggregateAnalysis,
      final AggregateAnalyzer aggregateAnalyzer,
      final AggregateExpressionRewriter aggregateExpressionRewriter
  ) {
    aggregateAnalyzer.processHaving(having);

    aggregateAnalysis.setHavingExpression(
        ExpressionTreeRewriter.rewriteWith(aggregateExpressionRewriter::process, having));
  }

  private static void processGroupByExpression(
      final Analysis analysis,
      final AggregateAnalyzer aggregateAnalyzer
  ) {
    for (final Expression exp : analysis.getGroupByExpressions()) {
      aggregateAnalyzer.processGroupBy(exp);
    }
  }

  private static void processSelectExpressions(
      final Analysis analysis,
      final MutableAggregateAnalysis aggregateAnalysis,
      final AggregateAnalyzer aggregateAnalyzer,
      final AggregateExpressionRewriter aggregateExpressionRewriter
  ) {
    for (final SelectExpression select : analysis.getSelectExpressions()) {
      final Expression exp = select.getExpression();
      aggregateAnalyzer.processSelect(exp);

      aggregateAnalysis.addFinalSelectExpression(
          ExpressionTreeRewriter.rewriteWith(aggregateExpressionRewriter::process, exp));
    }
  }

  private static void processSelectExpressionsForTableFunctionAnalysis(
      final Analysis analysis,
      final TableFunctionAnalysis tableFunctionAnalysis,
      final TableFunctionAnalyzer tableFunctionAnalyzer,
      final TableFunctionExpressionRewriter tableFunctionExpressionRewriter
  ) {
    for (final SelectExpression select : analysis.getSelectExpressions()) {
      final Expression exp = select.getExpression();
      tableFunctionAnalyzer.processSelect(exp);

      tableFunctionAnalysis.addFinalSelectExpression(
          ExpressionTreeRewriter.rewriteWith(tableFunctionExpressionRewriter::process, exp));
    }
  }

  private static void enforceAggregateRules(
      final Query query,
      final Analysis analysis,
      final AggregateAnalysis aggregateAnalysis
  ) {
    if (!query.getGroupBy().isPresent()) {
      return;
    }

    if (aggregateAnalysis.getAggregateFunctions().isEmpty()) {
      throw new KsqlException(
          "GROUP BY requires columns using aggregate functions in SELECT clause.");
    }

    final Set<Expression> groupByExprs = ImmutableSet.copyOf(analysis.getGroupByExpressions());

    final List<String> unmatchedSelects = aggregateAnalysis.getNonAggregateSelectExpressions()
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
        .difference(aggregateAnalysis.getAggregateSelectFields(), groupByExprs);
    if (!unmatchedSelectsAgg.isEmpty()) {
      throw new KsqlException(
          "Field used in aggregate SELECT expression(s) "
              + "outside of aggregate functions not part of GROUP BY: " + unmatchedSelectsAgg);
    }

    final Set<ColumnReferenceExp> havingColumns = aggregateAnalysis
        .getNonAggregateHavingFields();

    final Set<ColumnReferenceExp> havingOnly = Sets.difference(havingColumns, groupByExprs);
    if (!havingOnly.isEmpty()) {
      throw new KsqlException(
          "Non-aggregate HAVING expression not part of GROUP BY: " + havingOnly);
    }
  }
}
