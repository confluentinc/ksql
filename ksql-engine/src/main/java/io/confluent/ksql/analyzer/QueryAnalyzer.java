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

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.collect.Sets.SetView;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.parser.tree.DereferenceExpression;
import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.parser.tree.ExpressionTreeRewriter;
import io.confluent.ksql.parser.tree.FunctionCall;
import io.confluent.ksql.parser.tree.QualifiedName;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.parser.tree.Sink;
import io.confluent.ksql.serde.SerdeOption;
import io.confluent.ksql.util.AggregateExpressionRewriter;
import io.confluent.ksql.util.KsqlException;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class QueryAnalyzer {
  private final MetaStore metaStore;
  private final String outputTopicPrefix;
  private final Set<SerdeOption> defaultSerdeOptions;

  public QueryAnalyzer(
      final MetaStore metaStore,
      final String outputTopicPrefix,
      final Set<SerdeOption> defaultSerdeOptions
  ) {
    this.metaStore = requireNonNull(metaStore, "metaStore");
    this.outputTopicPrefix = requireNonNull(outputTopicPrefix, "outputTopicPrefix");
    this.defaultSerdeOptions = ImmutableSet.copyOf(
        requireNonNull(defaultSerdeOptions, "defaultSerdeOptions"));
  }

  public Analysis analyze(
      final String sqlExpression,
      final Query query,
      final Optional<Sink> sink
  ) {
    return new Analyzer(metaStore, outputTopicPrefix, defaultSerdeOptions)
        .analyze(sqlExpression, query, sink);
  }

  public AggregateAnalysis analyzeAggregate(final Query query, final Analysis analysis) {
    final MutableAggregateAnalysis aggregateAnalysis = new MutableAggregateAnalysis();
    final DereferenceExpression defaultArgument = analysis.getDefaultArgument();
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
          .map(QualifiedName::getSuffix)
          .collect(Collectors.joining(", "));
      throw new KsqlException("Use of aggregate functions requires a GROUP BY clause. "
          + "Aggregate function(s): " + aggFuncs);
    }

    processGroupByExpression(
        analysis,
        aggregateAnalyzer
    );

    if (analysis.getHavingExpression() != null) {
      processHavingExpression(
          analysis,
          aggregateAnalysis,
          aggregateAnalyzer,
          aggregateExpressionRewriter
      );
    }

    enforceAggregateRules(query, analysis, aggregateAnalysis);
    return aggregateAnalysis;
  }

  private static void processHavingExpression(
      final Analysis analysis,
      final MutableAggregateAnalysis aggregateAnalysis,
      final AggregateAnalyzer aggregateAnalyzer,
      final AggregateExpressionRewriter aggregateExpressionRewriter
  ) {
    final Expression exp = analysis.getHavingExpression();

    aggregateAnalyzer.processHaving(exp);

    aggregateAnalysis.setHavingExpression(
        ExpressionTreeRewriter.rewriteWith(aggregateExpressionRewriter::process, exp));
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
    for (final Expression exp : analysis.getSelectExpressions()) {
      aggregateAnalyzer.processSelect(exp);

      aggregateAnalysis.addFinalSelectExpression(
          ExpressionTreeRewriter.rewriteWith(aggregateExpressionRewriter::process, exp));
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

    final Set<Expression> unmatchedSelects = aggregateAnalysis.getNonAggregateSelectExpressions()
        .entrySet()
        .stream()
        // Remove any that exactly match a group by expression:
        .filter(e -> !groupByExprs.contains(e.getKey()))
        // Remove any that are constants,
        // or expressions where all params exactly match a group by expression:
        .filter(e -> !Sets.difference(e.getValue(), groupByExprs).isEmpty())
        .map(Map.Entry::getKey)
        .collect(Collectors.toSet());

    if (!unmatchedSelects.isEmpty()) {
      throw new KsqlException(
          "Non-aggregate SELECT expression(s) not part of GROUP BY: " + unmatchedSelects);
    }

    final SetView<DereferenceExpression> unmatchedSelectsAgg = Sets
        .difference(aggregateAnalysis.getAggregateSelectFields(), groupByExprs);
    if (!unmatchedSelectsAgg.isEmpty()) {
      throw new KsqlException(
          "Field used in aggregate SELECT expression(s) "
              + "outside of aggregate functions not part of GROUP BY: " + unmatchedSelectsAgg);
    }

    final Set<DereferenceExpression> havingColumns = aggregateAnalysis
        .getNonAggregateHavingFields();

    final Set<DereferenceExpression> havingOnly = Sets.difference(havingColumns, groupByExprs);
    if (!havingOnly.isEmpty()) {
      throw new KsqlException(
          "Non-aggregate HAVING expression not part of GROUP BY: " + havingOnly);
    }
  }
}
