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

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.parser.tree.DereferenceExpression;
import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.parser.tree.ExpressionTreeRewriter;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.parser.tree.QuerySpecification;
import io.confluent.ksql.util.AggregateExpressionRewriter;
import io.confluent.ksql.util.KsqlException;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class QueryAnalyzer {
  private final MetaStore metaStore;
  private final String outputTopicPrefix;

  public QueryAnalyzer(
      final MetaStore metaStore,
      final String outputTopicPrefix
  ) {
    this.metaStore = Objects.requireNonNull(metaStore, "metaStore");
    this.outputTopicPrefix = Objects.requireNonNull(outputTopicPrefix, "outputTopicPrefix");
  }

  public Analysis analyze(final String sqlExpression, final Query query) {
    final Analysis analysis = new Analysis();
    final Analyzer analyzer = new Analyzer(sqlExpression, analysis, metaStore, outputTopicPrefix);
    analyzer.process(query, new AnalysisContext());
    return analysis;
  }

  public AggregateAnalysis analyzeAggregate(final Query query, final Analysis analysis) {
    final AggregateAnalysis aggregateAnalysis = new AggregateAnalysis();
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

    if (!aggregateAnalysis.getAggregateFunctionArguments().isEmpty()
        && analysis.getGroupByExpressions().isEmpty()) {
      throw new KsqlException("Aggregate query needs GROUP BY clause. query:" + query);
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
      final AggregateAnalysis aggregateAnalysis,
      final AggregateAnalyzer aggregateAnalyzer,
      final AggregateExpressionRewriter aggregateExpressionRewriter
  ) {
    final Expression exp = analysis.getHavingExpression();

    aggregateAnalyzer.processHaving(exp);

    aggregateAnalysis.setHavingExpression(
        ExpressionTreeRewriter.rewriteWith(aggregateExpressionRewriter,exp));
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
      final AggregateAnalysis aggregateAnalysis,
      final AggregateAnalyzer aggregateAnalyzer,
      final AggregateExpressionRewriter aggregateExpressionRewriter
  ) {
    for (final Expression exp : analysis.getSelectExpressions()) {
      aggregateAnalyzer.processSelect(exp);

      aggregateAnalysis.addFinalSelectExpression(
          ExpressionTreeRewriter.rewriteWith(aggregateExpressionRewriter, exp));
    }
  }

  private static void enforceAggregateRules(
      final Query query,
      final Analysis analysis,
      final AggregateAnalysis aggregateAnalysis
  ) {
    if (!((QuerySpecification) query.getQueryBody()).getGroupBy().isPresent()) {
      return;
    }

    if (aggregateAnalysis.getAggregateFunctions().isEmpty()) {
      throw new KsqlException(
          "GROUP BY requires columns using aggregate functions in SELECT clause.");
    }

    final Set<Expression> groupByExprs = ImmutableSet.copyOf(analysis.getGroupByExpressions());

    final HashMap<Expression, Set<DereferenceExpression>> unmatchedSelects =
        new HashMap<>(aggregateAnalysis.getNonAggregateSelectExpressions());

    // Remove exact matchers:
    unmatchedSelects.keySet().removeAll(groupByExprs);

    // Remove expressions where all parameters are within group-by,
    // Or parameters are empty, i.e. literals:
    final Set<Expression> toRemove = unmatchedSelects.entrySet().stream()
        .filter(e -> Sets.difference(e.getValue(), groupByExprs).isEmpty())
        .map(Entry::getKey)
        .collect(Collectors.toSet());

    unmatchedSelects.keySet().removeAll(toRemove);

    if (!unmatchedSelects.isEmpty()) {
      throw new KsqlException(
          "Non-aggregate SELECT expression(s) must be part of GROUP BY: "
              + unmatchedSelects.keySet());
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
