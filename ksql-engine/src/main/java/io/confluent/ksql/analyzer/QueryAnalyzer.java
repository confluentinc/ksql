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

import com.google.common.collect.Sets;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.parser.tree.DereferenceExpression;
import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.parser.tree.ExpressionTreeRewriter;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.parser.tree.QuerySpecification;
import io.confluent.ksql.util.AggregateExpressionRewriter;
import io.confluent.ksql.util.KsqlException;
import java.util.Objects;
import java.util.Set;

public class QueryAnalyzer {
  private final MetaStore metaStore;
  private final FunctionRegistry functionRegistry;
  private final String outputTopicPrefix;

  public QueryAnalyzer(final MetaStore metaStore,
                       final FunctionRegistry functionRegistry,
                       final String outputTopicPrefix) {
    this.metaStore = Objects.requireNonNull(metaStore, "metaStore");
    this.functionRegistry = Objects.requireNonNull(functionRegistry, "functionRegistry");
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
        new AggregateAnalyzer(aggregateAnalysis, defaultArgument, functionRegistry);
    final AggregateExpressionRewriter aggregateExpressionRewriter =
        new AggregateExpressionRewriter(functionRegistry);

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

    enforceAggregateRules(query, aggregateAnalysis);
    return aggregateAnalysis;
  }

  private void processHavingExpression(
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

  private void processGroupByExpression(
      final Analysis analysis,
      final AggregateAnalyzer aggregateAnalyzer
  ) {
    for (final Expression exp : analysis.getGroupByExpressions()) {
      aggregateAnalyzer.processGroupBy(exp);
    }
  }

  private void processSelectExpressions(
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

  private void enforceAggregateRules(final Query query, final AggregateAnalysis aggregateAnalysis) {
    if (!((QuerySpecification) query.getQueryBody()).getGroupBy().isPresent()) {
      return;
    }

    final Set<DereferenceExpression> groupByColumns = aggregateAnalysis
        .getGroupByColumns();

    final Set<DereferenceExpression> selectColumns = aggregateAnalysis
        .getNonAggregateSelectColumns();

    final Set<DereferenceExpression> selectOnly = Sets.difference(selectColumns, groupByColumns);
    if (!selectOnly.isEmpty()) {
      throw new KsqlException(
          "Non-aggregate SELECT expression not part of GROUP BY: " + selectOnly);
    }

    final Set<DereferenceExpression> havingColumns = aggregateAnalysis
        .getNonAggregateHavingColumns();

    final Set<DereferenceExpression> havingOnly = Sets.difference(havingColumns, groupByColumns);
    if (!havingOnly.isEmpty()) {
      throw new KsqlException(
          "Non-aggregate HAVING expression not part of GROUP BY: " + havingOnly);
    }
  }
}
