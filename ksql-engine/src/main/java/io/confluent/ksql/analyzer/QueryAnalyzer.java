/**
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

import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.parser.tree.ExpressionTreeRewriter;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.parser.tree.QuerySpecification;
import io.confluent.ksql.util.AggregateExpressionRewriter;
import io.confluent.ksql.util.KsqlException;

public class QueryAnalyzer {
  private final MetaStore metaStore;
  private final FunctionRegistry functionRegistry;

  public QueryAnalyzer(final MetaStore metaStore, final FunctionRegistry functionRegistry) {
    this.metaStore = metaStore;
    this.functionRegistry = functionRegistry;
  }

  public Analysis analyze(final String sqlExpression, final Query query) {
    Analysis analysis = new Analysis();
    Analyzer analyzer = new Analyzer(sqlExpression, analysis, metaStore);
    analyzer.process(query, new AnalysisContext());
    return analysis;
  }

  public AggregateAnalysis analyzeAggregate(final Query query, final Analysis analysis) {
    AggregateAnalysis aggregateAnalysis = new AggregateAnalysis();
    AggregateAnalyzer aggregateAnalyzer = new
        AggregateAnalyzer(aggregateAnalysis, analysis, functionRegistry);
    AggregateExpressionRewriter aggregateExpressionRewriter =
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

    // TODO: make sure only aggregates are in the expression. For now we assume this is the case.
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
    aggregateAnalyzer.process(
        analysis.getHavingExpression(),
        new AnalysisContext()
    );
    if (!aggregateAnalyzer.isHasAggregateFunction()) {
      aggregateAnalysis.addNonAggResultColumns(analysis.getHavingExpression());
    }
    aggregateAnalysis
        .setHavingExpression(ExpressionTreeRewriter.rewriteWith(
            aggregateExpressionRewriter,
            analysis.getHavingExpression()
        ));
    aggregateAnalyzer.setHasAggregateFunction(false);
  }

  private void processSelectExpressions(
      final Analysis analysis,
      final AggregateAnalysis aggregateAnalysis,
      final AggregateAnalyzer aggregateAnalyzer,
      final AggregateExpressionRewriter aggregateExpressionRewriter
  ) {
    for (Expression expression : analysis.getSelectExpressions()) {
      aggregateAnalyzer.process(expression, new AnalysisContext());
      if (!aggregateAnalyzer.isHasAggregateFunction()) {
        aggregateAnalysis.addNonAggResultColumns(expression);
      }
      aggregateAnalysis.addFinalSelectExpression(ExpressionTreeRewriter.rewriteWith(
          aggregateExpressionRewriter,
          expression
      ));
      aggregateAnalyzer.setHasAggregateFunction(false);
    }
  }

  private void enforceAggregateRules(final Query query, final AggregateAnalysis aggregateAnalysis) {
    if (!((QuerySpecification) query.getQueryBody()).getGroupBy().isPresent()) {
      return;
    }
    int numberOfNonAggProjections = aggregateAnalysis.getNonAggResultColumns().size();
    int groupBySize = ((QuerySpecification) query.getQueryBody()).getGroupBy().get()
        .getGroupingElements().size();
    if (numberOfNonAggProjections != groupBySize) {
      throw new KsqlException("Group by elements should match the SELECT expressions.");
    }
  }
}
