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

import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.parser.tree.ExpressionTreeRewriter;
import io.confluent.ksql.parser.tree.GroupBy;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.parser.tree.QuerySpecification;
import io.confluent.ksql.util.AggregateExpressionRewriter;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class QueryAnalyzer {
  private final MetaStore metaStore;
  private final KsqlConfig config;

  public QueryAnalyzer(final MetaStore metaStore, final KsqlConfig config) {
    this.metaStore = Objects.requireNonNull(metaStore, "metaStore");
    this.config = Objects.requireNonNull(config, "config");
  }

  public Analysis analyze(final String sqlExpression, final Query query) {
    final Analysis analysis = new Analysis();
    final Analyzer analyzer = new Analyzer(sqlExpression, analysis, metaStore, topicPrefix());
    analyzer.process(query, new AnalysisContext());
    return analysis;
  }

  public AggregateAnalysis analyzeAggregate(final Query query, final Analysis analysis) {
    final AggregateAnalysis aggregateAnalysis = new AggregateAnalysis();
    final AggregateAnalyzer aggregateAnalyzer = new
        AggregateAnalyzer(aggregateAnalysis, analysis, metaStore);
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
    final Expression exp = analysis.getHavingExpression();

    processExpression(exp, aggregateAnalysis, aggregateAnalyzer);

    aggregateAnalysis.setHavingExpression(
        ExpressionTreeRewriter.rewriteWith(aggregateExpressionRewriter,exp));
  }

  private void processGroupByExpression(
      final Analysis analysis,
      final AggregateAnalyzer aggregateAnalyzer
  ) {
    for (final Expression exp : analysis.getGroupByExpressions()) {
      aggregateAnalyzer.process(exp, new AnalysisContext());
    }
  }

  private void processSelectExpressions(
      final Analysis analysis,
      final AggregateAnalysis aggregateAnalysis,
      final AggregateAnalyzer aggregateAnalyzer,
      final AggregateExpressionRewriter aggregateExpressionRewriter
  ) {
    for (final Expression exp : analysis.getSelectExpressions()) {
      processExpression(exp, aggregateAnalysis, aggregateAnalyzer);

      aggregateAnalysis.addFinalSelectExpression(
          ExpressionTreeRewriter.rewriteWith(aggregateExpressionRewriter, exp));
    }
  }

  private static void processExpression(
      final Expression expression,
      final AggregateAnalysis aggregateAnalysis,
      final AggregateAnalyzer aggregateAnalyzer) {
    aggregateAnalyzer.process(expression, new AnalysisContext());
    if (!aggregateAnalyzer.isHasAggregateFunction()) {
      aggregateAnalysis.addNonAggResultColumns(expression);
    }
    aggregateAnalyzer.setHasAggregateFunction(false);
  }

  private void enforceAggregateRules(final Query query, final AggregateAnalysis aggregateAnalysis) {
    final Optional<GroupBy> groupBy = ((QuerySpecification) query.getQueryBody()).getGroupBy();
    if (!groupBy.isPresent()) {
      return;
    }

    final Set<Expression> groups = groupBy.get()
        .getGroupingElements()
        .stream()
        .flatMap(group -> group.enumerateGroupingSets().stream())
        .flatMap(Set::stream)
        .collect(Collectors.toSet());

    final List<Expression> selectOnly = aggregateAnalysis.getNonAggResultColumns().stream()
        .filter(exp -> !groups.contains(exp))
        .collect(Collectors.toList());

    if (!selectOnly.isEmpty()) {
      throw new KsqlException(
          "Non-aggregate SELECT expression must be part of GROUP BY: " + selectOnly);
    }
  }

  private String topicPrefix() {
    return config.getString(KsqlConfig.KSQL_OUTPUT_TOPIC_NAME_PREFIX_CONFIG);
  }
}
