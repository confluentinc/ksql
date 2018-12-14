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
package io.confluent.ksql.structured;

import io.confluent.ksql.analyzer.AggregateAnalysis;
import io.confluent.ksql.analyzer.AggregateAnalyzer;
import io.confluent.ksql.analyzer.Analysis;
import io.confluent.ksql.analyzer.AnalysisContext;
import io.confluent.ksql.analyzer.Analyzer;
import io.confluent.ksql.function.InternalFunctionRegistry;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.parser.KsqlParser;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.parser.tree.ExpressionTreeRewriter;
import io.confluent.ksql.planner.LogicalPlanner;
import io.confluent.ksql.planner.plan.PlanNode;
import io.confluent.ksql.util.AggregateExpressionRewriter;
import java.util.List;

public class LogicalPlanBuilder {

  private final MetaStore metaStore;
  private final KsqlParser parser = new KsqlParser();
  private final InternalFunctionRegistry functionRegistry = new InternalFunctionRegistry();

  public LogicalPlanBuilder(final MetaStore metaStore) {
    this.metaStore = metaStore;
  }

  public PlanNode buildLogicalPlan(final String queryStr) {
    final List<PreparedStatement> statements = parser.buildAst(queryStr, metaStore);
    final Analysis analysis = new Analysis();
    final Analyzer analyzer = new Analyzer(queryStr, analysis, metaStore, "");
    analyzer.process(statements.get(0).getStatement(), new AnalysisContext(null));
    final AggregateAnalysis aggregateAnalysis = new AggregateAnalysis();
    final AggregateAnalyzer aggregateAnalyzer = new AggregateAnalyzer(aggregateAnalysis, analysis, functionRegistry);
    final AggregateExpressionRewriter aggregateExpressionRewriter = new AggregateExpressionRewriter
        (functionRegistry);
    for (final Expression expression: analysis.getSelectExpressions()) {
      aggregateAnalyzer.process(expression, new AnalysisContext(null));
      if (!aggregateAnalyzer.isHasAggregateFunction()) {
        aggregateAnalysis.addNonAggResultColumns(expression);
      }
      aggregateAnalysis.addFinalSelectExpression(
          ExpressionTreeRewriter.rewriteWith(aggregateExpressionRewriter, expression));
      aggregateAnalyzer.setHasAggregateFunction(false);
    }
    // Build a logical plan
    return new LogicalPlanner(analysis, aggregateAnalysis, functionRegistry).buildPlan();
  }
}
