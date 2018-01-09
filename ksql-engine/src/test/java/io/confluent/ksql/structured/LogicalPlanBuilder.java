/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.confluent.ksql.structured;

import java.util.List;

import io.confluent.ksql.analyzer.AggregateAnalysis;
import io.confluent.ksql.analyzer.AggregateAnalyzer;
import io.confluent.ksql.analyzer.Analysis;
import io.confluent.ksql.analyzer.AnalysisContext;
import io.confluent.ksql.analyzer.Analyzer;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.parser.KsqlParser;
import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.parser.tree.ExpressionTreeRewriter;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.planner.LogicalPlanner;
import io.confluent.ksql.planner.plan.PlanNode;
import io.confluent.ksql.util.AggregateExpressionRewriter;

public class LogicalPlanBuilder {

  private final MetaStore metaStore;
  private final KsqlParser parser = new KsqlParser();
  private final FunctionRegistry functionRegistry = new FunctionRegistry();

  public LogicalPlanBuilder(final MetaStore metaStore) {
    this.metaStore = metaStore;
  }

  public PlanNode buildLogicalPlan(String queryStr) {
    List<Statement> statements = parser.buildAst(queryStr, metaStore);
    Analysis analysis = new Analysis();
    Analyzer analyzer = new Analyzer(queryStr, analysis, metaStore);
    analyzer.process(statements.get(0), new AnalysisContext(null));
    AggregateAnalysis aggregateAnalysis = new AggregateAnalysis();
    AggregateAnalyzer aggregateAnalyzer = new AggregateAnalyzer(aggregateAnalysis, analysis, functionRegistry);
    AggregateExpressionRewriter aggregateExpressionRewriter = new AggregateExpressionRewriter
        (functionRegistry);
    for (Expression expression: analysis.getSelectExpressions()) {
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
