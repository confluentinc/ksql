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

package io.confluent.ksql.planner;


import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Set;

import io.confluent.ksql.analyzer.AggregateAnalysis;
import io.confluent.ksql.analyzer.AggregateAnalyzer;
import io.confluent.ksql.analyzer.Analysis;
import io.confluent.ksql.analyzer.AnalysisContext;
import io.confluent.ksql.analyzer.Analyzer;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.parser.KsqlParser;
import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.planner.plan.PlanNode;
import io.confluent.ksql.util.MetaStoreFixture;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

public class PlanSourceExtractorVisitorTest {

  private static final KsqlParser KSQL_PARSER = new KsqlParser();

  private MetaStore metaStore;
  private FunctionRegistry functionRegistry;

  @Before
  public void init() {
    metaStore = MetaStoreFixture.getNewMetaStore();
    functionRegistry = new FunctionRegistry();
  }

  private PlanNode buildLogicalPlan(String queryStr) {
    List<Statement> statements = KSQL_PARSER.buildAst(queryStr, metaStore);
    // Analyze the query to resolve the references and extract oeprations
    Analysis analysis = new Analysis();
    Analyzer analyzer = new Analyzer("sqlExpression", analysis, metaStore);
    analyzer.process(statements.get(0), new AnalysisContext(null));
    AggregateAnalysis aggregateAnalysis = new AggregateAnalysis();
    AggregateAnalyzer aggregateAnalyzer = new AggregateAnalyzer(aggregateAnalysis, analysis,
                                                                functionRegistry);
    for (Expression expression: analysis.getSelectExpressions()) {
      aggregateAnalyzer.process(expression, new AnalysisContext(null));
    }
    // Build a logical plan
    PlanNode logicalPlan = new LogicalPlanner(analysis, aggregateAnalysis, functionRegistry).buildPlan();
    return logicalPlan;
  }

  @Test
  public void shouldExtractCorrectSourceForSimpleQuery() {
    PlanNode planNode = buildLogicalPlan("select col0 from TEST2 limit 5;");
    PlanSourceExtractorVisitor planSourceExtractorVisitor = new PlanSourceExtractorVisitor();
    planSourceExtractorVisitor.process(planNode, null);
    Set<String> sourceNames = planSourceExtractorVisitor.getSourceNames();
    assertThat(sourceNames.size(), equalTo(1));
    Assert.assertTrue(sourceNames.contains("TEST2"));
  }

  @Test
  public void shouldExtractCorrectSourceForJoinQuery() {
    PlanNode planNode = buildLogicalPlan("SELECT t1.col1, t2.col1, t1.col4, t2.col2 FROM test1 t1 LEFT JOIN test2 t2 ON t1.col1 = t2.col1;");
    PlanSourceExtractorVisitor planSourceExtractorVisitor = new PlanSourceExtractorVisitor();
    planSourceExtractorVisitor.process(planNode, null);
    Set<String> sourceNames = planSourceExtractorVisitor.getSourceNames();
    assertThat(sourceNames.size(), equalTo(2));
    Assert.assertTrue(sourceNames.contains("TEST1"));
    Assert.assertTrue(sourceNames.contains("TEST2"));
  }

}
