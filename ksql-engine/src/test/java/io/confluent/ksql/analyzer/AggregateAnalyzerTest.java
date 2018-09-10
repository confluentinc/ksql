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

import static io.confluent.ksql.testutils.AnalysisTestUtil.analyzeQuery;

import io.confluent.ksql.function.InternalFunctionRegistry;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.parser.tree.ComparisonExpression;
import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.parser.tree.ExpressionTreeRewriter;
import io.confluent.ksql.util.AggregateExpressionRewriter;
import io.confluent.ksql.util.MetaStoreFixture;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class AggregateAnalyzerTest {

  private MetaStore metaStore;
  private InternalFunctionRegistry functionRegistry = new InternalFunctionRegistry();

  @Before
  public void init() {
    metaStore = MetaStoreFixture.getNewMetaStore(functionRegistry);
  }

  private AggregateAnalysis analyzeAggregates(final String queryStr) {
    System.out.println("Test query:" + queryStr);
    final Analysis analysis = analyzeQuery(queryStr, metaStore);
    final AggregateAnalysis aggregateAnalysis = new AggregateAnalysis();
    final AggregateAnalyzer aggregateAnalyzer = new AggregateAnalyzer(aggregateAnalysis, analysis,
                                                                functionRegistry);
    final AggregateExpressionRewriter aggregateExpressionRewriter = new AggregateExpressionRewriter(
        functionRegistry);
    for (final Expression expression: analysis.getSelectExpressions()) {
      aggregateAnalyzer.process(expression, new AnalysisContext(null));
      if (!aggregateAnalyzer.isHasAggregateFunction()) {
        aggregateAnalysis.addNonAggResultColumns(expression);
      }
      aggregateAnalysis.addFinalSelectExpression(
          ExpressionTreeRewriter.rewriteWith(aggregateExpressionRewriter, expression));
      aggregateAnalyzer.setHasAggregateFunction(false);
    }

    if (analysis.getHavingExpression() != null) {
      aggregateAnalyzer.process(analysis.getHavingExpression(), new AnalysisContext(null));
      if (!aggregateAnalyzer.isHasAggregateFunction()) {
        aggregateAnalysis.addNonAggResultColumns(analysis.getHavingExpression());
      }
      aggregateAnalysis.setHavingExpression(ExpressionTreeRewriter.rewriteWith(aggregateExpressionRewriter,
                                                                               analysis.getHavingExpression()));
      aggregateAnalyzer.setHasAggregateFunction(false);
    }

    return aggregateAnalysis;
  }

  @Test
  public void testSimpleAggregateQueryAnalysis() throws Exception {
    final String queryStr = "SELECT col1, count(col1) FROM test1 WHERE col0 > 100 group by col1;";
    final AggregateAnalysis aggregateAnalysis = analyzeAggregates(queryStr);
    Assert.assertNotNull(aggregateAnalysis);
    Assert.assertTrue(aggregateAnalysis.getFunctionList().size() == 1);
    Assert.assertTrue(aggregateAnalysis.getFunctionList().get(0).getName().getSuffix()
                          .equalsIgnoreCase("count"));
    Assert.assertTrue(aggregateAnalysis.getAggregateFunctionArguments().get(0).toString()
                          .equalsIgnoreCase("test1.col1"));
    Assert.assertTrue(aggregateAnalysis.getRequiredColumnsList().size() == 1);
    Assert.assertTrue(aggregateAnalysis.getRequiredColumnsList().get(0).toString()
                          .equalsIgnoreCase("test1.col1"));
    Assert.assertTrue(aggregateAnalysis.getRequiredColumnsMap().size() == 1);
    Assert.assertTrue(aggregateAnalysis.getFinalSelectExpressions().size() == 2);

  }

  @Test
  public void testMultipleAggregateQueryAnalysis() throws Exception {
    final String queryStr = "SELECT col1, sum(col3), count(col1) FROM test1 WHERE col0 > 100 group by "
                      + "col1;";
    final AggregateAnalysis aggregateAnalysis = analyzeAggregates(queryStr);
    Assert.assertTrue(aggregateAnalysis.getFunctionList().size() == 2);
    Assert.assertTrue(aggregateAnalysis.getFunctionList().get(0).getName().getSuffix()
                          .equalsIgnoreCase("sum"));
    Assert.assertTrue(aggregateAnalysis.getFunctionList().get(1).getName().getSuffix()
                          .equalsIgnoreCase("count"));
    Assert.assertTrue(aggregateAnalysis.getNonAggResultColumns().size() == 1);
    Assert.assertTrue(aggregateAnalysis.getNonAggResultColumns().get(0).toString()
                          .equalsIgnoreCase("test1.col1"));
    Assert.assertTrue(aggregateAnalysis.getAggregateFunctionArguments().get(0).toString()
                          .equalsIgnoreCase("test1.col3"));
    Assert.assertTrue(aggregateAnalysis.getAggregateFunctionArguments().get(1).toString()
                          .equalsIgnoreCase("test1.col1"));
    Assert.assertTrue(aggregateAnalysis.getFinalSelectExpressions().size() == 3);
    Assert.assertTrue(aggregateAnalysis.getFinalSelectExpressions().get(0).toString()
                          .equalsIgnoreCase("test1.col1"));
    Assert.assertTrue(aggregateAnalysis.getRequiredColumnsList().size() == 2);
    Assert.assertTrue(aggregateAnalysis.getRequiredColumnsList().get(1).toString()
                          .equalsIgnoreCase("test1.col3"));
  }

  @Test
  public void testExpressionArgAggregateQueryAnalysis() {
    final String queryStr = "SELECT col1, sum(col3*col0), sum(floor(col3)*3.0) FROM test1 window w "
                      + "TUMBLING ( size 2 second) WHERE col0 > "
                      + "100 "
                      + "group "
                      + "by "
                      + "col1;";
    final AggregateAnalysis aggregateAnalysis = analyzeAggregates(queryStr);
    Assert.assertTrue(aggregateAnalysis.getFunctionList().size() == 2);
    Assert.assertTrue(aggregateAnalysis.getFunctionList().get(0).getName().getSuffix()
                          .equalsIgnoreCase("sum"));
    Assert.assertTrue(aggregateAnalysis.getFunctionList().get(1).getName().getSuffix()
                          .equalsIgnoreCase("sum"));
    Assert.assertTrue(aggregateAnalysis.getAggregateFunctionArguments().size() == 2);
    Assert.assertTrue(aggregateAnalysis.getAggregateFunctionArguments().get(0).toString()
                          .equalsIgnoreCase("(TEST1.COL3 * TEST1.COL0)"));
    Assert.assertTrue(aggregateAnalysis.getAggregateFunctionArguments().get(1).toString()
                          .equalsIgnoreCase("(FLOOR(TEST1.COL3) * 3.0)"));
    Assert.assertTrue(aggregateAnalysis.getNonAggResultColumns().get(0).toString()
                          .equalsIgnoreCase("test1.col1"));

    Assert.assertTrue(aggregateAnalysis.getFinalSelectExpressions().size() == 3);

    Assert.assertTrue(aggregateAnalysis.getRequiredColumnsList().size() == 3);
    Assert.assertTrue(aggregateAnalysis.getRequiredColumnsList().get(1).toString()
                          .equalsIgnoreCase("test1.col3"));
  }

  @Test
  public void testAggregateWithExpressionQueryAnalysis() {
    final String queryStr = "SELECT col1, sum(col3*col0)/count(col1), sum(floor(col3)*3.0) FROM test1 "
                      + "window w "
                      + "TUMBLING ( size 2 second) WHERE col0 > "
                      + "100 "
                      + "group "
                      + "by "
                      + "col1;";
    final AggregateAnalysis aggregateAnalysis = analyzeAggregates(queryStr);
    Assert.assertTrue(aggregateAnalysis.getFunctionList().size() == 3);
    Assert.assertTrue(aggregateAnalysis.getFunctionList().get(0).getName().getSuffix()
                          .equalsIgnoreCase("sum"));
    Assert.assertTrue(aggregateAnalysis.getFunctionList().get(1).getName().getSuffix()
                          .equalsIgnoreCase("count"));
    Assert.assertTrue(aggregateAnalysis.getFunctionList().get(2).getName().getSuffix()
                          .equalsIgnoreCase("sum"));

    Assert.assertTrue(aggregateAnalysis.getAggregateFunctionArguments().size() == 3);
    Assert.assertTrue(aggregateAnalysis.getAggregateFunctionArguments().get(0).toString()
                          .equalsIgnoreCase("(TEST1.COL3 * TEST1.COL0)"));
    Assert.assertTrue(aggregateAnalysis.getAggregateFunctionArguments().get(1).toString()
                          .equalsIgnoreCase("TEST1.COL1"));
    Assert.assertTrue(aggregateAnalysis.getAggregateFunctionArguments().get(2).toString()
                          .equalsIgnoreCase("(FLOOR(TEST1.COL3) * 3.0)"));
    Assert.assertTrue(aggregateAnalysis.getNonAggResultColumns().get(0).toString()
                          .equalsIgnoreCase("test1.col1"));

    Assert.assertTrue(aggregateAnalysis.getFinalSelectExpressions().size() == 3);

    Assert.assertTrue(aggregateAnalysis.getRequiredColumnsList().size() == 3);
    Assert.assertTrue(aggregateAnalysis.getRequiredColumnsList().get(1).toString()
                          .equalsIgnoreCase("test1.col3"));
  }

  @Test
  public void testAggregateWithExpressionHavingQueryAnalysis() {
    final String queryStr = "SELECT col1, sum(col3*col0)/count(col1), sum(floor(col3)*3.0) FROM test1 "
                      + "window w "
                      + "TUMBLING ( size 2 second) WHERE col0 > "
                      + "100 "
                      + "group "
                      + "by "
                      + "col1 "
                      + "having count(col1) > 10;";
    final AggregateAnalysis aggregateAnalysis = analyzeAggregates(queryStr);
    Assert.assertTrue(aggregateAnalysis.getFunctionList().size() == 4);
    Assert.assertTrue(aggregateAnalysis.getFunctionList().get(0).getName().getSuffix()
                          .equalsIgnoreCase("sum"));
    Assert.assertTrue(aggregateAnalysis.getFunctionList().get(1).getName().getSuffix()
                          .equalsIgnoreCase("count"));
    Assert.assertTrue(aggregateAnalysis.getFunctionList().get(2).getName().getSuffix()
                          .equalsIgnoreCase("sum"));

    Assert.assertTrue(aggregateAnalysis.getAggregateFunctionArguments().size() == 4);
    Assert.assertTrue(aggregateAnalysis.getAggregateFunctionArguments().get(0).toString()
                          .equalsIgnoreCase("(TEST1.COL3 * TEST1.COL0)"));
    Assert.assertTrue(aggregateAnalysis.getAggregateFunctionArguments().get(1).toString()
                          .equalsIgnoreCase("TEST1.COL1"));
    Assert.assertTrue(aggregateAnalysis.getAggregateFunctionArguments().get(2).toString()
                          .equalsIgnoreCase("(FLOOR(TEST1.COL3) * 3.0)"));
    Assert.assertTrue(aggregateAnalysis.getNonAggResultColumns().get(0).toString()
                          .equalsIgnoreCase("test1.col1"));

    Assert.assertTrue(aggregateAnalysis.getFinalSelectExpressions().size() == 3);

    Assert.assertTrue(aggregateAnalysis.getRequiredColumnsList().size() == 3);
    Assert.assertTrue(aggregateAnalysis.getRequiredColumnsList().get(1).toString()
                          .equalsIgnoreCase("test1.col3"));
    Assert.assertTrue(aggregateAnalysis.getHavingExpression() instanceof ComparisonExpression);
    Assert.assertTrue(aggregateAnalysis.getHavingExpression().toString().equalsIgnoreCase(""
                                                                                          + ""
                                                                                          + "(KSQL_AGG_VARIABLE_3 > 10)"));

  }
}
