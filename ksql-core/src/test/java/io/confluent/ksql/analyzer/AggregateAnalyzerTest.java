package io.confluent.ksql.analyzer;

import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.parser.KSQLParser;
import io.confluent.ksql.parser.rewrite.AggregateExpressionRewriter;
import io.confluent.ksql.parser.tree.ComparisonExpression;
import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.parser.tree.ExpressionTreeRewriter;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.util.KSQLTestUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

public class AggregateAnalyzerTest {

  private static final KSQLParser KSQL_PARSER = new KSQLParser();
  private MetaStore metaStore;

  @Before
  public void init() {
    metaStore = KSQLTestUtil.getNewMetaStore();
  }

  private Analysis analyze(final String queryStr) {
    List<Statement> statements = KSQL_PARSER.buildAST(queryStr, metaStore);
//    System.out.println(SqlFormatterQueryRewrite.formatSql(statements.get(0)).replace("\n", " "));
    // Analyze the query to resolve the references and extract oeprations
    Analysis analysis = new Analysis();
    Analyzer analyzer = new Analyzer(analysis, metaStore);
    analyzer.process(statements.get(0), new AnalysisContext(null, null));
    return analysis;
  }

  private AggregateAnalysis analyzeAggregates(final String queryStr) {
    System.out.println("Test query:" + queryStr);
    Analysis analysis = analyze(queryStr);
    AggregateAnalysis aggregateAnalysis = new AggregateAnalysis();
    AggregateAnalyzer aggregateAnalyzer = new AggregateAnalyzer(aggregateAnalysis, metaStore);
    AggregateExpressionRewriter aggregateExpressionRewriter = new AggregateExpressionRewriter();
    for (Expression expression: analysis.getSelectExpressions()) {
      aggregateAnalyzer.process(expression, new AnalysisContext(null, null));
      if (!aggregateAnalyzer.isHasAggregateFunction()) {
        aggregateAnalysis.getNonAggResultColumns().add(expression);
      }
      aggregateAnalysis.getFinalSelectExpressions().add(
          ExpressionTreeRewriter.rewriteWith(aggregateExpressionRewriter, expression));
      aggregateAnalyzer.setHasAggregateFunction(false);
    }

    if (analysis.getHavingExpression() != null) {
      aggregateAnalyzer.process(analysis.getHavingExpression(), new AnalysisContext(null, null));
      if (!aggregateAnalyzer.isHasAggregateFunction()) {
        aggregateAnalysis.getNonAggResultColumns().add(analysis.getHavingExpression());
      }
      aggregateAnalysis.setHavingExpression(ExpressionTreeRewriter.rewriteWith(aggregateExpressionRewriter,
                                                                               analysis.getHavingExpression()));
      aggregateAnalyzer.setHasAggregateFunction(false);
    }

    return aggregateAnalysis;
  }

  @Test
  public void testSimpleAggregateQueryAnalysis() throws Exception {
    String queryStr = "SELECT col1, count(col1) FROM test1 WHERE col0 > 100 group by col1;";
    AggregateAnalysis aggregateAnalysis = analyzeAggregates(queryStr);
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
    String queryStr = "SELECT col1, sum(col3), count(col1) FROM test1 WHERE col0 > 100 group by "
                      + "col1;";
    AggregateAnalysis aggregateAnalysis = analyzeAggregates(queryStr);
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
    String queryStr = "SELECT col1, sum(col3*col0), sum(floor(col3)*3.0) FROM test1 window w "
                      + "TUMBLING ( size 2 second) WHERE col0 > "
                      + "100 "
                      + "group "
                      + "by "
                      + "col1;";
    AggregateAnalysis aggregateAnalysis = analyzeAggregates(queryStr);
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
    String queryStr = "SELECT col1, sum(col3*col0)/count(col1), sum(floor(col3)*3.0) FROM test1 "
                      + "window w "
                      + "TUMBLING ( size 2 second) WHERE col0 > "
                      + "100 "
                      + "group "
                      + "by "
                      + "col1;";
    AggregateAnalysis aggregateAnalysis = analyzeAggregates(queryStr);
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
    String queryStr = "SELECT col1, sum(col3*col0)/count(col1), sum(floor(col3)*3.0) FROM test1 "
                      + "window w "
                      + "TUMBLING ( size 2 second) WHERE col0 > "
                      + "100 "
                      + "group "
                      + "by "
                      + "col1 "
                      + "having count(col1) > 10;";
    AggregateAnalysis aggregateAnalysis = analyzeAggregates(queryStr);
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
