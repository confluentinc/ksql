package io.confluent.kql.planner;

import io.confluent.kql.analyzer.AggregateAnalysis;
import io.confluent.kql.analyzer.AggregateAnalyzer;
import io.confluent.kql.analyzer.Analysis;
import io.confluent.kql.analyzer.AnalysisContext;
import io.confluent.kql.analyzer.Analyzer;
import io.confluent.kql.metastore.MetaStore;
import io.confluent.kql.parser.KQLParser;
import io.confluent.kql.parser.tree.Expression;
import io.confluent.kql.parser.tree.Statement;
import io.confluent.kql.planner.plan.*;
import io.confluent.kql.util.KQLTestUtil;

import org.apache.kafka.connect.data.Schema;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

public class LogicalPlannerTest {

  private static final KQLParser kqlParser = new KQLParser();

  private MetaStore metaStore;

  @Before
  public void init() {
    metaStore = KQLTestUtil.getNewMetaStore();
  }

  private PlanNode buildLogicalPlan(String queryStr) {
    List<Statement> statements = kqlParser.buildAST(queryStr, metaStore);
    // Analyze the query to resolve the references and extract oeprations
    Analysis analysis = new Analysis();
    Analyzer analyzer = new Analyzer(analysis, metaStore);
    analyzer.process(statements.get(0), new AnalysisContext(null, null));
    AggregateAnalysis aggregateAnalysis = new AggregateAnalysis();
    AggregateAnalyzer aggregateAnalyzer = new AggregateAnalyzer(aggregateAnalysis, metaStore);
    for (Expression expression: analysis.getSelectExpressions()) {
      aggregateAnalyzer.process(expression, new AnalysisContext(null, null));
    }
    // Build a logical plan
    PlanNode logicalPlan = new LogicalPlanner(analysis, aggregateAnalysis).buildPlan();
    return logicalPlan;
  }

  @Test
  public void testSimpleQueryLogicalPlan() throws Exception {
    String simpleQuery = "SELECT col0, col2, col3 FROM test1 WHERE col0 > 100;";
    PlanNode logicalPlan = buildLogicalPlan(simpleQuery);

//    Assert.assertTrue(logicalPlan instanceof OutputKafkaTopicNode);
    Assert.assertTrue(logicalPlan.getSources().get(0) instanceof ProjectNode);
    Assert.assertTrue(logicalPlan.getSources().get(0).getSources().get(0) instanceof FilterNode);
    Assert.assertTrue(logicalPlan.getSources().get(0).getSources().get(0).getSources()
                          .get(0) instanceof StructuredDataSourceNode);

    Assert.assertTrue(logicalPlan.getSchema().fields().size() == 3);
    Assert.assertNotNull(
        ((FilterNode) logicalPlan.getSources().get(0).getSources().get(0)).getPredicate());
  }

  @Test
  public void testSimpleLeftJoinLogicalPlan() throws Exception {
    String simpleQuery = "SELECT t1.col1, t2.col1, t1.col4, t2.col2 FROM test1 t1 LEFT JOIN test2 t2 ON t1.col1 = t2.col1;";
    PlanNode logicalPlan = buildLogicalPlan(simpleQuery);

//    Assert.assertTrue(logicalPlan instanceof OutputKafkaTopicNode);
    Assert.assertTrue(logicalPlan.getSources().get(0) instanceof ProjectNode);
    Assert.assertTrue(logicalPlan.getSources().get(0).getSources().get(0) instanceof JoinNode);
    Assert.assertTrue(logicalPlan.getSources().get(0).getSources().get(0).getSources()
                          .get(0) instanceof StructuredDataSourceNode);
    Assert.assertTrue(logicalPlan.getSources().get(0).getSources().get(0).getSources()
                          .get(1) instanceof StructuredDataSourceNode);

    Assert.assertTrue(logicalPlan.getSchema().fields().size() == 4);

  }

  @Test
  public void testSimpleLeftJoinFilterLogicalPlan() throws Exception {
    String
        simpleQuery =
        "SELECT t1.col1, t2.col1, col5, t2.col4, t2.col2 FROM test1 t1 LEFT JOIN test2 t2 ON "
        + "t1.col1 = t2.col1 WHERE t1.col1 > 10 AND t2.col4 = 10.8;";
    PlanNode logicalPlan = buildLogicalPlan(simpleQuery);

//    Assert.assertTrue(logicalPlan instanceof OutputKafkaTopicNode);
    Assert.assertTrue(logicalPlan.getSources().get(0) instanceof ProjectNode);
    ProjectNode projectNode = (ProjectNode) logicalPlan.getSources().get(0);

    Assert.assertTrue(projectNode.getKeyField().name().equalsIgnoreCase("t1.col1"));
    Assert.assertTrue(projectNode.getSchema().fields().size() == 5);

    Assert.assertTrue(projectNode.getSources().get(0) instanceof FilterNode);
    FilterNode filterNode = (FilterNode) projectNode.getSources().get(0);
    Assert.assertTrue(filterNode.getPredicate().toString()
                          .equalsIgnoreCase("((T1.COL1 > 10) AND (T2.COL4 = 10.8))"));

    Assert.assertTrue(filterNode.getSources().get(0) instanceof JoinNode);
    JoinNode joinNode = (JoinNode) filterNode.getSources().get(0);
    Assert.assertTrue(joinNode.getSources().get(0) instanceof StructuredDataSourceNode);
    Assert.assertTrue(joinNode.getSources().get(1) instanceof StructuredDataSourceNode);

  }

  @Test
  public void testSimpleAggregateLogicalPlan() throws Exception {
    String simpleQuery = "SELECT col0, sum(col3), count(col3) FROM test1 window TUMBLING ( size 2 "
                         + "second) "
                         + "WHERE col0 > 100 GROUP BY col0;";

    PlanNode logicalPlan = buildLogicalPlan(simpleQuery);

    Assert.assertTrue(logicalPlan.getSources().get(0) instanceof AggregateNode);
    AggregateNode aggregateNode = (AggregateNode) logicalPlan.getSources().get(0);
    Assert.assertTrue(aggregateNode.getFunctionList().size() == 2);
    Assert.assertTrue(aggregateNode.getFunctionList().get(0).getName().getSuffix()
                          .equalsIgnoreCase("sum"));
    Assert.assertTrue(aggregateNode.getWindowExpression().getKqlWindowExpression().toString().equalsIgnoreCase(" TUMBLING ( SIZE 2 SECOND ) "));
    Assert.assertTrue(aggregateNode.getGroupByExpressions().size() == 1);
    Assert.assertTrue(aggregateNode.getGroupByExpressions().get(0).toString().equalsIgnoreCase("TEST1.COL0"));
    Assert.assertTrue(aggregateNode.getRequiredColumnList().size() == 2);
    Assert.assertTrue(aggregateNode.getSchema().fields().get(1).schema() == Schema.FLOAT64_SCHEMA);
    Assert.assertTrue(aggregateNode.getSchema().fields().get(2).schema() == Schema.INT32_SCHEMA);
    Assert.assertTrue(logicalPlan.getSources().get(0).getSchema().fields().size() == 3);

  }

  @Test
  public void testComplexAggregateLogicalPlan() throws Exception {
    String simpleQuery = "SELECT col0, sum(floor(col3)*100)/count(col3) FROM test1 window "
                         + "HOPPING ( size 2 second, advance by 1 second) "
                         + "WHERE col0 > 100 GROUP BY col0;";

    PlanNode logicalPlan = buildLogicalPlan(simpleQuery);

    Assert.assertTrue(logicalPlan.getSources().get(0) instanceof AggregateNode);
    AggregateNode aggregateNode = (AggregateNode) logicalPlan.getSources().get(0);
    Assert.assertTrue(aggregateNode.getFunctionList().size() == 2);
    Assert.assertTrue(aggregateNode.getFunctionList().get(0).getName().getSuffix()
                          .equalsIgnoreCase("sum"));
    Assert.assertTrue(aggregateNode.getWindowExpression().getKqlWindowExpression().toString().equalsIgnoreCase(" HOPPING ( SIZE 2 SECOND , ADVANCE BY 1 SECOND ) "));
    Assert.assertTrue(aggregateNode.getGroupByExpressions().size() == 1);
    Assert.assertTrue(aggregateNode.getGroupByExpressions().get(0).toString().equalsIgnoreCase("TEST1.COL0"));
    Assert.assertTrue(aggregateNode.getRequiredColumnList().size() == 2);
    Assert.assertTrue(aggregateNode.getSchema().fields().get(1).schema() == Schema.FLOAT64_SCHEMA);
    Assert.assertTrue(logicalPlan.getSources().get(0).getSchema().fields().size() == 2);

  }
}
