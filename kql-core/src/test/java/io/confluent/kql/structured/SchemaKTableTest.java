package io.confluent.kql.structured;


import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import io.confluent.kql.analyzer.AggregateAnalysis;
import io.confluent.kql.analyzer.AggregateAnalyzer;
import io.confluent.kql.analyzer.Analysis;
import io.confluent.kql.analyzer.AnalysisContext;
import io.confluent.kql.analyzer.Analyzer;
import io.confluent.kql.metastore.KQLTable;
import io.confluent.kql.metastore.MetaStore;
import io.confluent.kql.parser.KQLParser;
import io.confluent.kql.parser.rewrite.SqlFormatterQueryRewrite;
import io.confluent.kql.parser.tree.Expression;
import io.confluent.kql.parser.tree.Statement;
import io.confluent.kql.planner.LogicalPlanner;
import io.confluent.kql.planner.plan.FilterNode;
import io.confluent.kql.planner.plan.PlanNode;
import io.confluent.kql.planner.plan.ProjectNode;
import io.confluent.kql.util.KQLTestUtil;
import io.confluent.kql.util.SerDeUtil;

public class SchemaKTableTest {

  private SchemaKTable initialSchemaKTable;
  private static final KQLParser kqlParser = new KQLParser();

  MetaStore metaStore;
  KTable kTable;
  KQLTable kqlTable;

  @Before
  public void init() {
    metaStore = KQLTestUtil.getNewMetaStore();
    kqlTable = (KQLTable) metaStore.getSource("TEST2");
    KStreamBuilder builder = new KStreamBuilder();
    kTable = builder
            .table(Serdes.String(), SerDeUtil.getRowSerDe(kqlTable.getKqlTopic().getKqlTopicSerDe
                       (), null), kqlTable.getKqlTopic().getKafkaTopicName(),
                   kqlTable.getStateStoreName());

  }

  private Analysis analyze(String queryStr) {
    List<Statement> statements = kqlParser.buildAST(queryStr, metaStore);
    System.out.println(SqlFormatterQueryRewrite.formatSql(statements.get(0)).replace("\n", " "));
    // Analyze the query to resolve the references and extract oeprations
    Analysis analysis = new Analysis();
    Analyzer analyzer = new Analyzer(analysis, metaStore);
    analyzer.process(statements.get(0), new AnalysisContext(null, null));
    return analysis;
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
  public void testSelectSchemaKStream() throws Exception {
    String selectQuery = "SELECT col0, col2, col3 FROM test1 WHERE col0 > 100;";
    PlanNode logicalPlan = buildLogicalPlan(selectQuery);
    ProjectNode projectNode = (ProjectNode) logicalPlan.getSources().get(0);
    initialSchemaKTable = new SchemaKTable(logicalPlan.getTheSourceNode().getSchema(), kTable,
                                             kqlTable.getKeyField(), new ArrayList<>(), false);
    SchemaKTable projectedSchemaKStream = initialSchemaKTable.select(projectNode.getProjectExpressions
        ());
    Assert.assertTrue(projectedSchemaKStream.getSchema().fields().size() == 3);
    Assert.assertTrue(projectedSchemaKStream.getSchema().field("TEST1.COL0") ==
                      projectedSchemaKStream.getSchema().fields().get(0));
    Assert.assertTrue(projectedSchemaKStream.getSchema().field("TEST1.COL2") ==
                      projectedSchemaKStream.getSchema().fields().get(1));
    Assert.assertTrue(projectedSchemaKStream.getSchema().field("TEST1.COL3") ==
                      projectedSchemaKStream.getSchema().fields().get(2));

    Assert.assertTrue(projectedSchemaKStream.getSchema().field("TEST1.COL0").schema() == Schema.INT64_SCHEMA);
    Assert.assertTrue(projectedSchemaKStream.getSchema().field("TEST1.COL2").schema() == Schema.STRING_SCHEMA);
    Assert.assertTrue(projectedSchemaKStream.getSchema().field("TEST1.COL3").schema() == Schema.FLOAT64_SCHEMA);

    Assert.assertTrue(projectedSchemaKStream.getSourceSchemaKStreams().get(0) ==
                      initialSchemaKTable);
  }


  @Test
  public void testSelectWithExpression() throws Exception {
    String selectQuery = "SELECT col0, LEN(UCASE(col2)), col3*3+5 FROM test1 WHERE col0 > 100;";
    PlanNode logicalPlan = buildLogicalPlan(selectQuery);
    ProjectNode projectNode = (ProjectNode) logicalPlan.getSources().get(0);
    initialSchemaKTable = new SchemaKTable(logicalPlan.getTheSourceNode().getSchema(), kTable,
                                           kqlTable.getKeyField(), new ArrayList<>(), false);
    SchemaKTable projectedSchemaKStream = initialSchemaKTable.select(projectNode.getProjectExpressions());
    Assert.assertTrue(projectedSchemaKStream.getSchema().fields().size() == 3);
    Assert.assertTrue(projectedSchemaKStream.getSchema().field("TEST1.COL0") ==
                      projectedSchemaKStream.getSchema().fields().get(0));
    Assert.assertTrue(projectedSchemaKStream.getSchema().field("LEN(UCASE(TEST1.COL2))") ==
                      projectedSchemaKStream.getSchema().fields().get(1));
    Assert.assertTrue(projectedSchemaKStream.getSchema().field("((TEST1.COL3 * 3) + 5)") ==
                      projectedSchemaKStream.getSchema().fields().get(2));

    Assert.assertTrue(projectedSchemaKStream.getSchema().field("TEST1.COL0").schema() == Schema.INT64_SCHEMA);
    Assert.assertTrue(projectedSchemaKStream.getSchema().fields().get(1).schema() == Schema
        .INT32_SCHEMA);
    Assert.assertTrue(projectedSchemaKStream.getSchema().fields().get(2).schema() == Schema
        .FLOAT64_SCHEMA);

    Assert.assertTrue(projectedSchemaKStream.getSourceSchemaKStreams().get(0) ==
                      initialSchemaKTable);
  }

  @Test
  public void testFilter() throws Exception {
    String selectQuery = "SELECT col0, col2, col3 FROM test1 WHERE col0 > 100;";
    PlanNode logicalPlan = buildLogicalPlan(selectQuery);
    FilterNode filterNode = (FilterNode) logicalPlan.getSources().get(0).getSources().get(0);

    initialSchemaKTable = new SchemaKTable(logicalPlan.getTheSourceNode().getSchema(), kTable,
                                           kqlTable.getKeyField(), new ArrayList<>(), false);
    SchemaKTable filteredSchemaKStream = initialSchemaKTable.filter(filterNode.getPredicate());

    Assert.assertTrue(filteredSchemaKStream.getSchema().fields().size() == 4);
    Assert.assertTrue(filteredSchemaKStream.getSchema().field("TEST1.COL0") ==
                      filteredSchemaKStream.getSchema().fields().get(0));
    Assert.assertTrue(filteredSchemaKStream.getSchema().field("TEST1.COL1") ==
                      filteredSchemaKStream.getSchema().fields().get(1));
    Assert.assertTrue(filteredSchemaKStream.getSchema().field("TEST1.COL2") ==
                      filteredSchemaKStream.getSchema().fields().get(2));
    Assert.assertTrue(filteredSchemaKStream.getSchema().field("TEST1.COL3") ==
                      filteredSchemaKStream.getSchema().fields().get(3));

    Assert.assertTrue(filteredSchemaKStream.getSchema().field("TEST1.COL0").schema() == Schema.INT64_SCHEMA);
    Assert.assertTrue(filteredSchemaKStream.getSchema().field("TEST1.COL1").schema() == Schema.STRING_SCHEMA);
    Assert.assertTrue(filteredSchemaKStream.getSchema().field("TEST1.COL2").schema() == Schema.STRING_SCHEMA);
    Assert.assertTrue(filteredSchemaKStream.getSchema().field("TEST1.COL3").schema() == Schema.FLOAT64_SCHEMA);

    Assert.assertTrue(filteredSchemaKStream.getSourceSchemaKStreams().get(0) ==
                      initialSchemaKTable);
  }

}
