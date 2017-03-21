package io.confluent.kql.structured;


import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
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
import io.confluent.kql.metastore.KQLStream;
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

public class SQLPredicateTest {
  private SchemaKStream initialSchemaKStream;
  private static final KQLParser kqlParser = new KQLParser();

  MetaStore metaStore;
  KStream kStream;
  KQLStream kqlStream;

  @Before
  public void init() {
    metaStore = KQLTestUtil.getNewMetaStore();
    kqlStream = (KQLStream) metaStore.getSource("test1");
    KStreamBuilder builder = new KStreamBuilder();
    kStream = builder.stream(Serdes.String(), SerDeUtil.getRowSerDe(kqlStream.getKqlTopic().getKqlTopicSerDe()),
                             kqlStream.getKqlTopic().getKafkaTopicName());
//    initialSchemaKStream = new SchemaKStream(kqlStream.getSchema(), kStream,
//                                             kqlStream.getKeyField(), new ArrayList<>());
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
  public void testFilter() throws Exception {
    String selectQuery = "SELECT col0, col2, col3 FROM test1 WHERE col0 > 100;";
    PlanNode logicalPlan = buildLogicalPlan(selectQuery);
    FilterNode filterNode = (FilterNode) logicalPlan.getSources().get(0).getSources().get(0);

    initialSchemaKStream = new SchemaKStream(logicalPlan.getTheSourceNode().getSchema(), kStream,
                                             kqlStream.getKeyField(), new ArrayList<>());
    SQLPredicate predicate = new SQLPredicate(filterNode.getPredicate(), initialSchemaKStream.getSchema());

    Assert.assertTrue(predicate.filterExpression.toString().equalsIgnoreCase("(TEST1.COL0 > 100)"));
    Assert.assertTrue(predicate.columnIndexes.length == 1);

  }

  @Test
  public void testFilterBiggerExpression() throws Exception {
    String selectQuery = "SELECT col0, col2, col3 FROM test1 WHERE col0 > 100 AND LEN(col2) = 5;";
    PlanNode logicalPlan = buildLogicalPlan(selectQuery);
    FilterNode filterNode = (FilterNode) logicalPlan.getSources().get(0).getSources().get(0);

    initialSchemaKStream = new SchemaKStream(logicalPlan.getTheSourceNode().getSchema(), kStream,
                                             kqlStream.getKeyField(), new ArrayList<>());
    SQLPredicate predicate = new SQLPredicate(filterNode.getPredicate(), initialSchemaKStream.getSchema());

    Assert.assertTrue(predicate.filterExpression.toString().equalsIgnoreCase("((TEST1.COL0 > 100) AND (LEN(TEST1.COL2) = 5))"));
    Assert.assertTrue(predicate.columnIndexes.length == 3);

  }

}
