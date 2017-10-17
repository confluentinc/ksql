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

package io.confluent.ksql.structured;

import io.confluent.ksql.analyzer.AggregateAnalysis;
import io.confluent.ksql.analyzer.AggregateAnalyzer;
import io.confluent.ksql.analyzer.Analysis;
import io.confluent.ksql.analyzer.AnalysisContext;
import io.confluent.ksql.analyzer.Analyzer;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.metastore.KsqlStream;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.parser.KsqlParser;
import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.planner.LogicalPlanner;
import io.confluent.ksql.planner.plan.FilterNode;
import io.confluent.ksql.planner.plan.PlanNode;
import io.confluent.ksql.planner.plan.ProjectNode;
import io.confluent.ksql.util.MetaStoreFixture;
import io.confluent.ksql.util.Pair;
import io.confluent.ksql.util.SerDeUtil;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.streams.Consumed;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class SchemaKStreamTest {

  private SchemaKStream initialSchemaKStream;
  private static final KsqlParser KSQL_PARSER = new KsqlParser();

  private MetaStore metaStore;
  private KStream kStream;
  private KsqlStream ksqlStream;
  private FunctionRegistry functionRegistry;


  @Before
  public void init() {
    metaStore = MetaStoreFixture.getNewMetaStore();
    functionRegistry = new FunctionRegistry();
    ksqlStream = (KsqlStream) metaStore.getSource("TEST1");
    StreamsBuilder builder = new StreamsBuilder();
    kStream = builder.stream(ksqlStream.getKsqlTopic().getKafkaTopicName(),
        Consumed.with(Serdes.String(), SerDeUtil.getRowSerDe(ksqlStream.getKsqlTopic()
            .getKsqlTopicSerDe(), null)));
  }

  private PlanNode buildLogicalPlan(String queryStr) {
    List<Statement> statements = KSQL_PARSER.buildAst(queryStr, metaStore);
    // Analyze the query to resolve the references and extract oeprations
    Analysis analysis = new Analysis();
    Analyzer analyzer = new Analyzer(analysis, metaStore);
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
  public void testSelectSchemaKStream() throws Exception {
    String selectQuery = "SELECT col0, col2, col3 FROM test1 WHERE col0 > 100;";
    PlanNode logicalPlan = buildLogicalPlan(selectQuery);
    ProjectNode projectNode = (ProjectNode) logicalPlan.getSources().get(0);
    initialSchemaKStream = new SchemaKStream(logicalPlan.getTheSourceNode().getSchema(), kStream,
                                             ksqlStream.getKeyField(), new ArrayList<>(),
                                             SchemaKStream.Type.SOURCE, functionRegistry);

    List<Pair<String, Expression>> projectNameExpressionPairList = projectNode.getProjectNameExpressionPairList();
    SchemaKStream projectedSchemaKStream = initialSchemaKStream.select(projectNameExpressionPairList);
    Assert.assertTrue(projectedSchemaKStream.getSchema().fields().size() == 3);
    Assert.assertTrue(projectedSchemaKStream.getSchema().field("COL0") ==
                      projectedSchemaKStream.getSchema().fields().get(0));
    Assert.assertTrue(projectedSchemaKStream.getSchema().field("COL2") ==
                      projectedSchemaKStream.getSchema().fields().get(1));
    Assert.assertTrue(projectedSchemaKStream.getSchema().field("COL3") ==
                      projectedSchemaKStream.getSchema().fields().get(2));

    Assert.assertTrue(projectedSchemaKStream.getSchema().field("COL0").schema() == Schema.INT64_SCHEMA);
    Assert.assertTrue(projectedSchemaKStream.getSchema().field("COL2").schema() == Schema.STRING_SCHEMA);
    Assert.assertTrue(projectedSchemaKStream.getSchema().field("COL3").schema() == Schema.FLOAT64_SCHEMA);

    Assert.assertTrue(projectedSchemaKStream.getSourceSchemaKStreams().get(0) ==
                      initialSchemaKStream);
  }


  @Test
  public void testSelectWithExpression() throws Exception {
    String selectQuery = "SELECT col0, LEN(UCASE(col2)), col3*3+5 FROM test1 WHERE col0 > 100;";
    PlanNode logicalPlan = buildLogicalPlan(selectQuery);
    ProjectNode projectNode = (ProjectNode) logicalPlan.getSources().get(0);
    initialSchemaKStream = new SchemaKStream(logicalPlan.getTheSourceNode().getSchema(), kStream,
                                             ksqlStream.getKeyField(), new ArrayList<>(),
                                             SchemaKStream.Type.SOURCE, functionRegistry);
    SchemaKStream projectedSchemaKStream = initialSchemaKStream.select(projectNode.getProjectNameExpressionPairList());
    Assert.assertTrue(projectedSchemaKStream.getSchema().fields().size() == 3);
    Assert.assertTrue(projectedSchemaKStream.getSchema().field("COL0") ==
                      projectedSchemaKStream.getSchema().fields().get(0));
    Assert.assertTrue(projectedSchemaKStream.getSchema().field("KSQL_COL_1") ==
                      projectedSchemaKStream.getSchema().fields().get(1));
    Assert.assertTrue(projectedSchemaKStream.getSchema().field("KSQL_COL_2") ==
                      projectedSchemaKStream.getSchema().fields().get(2));

    Assert.assertTrue(projectedSchemaKStream.getSchema().field("COL0").schema() == Schema.INT64_SCHEMA);
    Assert.assertTrue(projectedSchemaKStream.getSchema().fields().get(1).schema() == Schema
        .INT32_SCHEMA);
    Assert.assertTrue(projectedSchemaKStream.getSchema().fields().get(2).schema() == Schema
        .FLOAT64_SCHEMA);

    Assert.assertTrue(projectedSchemaKStream.getSourceSchemaKStreams().get(0) ==
                      initialSchemaKStream);
  }

  @Test
  public void testFilter() throws Exception {
    String selectQuery = "SELECT col0, col2, col3 FROM test1 WHERE col0 > 100;";
    PlanNode logicalPlan = buildLogicalPlan(selectQuery);
    FilterNode filterNode = (FilterNode) logicalPlan.getSources().get(0).getSources().get(0);

    initialSchemaKStream = new SchemaKStream(logicalPlan.getTheSourceNode().getSchema(), kStream,
                                             ksqlStream.getKeyField(), new ArrayList<>(),
                                             SchemaKStream.Type.SOURCE, functionRegistry);
    SchemaKStream filteredSchemaKStream = initialSchemaKStream.filter(filterNode.getPredicate());

    Assert.assertTrue(filteredSchemaKStream.getSchema().fields().size() == 6);
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
                      initialSchemaKStream);
  }

  @Test
  public void testSelectKey() throws Exception {
    String selectQuery = "SELECT col0, col2, col3 FROM test1 WHERE col0 > 100;";
    PlanNode logicalPlan = buildLogicalPlan(selectQuery);

    initialSchemaKStream = new SchemaKStream(logicalPlan.getTheSourceNode().getSchema(), kStream,
                                             ksqlStream.getKeyField(), new ArrayList<>(),
                                             SchemaKStream.Type.SOURCE, functionRegistry);
    SchemaKStream rekeyedSchemaKStream = initialSchemaKStream.selectKey(initialSchemaKStream
                                                                            .getSchema().fields()
                                                                            .get(1));
    Assert.assertTrue(rekeyedSchemaKStream.getKeyField().name().equalsIgnoreCase("TEST1.COL1"));

  }

}
