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

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
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
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.MetaStoreFixture;
import io.confluent.ksql.util.SerDeUtil;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.Consumed;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class SqlPredicateTest {
  private SchemaKStream initialSchemaKStream;
  private static final KsqlParser KSQL_PARSER = new KsqlParser();

  MetaStore metaStore;
  KStream kStream;
  KsqlStream ksqlStream;
  FunctionRegistry functionRegistry;

  @Before
  public void init() {
    metaStore = MetaStoreFixture.getNewMetaStore();
    functionRegistry = new FunctionRegistry();
    ksqlStream = (KsqlStream) metaStore.getSource("TEST1");
    StreamsBuilder builder = new StreamsBuilder();
    kStream = builder.stream(ksqlStream.getKsqlTopic().getKafkaTopicName(), Consumed.with(Serdes.String(),
                             ksqlStream.getKsqlTopic().getKsqlTopicSerDe().getGenericRowSerde(
                                                   null, new KsqlConfig(Collections.emptyMap()),
                                                   false, new MockSchemaRegistryClient()
                                                   )));
  }


  private PlanNode buildLogicalPlan(String queryStr) {
    List<Statement> statements = KSQL_PARSER.buildAst(queryStr, metaStore);
    // Analyze the query to resolve the references and extract oeprations
    Analysis analysis = new Analysis();
    Analyzer analyzer = new Analyzer("sqlExpression", analysis, metaStore);
    analyzer.process(statements.get(0), new AnalysisContext(null));
    AggregateAnalysis aggregateAnalysis = new AggregateAnalysis();
    AggregateAnalyzer aggregateAnalyzer = new AggregateAnalyzer(aggregateAnalysis,
                                                                analysis, functionRegistry);
    for (Expression expression: analysis.getSelectExpressions()) {
      aggregateAnalyzer.process(expression, new AnalysisContext(null));
    }
    // Build a logical plan
    PlanNode logicalPlan = new LogicalPlanner(analysis, aggregateAnalysis, functionRegistry).buildPlan();
    return logicalPlan;
  }

  @Test
  public void testFilter() throws Exception {
    String selectQuery = "SELECT col0, col2, col3 FROM test1 WHERE col0 > 100;";
    PlanNode logicalPlan = buildLogicalPlan(selectQuery);
    FilterNode filterNode = (FilterNode) logicalPlan.getSources().get(0).getSources().get(0);

    initialSchemaKStream = new SchemaKStream(logicalPlan.getTheSourceNode().getSchema(),
                                             kStream,
                                             ksqlStream.getKeyField(), new ArrayList<>(),
                                             SchemaKStream.Type.SOURCE, functionRegistry, new MockSchemaRegistryClient());
    SqlPredicate predicate = new SqlPredicate(filterNode.getPredicate(), initialSchemaKStream
        .getSchema(), false, functionRegistry);

    Assert.assertTrue(predicate.getFilterExpression()
                          .toString().equalsIgnoreCase("(TEST1.COL0 > 100)"));
    Assert.assertTrue(predicate.getColumnIndexes().length == 1);

  }

  @Test
  public void testFilterBiggerExpression() throws Exception {
    String selectQuery = "SELECT col0, col2, col3 FROM test1 WHERE col0 > 100 AND LEN(col2) = 5;";
    PlanNode logicalPlan = buildLogicalPlan(selectQuery);
    FilterNode filterNode = (FilterNode) logicalPlan.getSources().get(0).getSources().get(0);

    initialSchemaKStream = new SchemaKStream(logicalPlan.getTheSourceNode().getSchema(),
                                             kStream,
                                             ksqlStream.getKeyField(), new ArrayList<>(),
                                             SchemaKStream.Type.SOURCE, functionRegistry, new MockSchemaRegistryClient());
    SqlPredicate predicate = new SqlPredicate(filterNode.getPredicate(), initialSchemaKStream
        .getSchema(), false, functionRegistry);

    Assert.assertTrue(predicate
                          .getFilterExpression()
                          .toString()
                          .equalsIgnoreCase("((TEST1.COL0 > 100) AND"
                                            + " (LEN(TEST1.COL2) = 5))"));
    Assert.assertTrue(predicate.getColumnIndexes().length == 3);

  }

}
