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

package io.confluent.ksql.structured;

import static io.confluent.ksql.testutils.AnalysisTestUtil.analyzeQuery;

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.ksql.analyzer.AggregateAnalysis;
import io.confluent.ksql.analyzer.AggregateAnalyzer;
import io.confluent.ksql.analyzer.Analysis;
import io.confluent.ksql.analyzer.AnalysisContext;
import io.confluent.ksql.function.InternalFunctionRegistry;
import io.confluent.ksql.metastore.KsqlStream;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.parser.KsqlParser;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.parser.tree.QuerySpecification;
import io.confluent.ksql.planner.LogicalPlanner;
import io.confluent.ksql.planner.plan.FilterNode;
import io.confluent.ksql.planner.plan.PlanNode;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.MetaStoreFixture;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

@SuppressWarnings("unchecked")
public class SqlPredicateTest {

  private final MockSchemaRegistryClient schemaRegistryClient = new MockSchemaRegistryClient();
  private SchemaKStream initialSchemaKStream;
  private static final KsqlParser KSQL_PARSER = new KsqlParser();
  private final KsqlConfig ksqlConfig = new KsqlConfig(Collections.emptyMap());

  private MetaStore metaStore;
  private KStream kStream;
  private KsqlStream ksqlStream;
  private InternalFunctionRegistry functionRegistry;

  @Before
  public void init() {
    metaStore = MetaStoreFixture.getNewMetaStore(new InternalFunctionRegistry());
    functionRegistry = new InternalFunctionRegistry();
    ksqlStream = (KsqlStream) metaStore.getSource("TEST1");
    final StreamsBuilder builder = new StreamsBuilder();
    kStream = builder.stream(ksqlStream.getKsqlTopic().getKafkaTopicName(), Consumed.with(Serdes.String(),
                             ksqlStream.getKsqlTopic().getKsqlTopicSerDe().getGenericRowSerde(
                                 ksqlStream.getSchema(), new KsqlConfig(Collections.emptyMap()),
                                                   false, schemaRegistryClient
                                                   )));
  }


  private PlanNode buildLogicalPlan(final String queryStr) {
    final Analysis analysis = analyzeQuery(queryStr, metaStore);
    final AggregateAnalysis aggregateAnalysis = new AggregateAnalysis();
    final AggregateAnalyzer aggregateAnalyzer = new AggregateAnalyzer(aggregateAnalysis,
                                                                analysis, functionRegistry);
    for (final Expression expression: analysis.getSelectExpressions()) {
      aggregateAnalyzer.process(expression, new AnalysisContext(null));
    }
    // Build a logical plan
    return new LogicalPlanner(analysis, aggregateAnalysis, functionRegistry).buildPlan();
  }

  @Test
  public void testFilter() throws Exception {
    final String selectQuery = "SELECT col0, col2, col3 FROM test1 WHERE col0 > 100;";
    final PlanNode logicalPlan = buildLogicalPlan(selectQuery);
    final FilterNode filterNode = (FilterNode) logicalPlan.getSources().get(0).getSources().get(0);

    initialSchemaKStream = new SchemaKStream(logicalPlan.getTheSourceNode().getSchema(),
                                             kStream,
                                             ksqlStream.getKeyField(), new ArrayList<>(),
                                             SchemaKStream.Type.SOURCE, ksqlConfig,
                                             functionRegistry, schemaRegistryClient);
    final SqlPredicate predicate = new SqlPredicate(filterNode.getPredicate(), initialSchemaKStream
        .getSchema(), false, ksqlConfig, functionRegistry);

    Assert.assertTrue(predicate.getFilterExpression()
                          .toString().equalsIgnoreCase("(TEST1.COL0 > 100)"));
    Assert.assertTrue(predicate.getColumnIndexes().length == 1);

  }

  @Test
  public void testFilterBiggerExpression() throws Exception {
    final String selectQuery = "SELECT col0, col2, col3 FROM test1 WHERE col0 > 100 AND LEN(col2) = 5;";
    final PlanNode logicalPlan = buildLogicalPlan(selectQuery);
    final FilterNode filterNode = (FilterNode) logicalPlan.getSources().get(0).getSources().get(0);

    initialSchemaKStream = new SchemaKStream(logicalPlan.getTheSourceNode().getSchema(),
                                             kStream,
                                             ksqlStream.getKeyField(), new ArrayList<>(),
                                             SchemaKStream.Type.SOURCE, ksqlConfig,
                                             functionRegistry, schemaRegistryClient);
    final SqlPredicate predicate = new SqlPredicate(filterNode.getPredicate(), initialSchemaKStream
        .getSchema(), false, ksqlConfig, functionRegistry);

    Assert.assertTrue(predicate
                          .getFilterExpression()
                          .toString()
                          .equalsIgnoreCase("((TEST1.COL0 > 100) AND"
                                            + " (LEN(TEST1.COL2) = 5))"));
    Assert.assertTrue(predicate.getColumnIndexes().length == 3);

  }

  @Test
  @SuppressWarnings("unchecked")
  public void shouldIgnoreNullRows() {
    final String selectQuery = "SELECT col0 FROM test1 WHERE col0 > 100;";
    final List<PreparedStatement> statements = KSQL_PARSER.buildAst(selectQuery, metaStore);
    final QuerySpecification querySpecification = (QuerySpecification)((Query) statements.get(0)
        .getStatement()).getQueryBody();
    final Expression filterExpr = querySpecification.getWhere().get();
    final PlanNode logicalPlan = buildLogicalPlan(selectQuery);

    initialSchemaKStream = new SchemaKStream(logicalPlan.getTheSourceNode().getSchema(),
        kStream,
        ksqlStream.getKeyField(), new ArrayList<>(),
        SchemaKStream.Type.SOURCE, ksqlConfig, functionRegistry, new MockSchemaRegistryClient());
    final SqlPredicate sqlPredicate = new SqlPredicate(filterExpr, initialSchemaKStream.getSchema(), false, ksqlConfig, functionRegistry);
    final boolean result = sqlPredicate.getPredicate().test("key", null);
    Assert.assertFalse(result);
  }

}
