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
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.function.InternalFunctionRegistry;
import io.confluent.ksql.metastore.KsqlTable;
import io.confluent.ksql.metastore.KsqlTopic;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.parser.tree.DereferenceExpression;
import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.parser.tree.QualifiedName;
import io.confluent.ksql.parser.tree.QualifiedNameReference;
import io.confluent.ksql.planner.plan.FilterNode;
import io.confluent.ksql.planner.plan.PlanNode;
import io.confluent.ksql.planner.plan.ProjectNode;
import io.confluent.ksql.serde.KsqlTopicSerDe;
import io.confluent.ksql.serde.json.KsqlJsonTopicSerDe;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.MetaStoreFixture;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Joined;
import org.apache.kafka.streams.kstream.KGroupedTable;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Predicate;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.capture;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.mock;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertThat;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertEquals;

public class SchemaKTableTest {

  private SchemaKTable initialSchemaKTable;
  private final MetaStore metaStore = MetaStoreFixture.getNewMetaStore(new InternalFunctionRegistry());
  private final LogicalPlanBuilder planBuilder = new LogicalPlanBuilder(metaStore);
  private KTable kTable;
  private KsqlTable ksqlTable;
  private KTable secondKTable;
  private KsqlTable secondKsqlTable;
  private InternalFunctionRegistry functionRegistry;
  private KTable mockKTable;
  private SchemaKTable firstSchemaKTable;
  private SchemaKTable secondSchemaKTable;
  private Schema joinSchema;


  @Before
  public void init() {
    functionRegistry = new InternalFunctionRegistry();
    ksqlTable = (KsqlTable) metaStore.getSource("TEST2");
    StreamsBuilder builder = new StreamsBuilder();
    kTable = builder.table(ksqlTable.getKsqlTopic().getKafkaTopicName(),
                           Consumed.with(Serdes.String(),
                                         getRowSerde(ksqlTable.getKsqlTopic(),
                                                     ksqlTable.getSchema())));

    secondKsqlTable = (KsqlTable) metaStore.getSource("TEST3");
    secondKTable = builder.table(secondKsqlTable.getKsqlTopic().getKafkaTopicName(),
                                 Consumed.with(Serdes.String(),
                                               getRowSerde(secondKsqlTable.getKsqlTopic(),
                                                           secondKsqlTable.getSchema())));

    mockKTable = EasyMock.niceMock(KTable.class);
    firstSchemaKTable = new SchemaKTable(ksqlTable.getSchema(), mockKTable,
                                         ksqlTable.getKeyField(), new ArrayList<>(),
                                         false, SchemaKStream.Type.SOURCE, functionRegistry,
                                         new MockSchemaRegistryClient());

    secondSchemaKTable = new SchemaKTable(secondKsqlTable.getSchema(), secondKTable,
                                          secondKsqlTable.getKeyField(), new ArrayList<>(), false,
                                          SchemaKStream.Type.SOURCE, functionRegistry,
                                          new MockSchemaRegistryClient());


    joinSchema = getJoinSchema(ksqlTable.getSchema(), secondKsqlTable.getSchema());


  }

  private Serde<GenericRow> getRowSerde(KsqlTopic topic, Schema schema) {
    return topic.getKsqlTopicSerDe().getGenericRowSerde(schema,
                                                        new KsqlConfig(Collections.emptyMap()),
                                                        false, new MockSchemaRegistryClient());
  }


  @Test
  public void testSelectSchemaKStream() throws Exception {
    String selectQuery = "SELECT col0, col2, col3 FROM test2 WHERE col0 > 100;";
    PlanNode logicalPlan = planBuilder.buildLogicalPlan(selectQuery);
    ProjectNode projectNode = (ProjectNode) logicalPlan.getSources().get(0);

    initialSchemaKTable = new SchemaKTable(logicalPlan.getTheSourceNode().getSchema(),
                                           kTable,
                                           ksqlTable.getKeyField(), new ArrayList<>(),
                                           false,
                                           SchemaKStream.Type.SOURCE, functionRegistry, new MockSchemaRegistryClient());
    SchemaKTable projectedSchemaKStream = initialSchemaKTable
        .select(projectNode.getProjectNameExpressionPairList());
    Assert.assertTrue(projectedSchemaKStream.getSchema().fields().size() == 3);
    Assert.assertTrue(projectedSchemaKStream.getSchema().field("COL0") ==
                      projectedSchemaKStream.getSchema().fields().get(0));
    Assert.assertTrue(projectedSchemaKStream.getSchema().field("COL2") ==
                      projectedSchemaKStream.getSchema().fields().get(1));
    Assert.assertTrue(projectedSchemaKStream.getSchema().field("COL3") ==
                      projectedSchemaKStream.getSchema().fields().get(2));

    Assert.assertTrue(projectedSchemaKStream.getSchema()
                          .field("COL0").schema().type() == Schema.Type.INT64);
    Assert.assertTrue(projectedSchemaKStream.getSchema()
                          .field("COL2").schema().type() == Schema.Type.STRING);
    Assert.assertTrue(projectedSchemaKStream.getSchema()
                          .field("COL3").schema().type() == Schema.Type.FLOAT64);

    Assert.assertTrue(projectedSchemaKStream.getSourceSchemaKStreams().get(0) ==
                      initialSchemaKTable);
  }


  @Test
  public void testSelectWithExpression() throws Exception {
    String selectQuery = "SELECT col0, LEN(UCASE(col2)), col3*3+5 FROM test2 WHERE col0 > 100;";
    PlanNode logicalPlan = planBuilder.buildLogicalPlan(selectQuery);
    ProjectNode projectNode = (ProjectNode) logicalPlan.getSources().get(0);
    initialSchemaKTable = new SchemaKTable(logicalPlan.getTheSourceNode().getSchema(),
                                           kTable,
                                           ksqlTable.getKeyField(),
                                           new ArrayList<>(), false,
                                           SchemaKStream.Type.SOURCE, functionRegistry, new MockSchemaRegistryClient());
    SchemaKTable projectedSchemaKStream = initialSchemaKTable
        .select(projectNode.getProjectNameExpressionPairList());
    Assert.assertTrue(projectedSchemaKStream.getSchema().fields().size() == 3);
    Assert.assertTrue(projectedSchemaKStream.getSchema().field("COL0") ==
                      projectedSchemaKStream.getSchema().fields().get(0));
    Assert.assertTrue(projectedSchemaKStream.getSchema()
                          .field("KSQL_COL_1") ==
                      projectedSchemaKStream.getSchema().fields().get(1));
    Assert.assertTrue(projectedSchemaKStream.getSchema()
                          .field("KSQL_COL_2") ==
                      projectedSchemaKStream.getSchema().fields().get(2));

    Assert.assertTrue(projectedSchemaKStream.getSchema()
                          .field("COL0").schema().type() == Schema.Type.INT64);
    Assert.assertTrue(projectedSchemaKStream.getSchema().fields().get(1).schema() == Schema
        .OPTIONAL_INT32_SCHEMA);
    Assert.assertTrue(projectedSchemaKStream.getSchema().fields().get(2).schema() == Schema
        .OPTIONAL_FLOAT64_SCHEMA);

    Assert.assertTrue(projectedSchemaKStream.getSourceSchemaKStreams().get(0) ==
                      initialSchemaKTable);
  }

  @Test
  public void testFilter() throws Exception {
    String selectQuery = "SELECT col0, col2, col3 FROM test2 WHERE col0 > 100;";
    PlanNode logicalPlan = planBuilder.buildLogicalPlan(selectQuery);
    FilterNode filterNode = (FilterNode) logicalPlan.getSources().get(0).getSources().get(0);

    initialSchemaKTable = new SchemaKTable(logicalPlan.getTheSourceNode().getSchema(),
                                           kTable,
                                           ksqlTable.getKeyField(), new ArrayList<>(),
                                           false,
                                           SchemaKStream.Type.SOURCE, functionRegistry, new MockSchemaRegistryClient());
    SchemaKTable filteredSchemaKStream = initialSchemaKTable.filter(filterNode.getPredicate());

    Assert.assertTrue(filteredSchemaKStream.getSchema().fields().size() == 7);
    Assert.assertTrue(filteredSchemaKStream.getSchema().field("TEST2.COL0") ==
                      filteredSchemaKStream.getSchema().fields().get(2));
    Assert.assertTrue(filteredSchemaKStream.getSchema().field("TEST2.COL1") ==
                      filteredSchemaKStream.getSchema().fields().get(3));
    Assert.assertTrue(filteredSchemaKStream.getSchema().field("TEST2.COL2") ==
                      filteredSchemaKStream.getSchema().fields().get(4));
    Assert.assertTrue(filteredSchemaKStream.getSchema().field("TEST2.COL3") ==
                      filteredSchemaKStream.getSchema().fields().get(5));

    Assert.assertTrue(filteredSchemaKStream.getSchema()
                          .field("TEST2.COL0").schema().type() == Schema.Type.INT64);
    Assert.assertTrue(filteredSchemaKStream.getSchema()
                          .field("TEST2.COL1").schema().type() == Schema.Type.STRING);
    Assert.assertTrue(filteredSchemaKStream.getSchema()
                          .field("TEST2.COL2").schema().type() == Schema.Type.STRING);
    Assert.assertTrue(filteredSchemaKStream.getSchema()
                          .field("TEST2.COL3").schema().type() == Schema.Type.FLOAT64);

    Assert.assertTrue(filteredSchemaKStream.getSourceSchemaKStreams().get(0) ==
                      initialSchemaKTable);
  }

  @Test
  public void testGroupBy() {
    String selectQuery = "SELECT col0, col1, col2 FROM test2;";
    PlanNode logicalPlan = planBuilder.buildLogicalPlan(selectQuery);
    initialSchemaKTable = new SchemaKTable(
        logicalPlan.getTheSourceNode().getSchema(), kTable,
        ksqlTable.getKeyField(), new ArrayList<>(), false,
        SchemaKStream.Type.SOURCE, functionRegistry, new MockSchemaRegistryClient());

    Expression col1Expression = new DereferenceExpression(
        new QualifiedNameReference(QualifiedName.of("TEST2")), "COL1");
    Expression col2Expression = new DereferenceExpression(
        new QualifiedNameReference(QualifiedName.of("TEST2")), "COL2");
    KsqlTopicSerDe ksqlTopicSerDe = new KsqlJsonTopicSerDe();
    Serde<GenericRow> rowSerde = ksqlTopicSerDe.getGenericRowSerde(
        initialSchemaKTable.getSchema(), null, false, null);
    List<Expression> groupByExpressions = Arrays.asList(col2Expression, col1Expression);
    SchemaKGroupedStream groupedSchemaKTable = initialSchemaKTable.groupBy(
        Serdes.String(), rowSerde, groupByExpressions);

    assertThat(groupedSchemaKTable, instanceOf(SchemaKGroupedTable.class));
    assertThat(groupedSchemaKTable.getKeyField().name(), equalTo("TEST2.COL2|+|TEST2.COL1"));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void shouldGroupKeysCorrectly() {
    // set up a mock KTable and KGroupedTable for the test. Set up the KTable to
    // capture the mapper that is passed in to produce new keys
    KTable mockKTable = mock(KTable.class);
    KGroupedTable mockKGroupedTable = mock(KGroupedTable.class);
    Capture<KeyValueMapper> capturedKeySelector = Capture.newInstance();
    expect(mockKTable.filter(anyObject(Predicate.class))).andReturn(mockKTable);
    expect(mockKTable.groupBy(capture(capturedKeySelector), anyObject()))
        .andReturn(mockKGroupedTable);
    replay(mockKTable, mockKGroupedTable);

    // Build our test object from the mocks
    String selectQuery = "SELECT col0, col1, col2 FROM test2;";
    PlanNode logicalPlan = planBuilder.buildLogicalPlan(selectQuery);
    initialSchemaKTable = new SchemaKTable(
        logicalPlan.getTheSourceNode().getSchema(), mockKTable,
        ksqlTable.getKeyField(), new ArrayList<>(), false,
        SchemaKStream.Type.SOURCE, functionRegistry, new MockSchemaRegistryClient());

    // Given a grouping expression comprising COL1 and COL2
    Expression col1Expression = new DereferenceExpression(
        new QualifiedNameReference(QualifiedName.of("TEST2")), "COL1");
    Expression col2Expression = new DereferenceExpression(
        new QualifiedNameReference(QualifiedName.of("TEST2")), "COL2");
    List<Expression> groupByExpressions = Arrays.asList(col2Expression, col1Expression);
    Serde<GenericRow> rowSerde = new KsqlJsonTopicSerDe().getGenericRowSerde(
        initialSchemaKTable.getSchema(), null, false, null);

    // Call groupBy and extract the captured mapper
    initialSchemaKTable.groupBy(Serdes.String(), rowSerde, groupByExpressions);
    verify(mockKTable, mockKGroupedTable);
    KeyValueMapper keySelector = capturedKeySelector.getValue();
    GenericRow value = new GenericRow(Arrays.asList("key", 0, 100, "foo", "bar"));
    KeyValue<String, GenericRow> keyValue =
        (KeyValue<String, GenericRow>) keySelector.apply("key", value);

    // Validate that the captured mapper produces the correct key
    assertThat(keyValue.key, equalTo("bar|+|foo"));
    assertThat(keyValue.value, equalTo(value));
  }


  @SuppressWarnings("unchecked")
  @Test
  public void shouldPerformTableToTableLeftJoin() {
    expect(mockKTable.leftJoin(eq(secondSchemaKTable.getKtable()),
                               anyObject(SchemaKStream.KsqlValueJoiner.class)))
        .andReturn(mockKTable);

    replay(mockKTable);

    final SchemaKStream joinedKStream = firstSchemaKTable
        .leftJoin(secondSchemaKTable, joinSchema, joinSchema.fields().get(0));

    verify(mockKTable);

    assertThat(joinedKStream, instanceOf(SchemaKTable.class));
    assertEquals(SchemaKStream.Type.JOIN, joinedKStream.type);
    assertEquals(joinSchema, joinedKStream.schema);
    assertEquals(joinSchema.fields().get(0), joinedKStream.keyField);
    assertEquals(Arrays.asList(firstSchemaKTable, secondSchemaKTable),
                 joinedKStream.sourceSchemaKStreams);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void shouldPerformTableToTableInnerJoin() {
    expect(mockKTable.join(eq(secondSchemaKTable.getKtable()),
                           anyObject(SchemaKStream.KsqlValueJoiner.class)))
        .andReturn(EasyMock.niceMock(KTable.class));

    replay(mockKTable);

    final SchemaKStream joinedKStream = firstSchemaKTable
        .join(secondSchemaKTable, joinSchema, joinSchema.fields().get(0));

    verify(mockKTable);

    assertThat(joinedKStream, instanceOf(SchemaKTable.class));
    assertEquals(SchemaKStream.Type.JOIN, joinedKStream.type);
    assertEquals(joinSchema, joinedKStream.schema);
    assertEquals(joinSchema.fields().get(0), joinedKStream.keyField);
    assertEquals(Arrays.asList(firstSchemaKTable, secondSchemaKTable),
                 joinedKStream.sourceSchemaKStreams);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void shouldPerformTableToTableOuterJoin() {
    expect(mockKTable.outerJoin(eq(secondSchemaKTable.getKtable()),
                                anyObject(SchemaKStream.KsqlValueJoiner.class)))
        .andReturn(EasyMock.niceMock(KTable.class));

    replay(mockKTable);

    final SchemaKStream joinedKStream = firstSchemaKTable
        .outerJoin(secondSchemaKTable, joinSchema, joinSchema.fields().get(0));

    verify(mockKTable);

    assertThat(joinedKStream, instanceOf(SchemaKTable.class));
    assertEquals(SchemaKStream.Type.JOIN, joinedKStream.type);
    assertEquals(joinSchema, joinedKStream.schema);
    assertEquals(joinSchema.fields().get(0), joinedKStream.keyField);
    assertEquals(Arrays.asList(firstSchemaKTable, secondSchemaKTable),
                 joinedKStream.sourceSchemaKStreams);

  }

  Schema getJoinSchema(Schema leftSchema, Schema rightSchema) {
    SchemaBuilder schemaBuilder = SchemaBuilder.struct();
    String leftAlias = "left";
    String rightAlias = "right";
    for (Field field : leftSchema.fields()) {
      String fieldName = leftAlias + "." + field.name();
      schemaBuilder.field(fieldName, field.schema());
    }

    for (Field field : rightSchema.fields()) {
      String fieldName = rightAlias + "." + field.name();
      schemaBuilder.field(fieldName, field.schema());
    }
    return schemaBuilder.build();
  }

}
