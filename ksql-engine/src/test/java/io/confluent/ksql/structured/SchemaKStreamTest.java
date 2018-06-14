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
import io.confluent.ksql.metastore.KsqlStream;
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
import io.confluent.ksql.util.Pair;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.Joined;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;


import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.niceMock;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.testng.Assert.assertEquals;


public class SchemaKStreamTest {

  private SchemaKStream initialSchemaKStream;

  private final MetaStore metaStore = MetaStoreFixture.getNewMetaStore(new InternalFunctionRegistry());
  private final LogicalPlanBuilder planBuilder = new LogicalPlanBuilder(metaStore);
  private KStream kStream;
  private KsqlStream ksqlStream;
  private KStream secondKStream;
  private KsqlStream secondKsqlStream;
  private KTable kTable;
  private KsqlTable ksqlTable;
  private InternalFunctionRegistry functionRegistry;

  @Before
  public void init() {
    functionRegistry = new InternalFunctionRegistry();
    ksqlStream = (KsqlStream) metaStore.getSource("TEST1");
    StreamsBuilder builder = new StreamsBuilder();
    kStream = builder.stream(ksqlStream.getKsqlTopic().getKafkaTopicName(),
                             Consumed.with(Serdes.String(),
                                           getRowSerde(ksqlStream.getKsqlTopic(),
                                                       ksqlStream.getSchema())));

    secondKsqlStream = (KsqlStream) metaStore.getSource("ORDERS");
    secondKStream = builder.stream(secondKsqlStream.getKsqlTopic().getKafkaTopicName(),
                                   Consumed.with(Serdes.String(),
                                                 getRowSerde(secondKsqlStream.getKsqlTopic(),
                                                             secondKsqlStream.getSchema())));

    ksqlTable = (KsqlTable) metaStore.getSource("TEST2");
    kTable = builder.table(ksqlTable.getKsqlTopic().getKafkaTopicName(),
                           Consumed.with(Serdes.String(),
                                         getRowSerde(ksqlTable.getKsqlTopic(),
                                                     ksqlTable.getSchema())));
  }

  private Serde<GenericRow> getRowSerde(KsqlTopic topic, Schema schema) {
    return topic.getKsqlTopicSerDe().getGenericRowSerde(schema,
                                                        new KsqlConfig(Collections.emptyMap()),
                                                        false, new MockSchemaRegistryClient());
  }

  @Test
  public void testSelectSchemaKStream() {
    String selectQuery = "SELECT col0, col2, col3 FROM test1 WHERE col0 > 100;";
    PlanNode logicalPlan = planBuilder.buildLogicalPlan(selectQuery);
    ProjectNode projectNode = (ProjectNode) logicalPlan.getSources().get(0);
    initialSchemaKStream = new SchemaKStream(logicalPlan.getTheSourceNode().getSchema(), kStream,
                                             ksqlStream.getKeyField(), new ArrayList<>(),
                                             SchemaKStream.Type.SOURCE, functionRegistry, new MockSchemaRegistryClient());

    List<Pair<String, Expression>> projectNameExpressionPairList = projectNode.getProjectNameExpressionPairList();
    SchemaKStream projectedSchemaKStream = initialSchemaKStream.select(projectNameExpressionPairList);
    Assert.assertTrue(projectedSchemaKStream.getSchema().fields().size() == 3);
    Assert.assertTrue(projectedSchemaKStream.getSchema().field("COL0") ==
                      projectedSchemaKStream.getSchema().fields().get(0));
    Assert.assertTrue(projectedSchemaKStream.getSchema().field("COL2") ==
                      projectedSchemaKStream.getSchema().fields().get(1));
    Assert.assertTrue(projectedSchemaKStream.getSchema().field("COL3") ==
                      projectedSchemaKStream.getSchema().fields().get(2));

    Assert.assertTrue(projectedSchemaKStream.getSchema().field("COL0").schema().type() == Schema.Type.INT64);
    Assert.assertTrue(projectedSchemaKStream.getSchema().field("COL2").schema().type() == Schema.Type.STRING);
    Assert.assertTrue(projectedSchemaKStream.getSchema().field("COL3").schema().type() == Schema.Type.FLOAT64);

    Assert.assertTrue(projectedSchemaKStream.getSourceSchemaKStreams().get(0) ==
                      initialSchemaKStream);
  }

  @Test
  public void shouldUpdateKeyIfRenamed() {
    String selectQuery = "SELECT col0 as NEWKEY, col2, col3 FROM test1;";
    PlanNode logicalPlan = planBuilder.buildLogicalPlan(selectQuery);
    ProjectNode projectNode = (ProjectNode) logicalPlan.getSources().get(0);
    initialSchemaKStream = new SchemaKStream(logicalPlan.getTheSourceNode().getSchema(), kStream,
        ksqlStream.getKeyField(), new ArrayList<>(),
        SchemaKStream.Type.SOURCE, functionRegistry, new MockSchemaRegistryClient());

    List<Pair<String, Expression>> projectNameExpressionPairList = projectNode.getProjectNameExpressionPairList();
    SchemaKStream projectedSchemaKStream = initialSchemaKStream.select(projectNameExpressionPairList);
    assertThat(
        projectedSchemaKStream.getKeyField(),
        equalTo(new Field("NEWKEY", 0, Schema.OPTIONAL_INT64_SCHEMA)));
  }

  @Test
  public void shouldUpdateKeyIfMovedToDifferentIndex() {
    String selectQuery = "SELECT col2, col0, col3 FROM test1;";
    PlanNode logicalPlan = planBuilder.buildLogicalPlan(selectQuery);
    ProjectNode projectNode = (ProjectNode) logicalPlan.getSources().get(0);
    initialSchemaKStream = new SchemaKStream(logicalPlan.getTheSourceNode().getSchema(), kStream,
        ksqlStream.getKeyField(), new ArrayList<>(),
        SchemaKStream.Type.SOURCE, functionRegistry, new MockSchemaRegistryClient());

    List<Pair<String, Expression>> projectNameExpressionPairList = projectNode.getProjectNameExpressionPairList();
    SchemaKStream projectedSchemaKStream = initialSchemaKStream.select(projectNameExpressionPairList);
    assertThat(
        projectedSchemaKStream.getKeyField(),
        equalTo(new Field("COL0", 1, Schema.OPTIONAL_INT64_SCHEMA)));
  }

  @Test
  public void shouldDropKeyIfNotSelected() {
    String selectQuery = "SELECT col2, col3 FROM test1;";
    PlanNode logicalPlan = planBuilder.buildLogicalPlan(selectQuery);
    ProjectNode projectNode = (ProjectNode) logicalPlan.getSources().get(0);
    initialSchemaKStream = new SchemaKStream(logicalPlan.getTheSourceNode().getSchema(), kStream,
        ksqlStream.getKeyField(), new ArrayList<>(),
        SchemaKStream.Type.SOURCE, functionRegistry, new MockSchemaRegistryClient());

    List<Pair<String, Expression>> projectNameExpressionPairList = projectNode.getProjectNameExpressionPairList();
    SchemaKStream projectedSchemaKStream = initialSchemaKStream.select(projectNameExpressionPairList);
    assertThat(projectedSchemaKStream.getKeyField(), nullValue());
  }

  @Test
  public void testSelectWithExpression() {
    String selectQuery = "SELECT col0, LEN(UCASE(col2)), col3*3+5 FROM test1 WHERE col0 > 100;";
    PlanNode logicalPlan = planBuilder.buildLogicalPlan(selectQuery);
    ProjectNode projectNode = (ProjectNode) logicalPlan.getSources().get(0);
    initialSchemaKStream = new SchemaKStream(logicalPlan.getTheSourceNode().getSchema(), kStream,
                                             ksqlStream.getKeyField(), new ArrayList<>(),
                                             SchemaKStream.Type.SOURCE, functionRegistry, new MockSchemaRegistryClient());
    SchemaKStream projectedSchemaKStream = initialSchemaKStream.select(projectNode.getProjectNameExpressionPairList());
    Assert.assertTrue(projectedSchemaKStream.getSchema().fields().size() == 3);
    Assert.assertTrue(projectedSchemaKStream.getSchema().field("COL0") ==
                      projectedSchemaKStream.getSchema().fields().get(0));
    Assert.assertTrue(projectedSchemaKStream.getSchema().field("KSQL_COL_1") ==
                      projectedSchemaKStream.getSchema().fields().get(1));
    Assert.assertTrue(projectedSchemaKStream.getSchema().field("KSQL_COL_2") ==
                      projectedSchemaKStream.getSchema().fields().get(2));

    Assert.assertTrue(projectedSchemaKStream.getSchema().field("COL0").schema().type() == Schema.Type.INT64);
    Assert.assertTrue(projectedSchemaKStream.getSchema().fields().get(1).schema().type() == Schema.Type.INT32);
    Assert.assertTrue(projectedSchemaKStream.getSchema().fields().get(2).schema().type() == Schema.Type.FLOAT64);

    Assert.assertTrue(projectedSchemaKStream.getSourceSchemaKStreams().get(0) == initialSchemaKStream);
  }

  @Test
  public void testFilter() {
    String selectQuery = "SELECT col0, col2, col3 FROM test1 WHERE col0 > 100;";
    PlanNode logicalPlan = planBuilder.buildLogicalPlan(selectQuery);
    FilterNode filterNode = (FilterNode) logicalPlan.getSources().get(0).getSources().get(0);

    initialSchemaKStream = new SchemaKStream(logicalPlan.getTheSourceNode().getSchema(), kStream,
                                             ksqlStream.getKeyField(), new ArrayList<>(),
                                             SchemaKStream.Type.SOURCE, functionRegistry, new MockSchemaRegistryClient());
    SchemaKStream filteredSchemaKStream = initialSchemaKStream.filter(filterNode.getPredicate());

    Assert.assertTrue(filteredSchemaKStream.getSchema().fields().size() == 8);
    Assert.assertTrue(filteredSchemaKStream.getSchema().field("TEST1.COL0") ==
                      filteredSchemaKStream.getSchema().fields().get(2));
    Assert.assertTrue(filteredSchemaKStream.getSchema().field("TEST1.COL1") ==
                      filteredSchemaKStream.getSchema().fields().get(3));
    Assert.assertTrue(filteredSchemaKStream.getSchema().field("TEST1.COL2") ==
                      filteredSchemaKStream.getSchema().fields().get(4));
    Assert.assertTrue(filteredSchemaKStream.getSchema().field("TEST1.COL3") ==
                      filteredSchemaKStream.getSchema().fields().get(5));

    Assert.assertTrue(filteredSchemaKStream.getSchema().field("TEST1.COL0").schema().type() == Schema.Type.INT64);
    Assert.assertTrue(filteredSchemaKStream.getSchema().field("TEST1.COL1").schema().type() == Schema.Type.STRING);
    Assert.assertTrue(filteredSchemaKStream.getSchema().field("TEST1.COL2").schema().type() == Schema.Type.STRING);
    Assert.assertTrue(filteredSchemaKStream.getSchema().field("TEST1.COL3").schema().type() == Schema.Type.FLOAT64);

    Assert.assertTrue(filteredSchemaKStream.getSourceSchemaKStreams().get(0) == initialSchemaKStream);
  }

  @Test
  public void testSelectKey() {
    String selectQuery = "SELECT col0, col2, col3 FROM test1 WHERE col0 > 100;";
    PlanNode logicalPlan = planBuilder.buildLogicalPlan(selectQuery);

    initialSchemaKStream = new SchemaKStream(logicalPlan.getTheSourceNode().getSchema(), kStream,
        ksqlStream.getKeyField(), new ArrayList<>(),
        SchemaKStream.Type.SOURCE, functionRegistry, new MockSchemaRegistryClient());
    SchemaKStream rekeyedSchemaKStream = initialSchemaKStream.selectKey(initialSchemaKStream
        .getSchema().fields()
        .get(3), true);
    assertThat(rekeyedSchemaKStream.getKeyField().name().toUpperCase(), equalTo("TEST1.COL1"));
  }

  @Test
  public void testGroupByKey() {
    String selectQuery = "SELECT col0, col1 FROM test1 WHERE col0 > 100;";
    PlanNode logicalPlan = planBuilder.buildLogicalPlan(selectQuery);
    initialSchemaKStream = new SchemaKStream(logicalPlan.getTheSourceNode().getSchema(), kStream,
        ksqlStream.getKeyField(), new ArrayList<>(),
        SchemaKStream.Type.SOURCE, functionRegistry, new MockSchemaRegistryClient());

    Expression keyExpression = new DereferenceExpression(
        new QualifiedNameReference(QualifiedName.of("TEST1")), "COL0");
    KsqlTopicSerDe ksqlTopicSerDe = new KsqlJsonTopicSerDe();
    Serde<GenericRow> rowSerde = ksqlTopicSerDe.getGenericRowSerde(
        initialSchemaKStream.getSchema(), null, false, null);
    List<Expression> groupByExpressions = Arrays.asList(keyExpression);
    SchemaKGroupedStream groupedSchemaKStream = initialSchemaKStream.groupBy(
        Serdes.String(), rowSerde, groupByExpressions);

    Assert.assertEquals(groupedSchemaKStream.getKeyField().name(), "COL0");
  }

  @Test
  public void testGroupByMultipleColumns() {
    String selectQuery = "SELECT col0, col1 FROM test1 WHERE col0 > 100;";
    PlanNode logicalPlan = planBuilder.buildLogicalPlan(selectQuery);
    initialSchemaKStream = new SchemaKStream(logicalPlan.getTheSourceNode().getSchema(), kStream,
        ksqlStream.getKeyField(), new ArrayList<>(),
        SchemaKStream.Type.SOURCE, functionRegistry, new MockSchemaRegistryClient());

    Expression col0Expression = new DereferenceExpression(
        new QualifiedNameReference(QualifiedName.of("TEST1")), "COL0");
    Expression col1Expression = new DereferenceExpression(
        new QualifiedNameReference(QualifiedName.of("TEST1")), "COL1");
    KsqlTopicSerDe ksqlTopicSerDe = new KsqlJsonTopicSerDe();
    Serde<GenericRow> rowSerde = ksqlTopicSerDe.getGenericRowSerde(
        initialSchemaKStream.getSchema(), null, false, null);
    List<Expression> groupByExpressions = Arrays.asList(col1Expression, col0Expression);
    SchemaKGroupedStream groupedSchemaKStream = initialSchemaKStream.groupBy(
        Serdes.String(), rowSerde, groupByExpressions);

    Assert.assertEquals(groupedSchemaKStream.getKeyField().name(), "TEST1.COL1|+|TEST1.COL0");
  }

  @SuppressWarnings("unchecked")
  @Test
  public void shouldPerformStreamToStreamLeftJoin() {
    final SchemaKStream secondSchemaKStream
        = new SchemaKStream(secondKsqlStream.getSchema(), secondKStream,
                            secondKsqlStream.getKeyField(), new ArrayList<>(),
                            SchemaKStream.Type.SOURCE, functionRegistry,
                            new MockSchemaRegistryClient());

    final Serde<GenericRow> leftSerde = getRowSerde(ksqlStream.getKsqlTopic(),
                                                    ksqlStream.getSchema());
    final Serde<GenericRow> rightSerde = getRowSerde(secondKsqlStream.getKsqlTopic(),
                                                     secondKsqlStream.getSchema());

    final JoinWindows joinWindow = JoinWindows.of(10);
    final Schema joinSchema = getJoinSchema(ksqlStream.getSchema(),
                                            secondKsqlStream.getSchema());

    KStream mockKstream = EasyMock.niceMock(KStream.class);
    expect(mockKstream.leftJoin(anyObject(KStream.class),
                                anyObject(SchemaKStream.KsqlValueJoiner.class),
                                eq(joinWindow),
                                anyObject(Joined.class)))
        .andReturn(niceMock(KStream.class));

    replay(mockKstream);
    initialSchemaKStream = new SchemaKStream(ksqlStream.getSchema(), mockKstream,
                                             ksqlStream.getKeyField(), new ArrayList<>(),
                                             SchemaKStream.Type.SOURCE, functionRegistry,
                                             new MockSchemaRegistryClient());

    final SchemaKStream joinedKStream = initialSchemaKStream
        .leftJoin(secondSchemaKStream,
                  joinSchema,
                  joinSchema.fields().get(0),
                  joinWindow,
                  leftSerde,
                  rightSerde);

    verify(mockKstream);
    assertThat(joinedKStream, instanceOf(SchemaKStream.class));
    assertEquals(joinedKStream.type, SchemaKStream.Type.JOIN);
    assertEquals(joinedKStream.schema, joinSchema);
    assertEquals(joinedKStream.keyField, joinSchema.fields().get(0));
    assertEquals(joinedKStream.sourceSchemaKStreams,
                 Arrays.asList(initialSchemaKStream, secondSchemaKStream));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void shouldPerformStreamToStreamInnerJoin() {

    final SchemaKStream secondSchemaKStream
        = new SchemaKStream(secondKsqlStream.getSchema(), secondKStream,
                            secondKsqlStream.getKeyField(), new ArrayList<>(),
                            SchemaKStream.Type.SOURCE, functionRegistry,
                            new MockSchemaRegistryClient());

    final Serde<GenericRow> leftSerde = getRowSerde(ksqlStream.getKsqlTopic(),
                                                    ksqlStream.getSchema());
    final Serde<GenericRow> rightSerde = getRowSerde(secondKsqlStream.getKsqlTopic(),
                                                     secondKsqlStream.getSchema());

    final JoinWindows joinWindow = JoinWindows.of(10);
    final Schema joinSchema = getJoinSchema(ksqlStream.getSchema(),
                                            secondKsqlStream.getSchema());

    final KStream mockKstream = EasyMock.niceMock(KStream.class);
    expect(mockKstream.join(anyObject(KStream.class),
                            anyObject(SchemaKStream.KsqlValueJoiner.class),
                            eq(joinWindow),
                            anyObject(Joined.class)))
        .andReturn(niceMock(KStream.class));

    replay(mockKstream);

    initialSchemaKStream = new SchemaKStream(ksqlStream.getSchema(), mockKstream,
                                             ksqlStream.getKeyField(), new ArrayList<>(),
                                             SchemaKStream.Type.SOURCE, functionRegistry,
                                             new MockSchemaRegistryClient());
   final SchemaKStream joinedKStream = initialSchemaKStream
        .join(secondSchemaKStream,
              joinSchema,
              joinSchema.fields().get(0),
              joinWindow,
              leftSerde,
              rightSerde);

    verify(mockKstream);

    assertThat(joinedKStream, instanceOf(SchemaKStream.class));
    assertEquals(joinedKStream.type, SchemaKStream.Type.JOIN);
    assertEquals(joinedKStream.schema, joinSchema);
    assertEquals(joinedKStream.keyField, joinSchema.fields().get(0));
    assertEquals(joinedKStream.sourceSchemaKStreams,
                 Arrays.asList(initialSchemaKStream, secondSchemaKStream));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void shouldPerformStreamToStreamOuterJoin() {

    final SchemaKStream secondSchemaKStream
        = new SchemaKStream(secondKsqlStream.getSchema(), secondKStream,
                            secondKsqlStream.getKeyField(), new ArrayList<>(),
                            SchemaKStream.Type.SOURCE, functionRegistry,
                            new MockSchemaRegistryClient());

    final Serde<GenericRow> leftSerde = getRowSerde(ksqlStream.getKsqlTopic(),
                                                    ksqlStream.getSchema());
    final Serde<GenericRow> rightSerde = getRowSerde(secondKsqlStream.getKsqlTopic(),
                                                     secondKsqlStream.getSchema());

    final JoinWindows joinWindow = JoinWindows.of(10);
    final Schema joinSchema = getJoinSchema(ksqlStream.getSchema(),
                                            secondKsqlStream.getSchema());

    final KStream mockKstream = EasyMock.niceMock(KStream.class);
    expect(mockKstream.outerJoin(anyObject(KStream.class),
                                 anyObject(SchemaKStream.KsqlValueJoiner.class),
                                 eq(joinWindow),
                                 anyObject(Joined.class)))
        .andReturn(niceMock(KStream.class));

    replay(mockKstream);

    initialSchemaKStream = new SchemaKStream(ksqlStream.getSchema(), mockKstream,
                                             ksqlStream.getKeyField(), new ArrayList<>(),
                                             SchemaKStream.Type.SOURCE, functionRegistry,
                                             new MockSchemaRegistryClient());


    final SchemaKStream joinedKStream = initialSchemaKStream
        .outerJoin(secondSchemaKStream,
                   joinSchema,
                   joinSchema.fields().get(0),
                   joinWindow,
                   leftSerde,
                   rightSerde);

    verify(mockKstream);
    assertThat(joinedKStream, instanceOf(SchemaKStream.class));
    assertEquals(joinedKStream.type, SchemaKStream.Type.JOIN);
    assertEquals(joinedKStream.schema, joinSchema);
    assertEquals(joinedKStream.keyField, joinSchema.fields().get(0));
    assertEquals(joinedKStream.sourceSchemaKStreams,
                 Arrays.asList(initialSchemaKStream, secondSchemaKStream));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void shouldPerformStreamToTableLeftJoin() {

    final KStream mockKstream = EasyMock.niceMock(KStream.class);
    expect(mockKstream.leftJoin(anyObject(KTable.class),
                                anyObject(SchemaKStream.KsqlValueJoiner.class),
                                anyObject(Joined.class)))
        .andReturn(niceMock(KStream.class));

    replay(mockKstream);
    initialSchemaKStream = new SchemaKStream(ksqlStream.getSchema(), mockKstream,
                                             ksqlStream.getKeyField(), new ArrayList<>(),
                                             SchemaKStream.Type.SOURCE, functionRegistry,
                                             new MockSchemaRegistryClient());

    final SchemaKTable schemaKTable
        = new SchemaKTable(ksqlTable.getSchema(), kTable,
                           ksqlTable.getKeyField(), new ArrayList<>(), false,
                           SchemaKStream.Type.SOURCE, functionRegistry,
                           new MockSchemaRegistryClient());

    final Serde<GenericRow> leftSerde = getRowSerde(ksqlStream.getKsqlTopic(),
                                                    ksqlStream.getSchema());

    final Schema joinSchema = getJoinSchema(ksqlStream.getSchema(),
                                            secondKsqlStream.getSchema());

    final SchemaKStream joinedKStream = initialSchemaKStream
        .leftJoin(schemaKTable, joinSchema, joinSchema.fields().get(0), leftSerde);

    verify(mockKstream);

    assertThat(joinedKStream, instanceOf(SchemaKStream.class));
    assertEquals(joinedKStream.type, SchemaKStream.Type.JOIN);
    assertEquals(joinedKStream.schema, joinSchema);
    assertEquals(joinedKStream.keyField, joinSchema.fields().get(0));
    assertEquals(joinedKStream.sourceSchemaKStreams,
                 Arrays.asList(initialSchemaKStream, schemaKTable));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void shouldPerformStreamToTableInnerJoin() {

    final KStream mockKstream = EasyMock.niceMock(KStream.class);
    expect(mockKstream.join(anyObject(KTable.class),
                            anyObject(SchemaKStream.KsqlValueJoiner.class),
                            anyObject(Joined.class)))
        .andReturn(niceMock(KStream.class));

    replay(mockKstream);
    initialSchemaKStream = new SchemaKStream(ksqlStream.getSchema(), mockKstream,
                                             ksqlStream.getKeyField(), new ArrayList<>(),
                                             SchemaKStream.Type.SOURCE, functionRegistry,
                                             new MockSchemaRegistryClient());

    final SchemaKTable schemaKTable
        = new SchemaKTable(ksqlTable.getSchema(), kTable,
                           ksqlTable.getKeyField(), new ArrayList<>(), false,
                           SchemaKStream.Type.SOURCE, functionRegistry,
                           new MockSchemaRegistryClient());

    final Serde<GenericRow> leftSerde = getRowSerde(ksqlStream.getKsqlTopic(),
                                                    ksqlStream.getSchema());

    final Schema joinSchema = getJoinSchema(ksqlStream.getSchema(),
                                            secondKsqlStream.getSchema());

    final SchemaKStream joinedKStream = initialSchemaKStream
        .join(schemaKTable, joinSchema, joinSchema.fields().get(0), leftSerde);

    verify(mockKstream);

    assertThat(joinedKStream, instanceOf(SchemaKStream.class));
    assertEquals(joinedKStream.type, SchemaKStream.Type.JOIN);
    assertEquals(joinedKStream.schema, joinSchema);
    assertEquals(joinedKStream.keyField, joinSchema.fields().get(0));
    assertEquals(joinedKStream.sourceSchemaKStreams,
                 Arrays.asList(initialSchemaKStream, schemaKTable));
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
