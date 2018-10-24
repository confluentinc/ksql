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
import static org.junit.Assert.assertEquals;

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
import io.confluent.ksql.schema.registry.MockSchemaRegistryClientFactory;
import io.confluent.ksql.serde.KsqlTopicSerDe;
import io.confluent.ksql.serde.json.KsqlJsonTopicSerDe;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.MetaStoreFixture;
import io.confluent.ksql.util.SelectExpression;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
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
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;


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
  private KStream mockKStream;
  private SchemaKStream firstSchemaKStream;
  private SchemaKStream secondSchemaKStream;
  private SchemaKTable schemaKTable;
  private Serde<GenericRow> leftSerde;
  private Serde<GenericRow> rightSerde;
  private Schema joinSchema;


  @Before
  public void init() {
    functionRegistry = new InternalFunctionRegistry();
    ksqlStream = (KsqlStream) metaStore.getSource("TEST1");
    final StreamsBuilder builder = new StreamsBuilder();
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


    mockKStream = niceMock(KStream.class);

    firstSchemaKStream =  new SchemaKStream(ksqlStream.getSchema(), mockKStream,
                                            ksqlStream.getKeyField(), new ArrayList<>(),
                                            SchemaKStream.Type.SOURCE, functionRegistry,
                                            new MockSchemaRegistryClient());


    secondSchemaKStream  = new SchemaKStream(secondKsqlStream.getSchema(), secondKStream,
                                             secondKsqlStream.getKeyField(), new ArrayList<>(),
                                             SchemaKStream.Type.SOURCE, functionRegistry,
                                             new MockSchemaRegistryClient());


    leftSerde = getRowSerde(ksqlStream.getKsqlTopic(),
                                                    ksqlStream.getSchema());
    rightSerde = getRowSerde(secondKsqlStream.getKsqlTopic(),
                                                     secondKsqlStream.getSchema());


    schemaKTable = new SchemaKTable(ksqlTable.getSchema(), kTable,
                                    ksqlTable.getKeyField(), new ArrayList<>(), false,
                                    SchemaKStream.Type.SOURCE, functionRegistry,
                                    new MockSchemaRegistryClient());

    joinSchema = getJoinSchema(ksqlStream.getSchema(), secondKsqlStream.getSchema());
  }

  private Serde<GenericRow> getRowSerde(final KsqlTopic topic, final Schema schema) {
    return topic.getKsqlTopicSerDe().getGenericRowSerde(
        schema, new KsqlConfig(Collections.emptyMap()), false,
        new MockSchemaRegistryClientFactory()::get);
  }

  @Test
  public void testSelectSchemaKStream() {
    final String selectQuery = "SELECT col0, col2, col3 FROM test1 WHERE col0 > 100;";
    final PlanNode logicalPlan = planBuilder.buildLogicalPlan(selectQuery);
    final ProjectNode projectNode = (ProjectNode) logicalPlan.getSources().get(0);
    initialSchemaKStream = new SchemaKStream(logicalPlan.getTheSourceNode().getSchema(), kStream,
                                             ksqlStream.getKeyField(), new ArrayList<>(),
                                             SchemaKStream.Type.SOURCE, functionRegistry, new MockSchemaRegistryClient());

    final List<SelectExpression> selectExpressions = projectNode.getProjectSelectExpressions();
    final SchemaKStream projectedSchemaKStream = initialSchemaKStream.select(selectExpressions);
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
    final String selectQuery = "SELECT col0 as NEWKEY, col2, col3 FROM test1;";
    final PlanNode logicalPlan = planBuilder.buildLogicalPlan(selectQuery);
    final ProjectNode projectNode = (ProjectNode) logicalPlan.getSources().get(0);
    initialSchemaKStream = new SchemaKStream(logicalPlan.getTheSourceNode().getSchema(), kStream,
        ksqlStream.getKeyField(), new ArrayList<>(),
        SchemaKStream.Type.SOURCE, functionRegistry, new MockSchemaRegistryClient());

    final List<SelectExpression> selectExpressions = projectNode.getProjectSelectExpressions();
    final SchemaKStream projectedSchemaKStream = initialSchemaKStream.select(selectExpressions);
    assertThat(
        projectedSchemaKStream.getKeyField(),
        equalTo(new Field("NEWKEY", 0, Schema.OPTIONAL_INT64_SCHEMA)));
  }

  @Test
  public void shouldPreserveKeyOnSelectStar() {
    final String selectQuery = "SELECT * FROM test1;";
    final PlanNode logicalPlan = planBuilder.buildLogicalPlan(selectQuery);
    final ProjectNode projectNode = (ProjectNode) logicalPlan.getSources().get(0);

    initialSchemaKStream = new SchemaKStream(
        logicalPlan.getTheSourceNode().getSchema(),
        kStream,
        ksqlStream.getKeyField(),
        new ArrayList<>(),
        SchemaKStream.Type.SOURCE,
        functionRegistry,
        new MockSchemaRegistryClient());

    final List<SelectExpression> selectExpressions = projectNode.getProjectSelectExpressions();
    final SchemaKStream projectedSchemaKStream = initialSchemaKStream.select(selectExpressions);
    assertThat(
        projectedSchemaKStream.getKeyField(),
        equalTo(initialSchemaKStream.getKeyField()));
  }

  @Test
  public void shouldUpdateKeyIfMovedToDifferentIndex() {
    final String selectQuery = "SELECT col2, col0, col3 FROM test1;";
    final PlanNode logicalPlan = planBuilder.buildLogicalPlan(selectQuery);
    final ProjectNode projectNode = (ProjectNode) logicalPlan.getSources().get(0);
    initialSchemaKStream = new SchemaKStream(logicalPlan.getTheSourceNode().getSchema(), kStream,
        ksqlStream.getKeyField(), new ArrayList<>(),
        SchemaKStream.Type.SOURCE, functionRegistry, new MockSchemaRegistryClient());

    final List<SelectExpression> selectExpressions = projectNode.getProjectSelectExpressions();
    final SchemaKStream projectedSchemaKStream = initialSchemaKStream.select(selectExpressions);
    assertThat(
        projectedSchemaKStream.getKeyField(),
        equalTo(new Field("COL0", 1, Schema.OPTIONAL_INT64_SCHEMA)));
  }

  @Test
  public void shouldDropKeyIfNotSelected() {
    final String selectQuery = "SELECT col2, col3 FROM test1;";
    final PlanNode logicalPlan = planBuilder.buildLogicalPlan(selectQuery);
    final ProjectNode projectNode = (ProjectNode) logicalPlan.getSources().get(0);
    initialSchemaKStream = new SchemaKStream(logicalPlan.getTheSourceNode().getSchema(), kStream,
        ksqlStream.getKeyField(), new ArrayList<>(),
        SchemaKStream.Type.SOURCE, functionRegistry, new MockSchemaRegistryClient());

    final List<SelectExpression> selectExpressions = projectNode.getProjectSelectExpressions();
    final SchemaKStream projectedSchemaKStream = initialSchemaKStream.select(selectExpressions);
    assertThat(projectedSchemaKStream.getKeyField(), nullValue());
  }

  @Test
  public void testSelectWithExpression() {
    final String selectQuery = "SELECT col0, LEN(UCASE(col2)), col3*3+5 FROM test1 WHERE col0 > 100;";
    final PlanNode logicalPlan = planBuilder.buildLogicalPlan(selectQuery);
    final ProjectNode projectNode = (ProjectNode) logicalPlan.getSources().get(0);
    initialSchemaKStream = new SchemaKStream(logicalPlan.getTheSourceNode().getSchema(), kStream,
                                             ksqlStream.getKeyField(), new ArrayList<>(),
                                             SchemaKStream.Type.SOURCE,
                                             functionRegistry, new MockSchemaRegistryClient());
    final SchemaKStream projectedSchemaKStream = initialSchemaKStream.select(projectNode.getProjectSelectExpressions());
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
    final String selectQuery = "SELECT col0, col2, col3 FROM test1 WHERE col0 > 100;";
    final PlanNode logicalPlan = planBuilder.buildLogicalPlan(selectQuery);
    final FilterNode filterNode = (FilterNode) logicalPlan.getSources().get(0).getSources().get(0);

    initialSchemaKStream = new SchemaKStream(logicalPlan.getTheSourceNode().getSchema(), kStream,
                                             ksqlStream.getKeyField(), new ArrayList<>(),
                                             SchemaKStream.Type.SOURCE, functionRegistry, new MockSchemaRegistryClient());
    final SchemaKStream filteredSchemaKStream = initialSchemaKStream.filter(filterNode.getPredicate());

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
    final String selectQuery = "SELECT col0, col2, col3 FROM test1 WHERE col0 > 100;";
    final PlanNode logicalPlan = planBuilder.buildLogicalPlan(selectQuery);

    initialSchemaKStream = new SchemaKStream(logicalPlan.getTheSourceNode().getSchema(), kStream,
        ksqlStream.getKeyField(), new ArrayList<>(),
        SchemaKStream.Type.SOURCE, functionRegistry, new MockSchemaRegistryClient());
    final SchemaKStream rekeyedSchemaKStream = initialSchemaKStream.selectKey(initialSchemaKStream
        .getSchema().fields()
        .get(3), true);
    assertThat(rekeyedSchemaKStream.getKeyField().name().toUpperCase(), equalTo("TEST1.COL1"));
  }

  @Test
  public void testGroupByKey() {
    final String selectQuery = "SELECT col0, col1 FROM test1 WHERE col0 > 100;";
    final PlanNode logicalPlan = planBuilder.buildLogicalPlan(selectQuery);
    initialSchemaKStream = new SchemaKStream(logicalPlan.getTheSourceNode().getSchema(), kStream,
        ksqlStream.getKeyField(), new ArrayList<>(),
        SchemaKStream.Type.SOURCE, functionRegistry, new MockSchemaRegistryClient());

    final Expression keyExpression = new DereferenceExpression(
        new QualifiedNameReference(QualifiedName.of("TEST1")), "COL0");
    final KsqlTopicSerDe ksqlTopicSerDe = new KsqlJsonTopicSerDe();
    final Serde<GenericRow> rowSerde = ksqlTopicSerDe.getGenericRowSerde(
        initialSchemaKStream.getSchema(), null, false, () -> null);
    final List<Expression> groupByExpressions = Arrays.asList(keyExpression);
    final SchemaKGroupedStream groupedSchemaKStream = initialSchemaKStream.groupBy(
        Serdes.String(), rowSerde, groupByExpressions);

    Assert.assertEquals(groupedSchemaKStream.getKeyField().name(), "COL0");
  }

  @Test
  public void testGroupByMultipleColumns() {
    final String selectQuery = "SELECT col0, col1 FROM test1 WHERE col0 > 100;";
    final PlanNode logicalPlan = planBuilder.buildLogicalPlan(selectQuery);
    initialSchemaKStream = new SchemaKStream(logicalPlan.getTheSourceNode().getSchema(), kStream,
        ksqlStream.getKeyField(), new ArrayList<>(),
        SchemaKStream.Type.SOURCE, functionRegistry, new MockSchemaRegistryClient());

    final Expression col0Expression = new DereferenceExpression(
        new QualifiedNameReference(QualifiedName.of("TEST1")), "COL0");
    final Expression col1Expression = new DereferenceExpression(
        new QualifiedNameReference(QualifiedName.of("TEST1")), "COL1");
    final KsqlTopicSerDe ksqlTopicSerDe = new KsqlJsonTopicSerDe();
    final Serde<GenericRow> rowSerde = ksqlTopicSerDe.getGenericRowSerde(
        initialSchemaKStream.getSchema(), null, false, () -> null);
    final List<Expression> groupByExpressions = Arrays.asList(col1Expression, col0Expression);
    final SchemaKGroupedStream groupedSchemaKStream = initialSchemaKStream.groupBy(
        Serdes.String(), rowSerde, groupByExpressions);

    Assert.assertEquals(groupedSchemaKStream.getKeyField().name(), "TEST1.COL1|+|TEST1.COL0");
  }

  @SuppressWarnings("unchecked")
  @Test
  public void shouldPerformStreamToStreamLeftJoin() {
    final JoinWindows joinWindow = JoinWindows.of(10);
    expect(mockKStream.leftJoin(eq(secondSchemaKStream.kstream),
                                anyObject(SchemaKStream.KsqlValueJoiner.class),
                                eq(joinWindow),
                                anyObject(Joined.class)))
        .andReturn(mockKStream);

    replay(mockKStream);

    final SchemaKStream joinedKStream = firstSchemaKStream
        .leftJoin(secondSchemaKStream,
                  joinSchema,
                  joinSchema.fields().get(0),
                  joinWindow,
                  leftSerde,
                  rightSerde);

    verify(mockKStream);
    assertThat(joinedKStream, instanceOf(SchemaKStream.class));
    assertEquals(SchemaKStream.Type.JOIN, joinedKStream.type);
    assertEquals(joinSchema, joinedKStream.schema);
    assertEquals(joinSchema.fields().get(0), joinedKStream.keyField);
    assertEquals(Arrays.asList(firstSchemaKStream, secondSchemaKStream),
                 joinedKStream.sourceSchemaKStreams);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void shouldPerformStreamToStreamInnerJoin() {
    final JoinWindows joinWindow = JoinWindows.of(10);
    expect(mockKStream.join(eq(secondSchemaKStream.kstream),
                            anyObject(SchemaKStream.KsqlValueJoiner.class),
                            eq(joinWindow),
                            anyObject(Joined.class)))
        .andReturn(mockKStream);

    replay(mockKStream);

    final SchemaKStream joinedKStream = firstSchemaKStream
        .join(secondSchemaKStream,
              joinSchema,
              joinSchema.fields().get(0),
              joinWindow,
              leftSerde,
              rightSerde);

    verify(mockKStream);

    assertThat(joinedKStream, instanceOf(SchemaKStream.class));
    assertEquals(SchemaKStream.Type.JOIN, joinedKStream.type);
    assertEquals(joinSchema, joinedKStream.schema);
    assertEquals(joinSchema.fields().get(0), joinedKStream.keyField);
    assertEquals(Arrays.asList(firstSchemaKStream, secondSchemaKStream),
                 joinedKStream.sourceSchemaKStreams);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void shouldPerformStreamToStreamOuterJoin() {
    final JoinWindows joinWindow = JoinWindows.of(10);
    expect(mockKStream.outerJoin(eq(secondSchemaKStream.kstream),
                                 anyObject(SchemaKStream.KsqlValueJoiner.class),
                                 eq(joinWindow),
                                 anyObject(Joined.class)))
        .andReturn(mockKStream);
    replay(mockKStream);

    final SchemaKStream joinedKStream = firstSchemaKStream
        .outerJoin(secondSchemaKStream,
                   joinSchema,
                   joinSchema.fields().get(0),
                   joinWindow,
                   leftSerde,
                   rightSerde);

    verify(mockKStream);
    assertThat(joinedKStream, instanceOf(SchemaKStream.class));
    assertEquals(SchemaKStream.Type.JOIN, joinedKStream.type);
    assertEquals(joinSchema, joinedKStream.schema);
    assertEquals(joinSchema.fields().get(0), joinedKStream.keyField);
    assertEquals(Arrays.asList(firstSchemaKStream, secondSchemaKStream),
                 joinedKStream.sourceSchemaKStreams);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void shouldPerformStreamToTableLeftJoin() {
    expect(mockKStream.leftJoin(eq(schemaKTable.getKtable()),
                                anyObject(SchemaKStream.KsqlValueJoiner.class),
                                anyObject(Joined.class)))
        .andReturn(mockKStream);

    replay(mockKStream);

    final SchemaKStream joinedKStream = firstSchemaKStream
        .leftJoin(schemaKTable, joinSchema, joinSchema.fields().get(0), leftSerde);

    verify(mockKStream);

    assertThat(joinedKStream, instanceOf(SchemaKStream.class));
    assertEquals(SchemaKStream.Type.JOIN, joinedKStream.type);
    assertEquals(joinSchema, joinedKStream.schema);
    assertEquals(joinSchema.fields().get(0), joinedKStream.keyField);
    assertEquals(Arrays.asList(firstSchemaKStream, schemaKTable),
                 joinedKStream.sourceSchemaKStreams);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void shouldPerformStreamToTableInnerJoin() {

    expect(mockKStream.join(eq(schemaKTable.getKtable()),
                            anyObject(SchemaKStream.KsqlValueJoiner.class),
                            anyObject(Joined.class)))
        .andReturn(mockKStream);

    replay(mockKStream);

    final SchemaKStream joinedKStream = firstSchemaKStream
        .join(schemaKTable, joinSchema, joinSchema.fields().get(0), leftSerde);

    verify(mockKStream);

    assertThat(joinedKStream, instanceOf(SchemaKStream.class));
    assertEquals(SchemaKStream.Type.JOIN, joinedKStream.type);
    assertEquals(joinSchema, joinedKStream.schema);
    assertEquals(joinSchema.fields().get(0), joinedKStream.keyField);
    assertEquals(Arrays.asList(firstSchemaKStream, schemaKTable),
                 joinedKStream.sourceSchemaKStreams);
  }

  private Schema getJoinSchema(final Schema leftSchema, final Schema rightSchema) {
    final SchemaBuilder schemaBuilder = SchemaBuilder.struct();
    final String leftAlias = "left";
    final String rightAlias = "right";
    for (final Field field : leftSchema.fields()) {
      final String fieldName = leftAlias + "." + field.name();
      schemaBuilder.field(fieldName, field.schema());
    }

    for (final Field field : rightSchema.fields()) {
      final String fieldName = rightAlias + "." + field.name();
      schemaBuilder.field(fieldName, field.schema());
    }
    return schemaBuilder.build();
  }

}
