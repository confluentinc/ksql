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

import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.isNull;
import static org.easymock.EasyMock.mock;
import static org.easymock.EasyMock.niceMock;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.same;
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
import io.confluent.ksql.structured.SchemaKStream.Type;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.MetaStoreFixture;
import io.confluent.ksql.util.SelectExpression;
import io.confluent.ksql.util.Pair;
import io.confluent.ksql.util.SchemaUtil;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serdes.StringSerde;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.Joined;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Predicate;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import io.confluent.ksql.streams.GroupedFactory;
import io.confluent.ksql.streams.JoinedFactory;

@SuppressWarnings("unchecked")
public class SchemaKStreamTest {

  private final MockSchemaRegistryClient schemaRegistryClient = new MockSchemaRegistryClient();
  private SchemaKStream initialSchemaKStream;

  private final KsqlConfig ksqlConfig = new KsqlConfig(Collections.emptyMap());
  private final MetaStore metaStore = MetaStoreFixture.getNewMetaStore(new InternalFunctionRegistry());
  private final LogicalPlanBuilder planBuilder = new LogicalPlanBuilder(metaStore);
  private final String JOIN_OP_NAME = "JOIN";
  private final String GROUP_OP_NAME = "GROUP";
  private final Grouped grouped = Grouped.with(
      GROUP_OP_NAME, Serdes.String(), Serdes.String());
  private final GroupedFactory mockGroupedFactory = mock(GroupedFactory.class);
  private final JoinedFactory mockJoinedFactory = mock(JoinedFactory.class);
  private final Joined joined = Joined.with(
      Serdes.String(), Serdes.String(), Serdes.String(), JOIN_OP_NAME);

  private KStream kStream;
  private KsqlStream ksqlStream;
  private KStream secondKStream;
  private KsqlStream secondKsqlStream;
  private KTable kTable;
  private KsqlTable ksqlTable;
  private InternalFunctionRegistry functionRegistry;
  private KStream mockKStream;
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

    secondSchemaKStream = buildSchemaKStreamForJoin(secondKsqlStream, secondKStream);

    leftSerde = getRowSerde(ksqlStream.getKsqlTopic(),
                                                    ksqlStream.getSchema());
    rightSerde = getRowSerde(secondKsqlStream.getKsqlTopic(),
                                                     secondKsqlStream.getSchema());


    schemaKTable = new SchemaKTable<>(ksqlTable.getSchema(), kTable,
                                    ksqlTable.getKeyField(), new ArrayList<>(),  Serdes.String(),
                                    SchemaKStream.Type.SOURCE, ksqlConfig,
                                    functionRegistry, schemaRegistryClient);

    joinSchema = getJoinSchema(ksqlStream.getSchema(), secondKsqlStream.getSchema());
  }

  private SchemaKStream buildSchemaKStream(
      final KsqlStream ksqlStream,
      final Schema schema,
      final KStream kStream,
      final GroupedFactory groupedFactory,
      final JoinedFactory joinedFactory) {
    return new SchemaKStream(
        schema,
        kStream,
        ksqlStream.getKeyField(),
        new ArrayList<>(),
        Serdes.String(),
        Type.SOURCE,
        ksqlConfig,
        functionRegistry,
        schemaRegistryClient,
        groupedFactory,
        joinedFactory);
  }
  private SchemaKStream buildSchemaKStream(
      final KsqlStream ksqlStream,
      final KStream kStream,
      final GroupedFactory groupedFactory,
      final JoinedFactory joinedFactory) {
    return buildSchemaKStream(
        ksqlStream,
        SchemaUtil.buildSchemaWithAlias(ksqlStream.getSchema(),
            ksqlStream.getName()),
        kStream,
        groupedFactory,
        joinedFactory);
  }

  private SchemaKStream buildSchemaKStream(
      final KsqlStream ksqlStream,
      final KStream kStream) {
    return buildSchemaKStream(
        ksqlStream,
        SchemaUtil.buildSchemaWithAlias(ksqlStream.getSchema(),
            ksqlStream.getName()),
        kStream,
        GroupedFactory.create(ksqlConfig),
        JoinedFactory.create(ksqlConfig));
  }

  private SchemaKStream buildSchemaKStreamForJoin(
      final KsqlStream ksqlStream,
      final KStream kStream) {
    return buildSchemaKStreamForJoin(
        ksqlStream,
        kStream,
        GroupedFactory.create(ksqlConfig),
        JoinedFactory.create(ksqlConfig));
  }

  private SchemaKStream buildSchemaKStreamForJoin(
      final KsqlStream ksqlStream,
      final KStream kStream,
      final GroupedFactory groupedFactory,
      final JoinedFactory joinedFactory) {
    return buildSchemaKStream(
        ksqlStream,
        ksqlStream.getSchema(),
        kStream,
        groupedFactory,
        joinedFactory);
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
                                             Serdes.String(),
                                             SchemaKStream.Type.SOURCE, ksqlConfig,
                                             functionRegistry, schemaRegistryClient);

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
        ksqlStream.getKeyField(), new ArrayList<>(), Serdes.String(),
        SchemaKStream.Type.SOURCE, ksqlConfig,
        functionRegistry, schemaRegistryClient);

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
        Serdes.String(),
        SchemaKStream.Type.SOURCE,
        ksqlConfig,
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
        ksqlStream.getKeyField(), new ArrayList<>(), Serdes.String(),
        SchemaKStream.Type.SOURCE, ksqlConfig,
        functionRegistry, schemaRegistryClient);

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
        ksqlStream.getKeyField(), new ArrayList<>(), Serdes.String(),
        SchemaKStream.Type.SOURCE, ksqlConfig,
        functionRegistry, schemaRegistryClient);

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
                                             ksqlStream.getKeyField(), new ArrayList<>(), Serdes.String(),
                                             SchemaKStream.Type.SOURCE, ksqlConfig,
                                             functionRegistry, schemaRegistryClient);
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
                                             ksqlStream.getKeyField(), new ArrayList<>(), Serdes.String(),
                                             SchemaKStream.Type.SOURCE, ksqlConfig,
                                             functionRegistry, schemaRegistryClient);
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
        ksqlStream.getKeyField(), new ArrayList<>(), Serdes.String(),
        SchemaKStream.Type.SOURCE, ksqlConfig,
        functionRegistry, schemaRegistryClient);
    final SchemaKStream rekeyedSchemaKStream = initialSchemaKStream.selectKey(initialSchemaKStream
        .getSchema().fields()
        .get(3), true);
    assertThat(rekeyedSchemaKStream.getKeyField().name().toUpperCase(), equalTo("TEST1.COL1"));
  }

  @Test
  public void testGroupByKey() {
    initialSchemaKStream = buildSchemaKStream(ksqlStream, kStream);

    final Expression keyExpression = new DereferenceExpression(
        new QualifiedNameReference(QualifiedName.of("TEST1")), "COL0");
    final KsqlTopicSerDe ksqlTopicSerDe = new KsqlJsonTopicSerDe();
    final Serde<GenericRow> rowSerde = ksqlTopicSerDe.getGenericRowSerde(
        initialSchemaKStream.getSchema(), null, false, () -> null);
    final List<Expression> groupByExpressions = Arrays.asList(keyExpression);
    final SchemaKGroupedStream groupedSchemaKStream = initialSchemaKStream.groupBy(
        rowSerde, groupByExpressions, GROUP_OP_NAME);

    Assert.assertEquals(groupedSchemaKStream.getKeyField().name(), "COL0");
  }

  @Test
  public void testGroupByMultipleColumns() {
    final String selectQuery = "SELECT col0, col1 FROM test1 WHERE col0 > 100;";
    final PlanNode logicalPlan = planBuilder.buildLogicalPlan(selectQuery);
    initialSchemaKStream = new SchemaKStream(logicalPlan.getTheSourceNode().getSchema(), kStream,
        ksqlStream.getKeyField(), new ArrayList<>(), Serdes.String(),
        SchemaKStream.Type.SOURCE, ksqlConfig,
        functionRegistry, schemaRegistryClient);

    final Expression col0Expression = new DereferenceExpression(
        new QualifiedNameReference(QualifiedName.of("TEST1")), "COL0");
    final Expression col1Expression = new DereferenceExpression(
        new QualifiedNameReference(QualifiedName.of("TEST1")), "COL1");
    final KsqlTopicSerDe ksqlTopicSerDe = new KsqlJsonTopicSerDe();
    final Serde<GenericRow> rowSerde = ksqlTopicSerDe.getGenericRowSerde(
        initialSchemaKStream.getSchema(), null, false, () -> null);
    final List<Expression> groupByExpressions = Arrays.asList(col1Expression, col0Expression);
    final SchemaKGroupedStream groupedSchemaKStream = initialSchemaKStream.groupBy(
        rowSerde, groupByExpressions, GROUP_OP_NAME);

    Assert.assertEquals(groupedSchemaKStream.getKeyField().name(), "TEST1.COL1|+|TEST1.COL0");
  }

  @Test
  public void shouldUseFactoryForGroupedWithoutRekey() {
    // Given:
    expect(mockGroupedFactory.create(eq(GROUP_OP_NAME), anyObject(StringSerde.class), same(leftSerde)))
        .andReturn(grouped);
    final KGroupedStream groupedStream = mock(KGroupedStream.class);
    expect(mockKStream.groupByKey(same(grouped)))
        .andReturn(groupedStream);
    replay(mockKStream, mockGroupedFactory);
    final Expression keyExpression = new DereferenceExpression(
        new QualifiedNameReference(QualifiedName.of(ksqlStream.getName())),
        ksqlStream.getKeyField().name());
    final List<Expression> groupByExpressions = Collections.singletonList(keyExpression);
    final SchemaKStream initialSchemaKStream
        = buildSchemaKStream(ksqlStream, mockKStream, mockGroupedFactory, mockJoinedFactory);

    // When:
    initialSchemaKStream.groupBy(leftSerde, groupByExpressions, GROUP_OP_NAME);

    // Then:
    verify(mockKStream, mockGroupedFactory);
  }

  @Test
  public void shouldUseFactoryForGrouped() {
    // Given:
    expect(mockGroupedFactory.create(eq(GROUP_OP_NAME), anyObject(StringSerde.class), same(leftSerde)))
        .andReturn(grouped);
    expect(mockKStream.filter(anyObject(Predicate.class))).andReturn(mockKStream);
    final KGroupedStream groupedStream = mock(KGroupedStream.class);
    expect(mockKStream.groupBy(anyObject(KeyValueMapper.class), same(grouped)))
        .andReturn(groupedStream);
    replay(mockKStream, mockGroupedFactory);
    final Expression col0Expression = new DereferenceExpression(
        new QualifiedNameReference(QualifiedName.of(ksqlStream.getName())), "COL0");
    final Expression col1Expression = new DereferenceExpression(
        new QualifiedNameReference(QualifiedName.of(ksqlStream.getName())), "COL1");
    final List<Expression> groupByExpressions = Arrays.asList(col1Expression, col0Expression);
    final SchemaKStream initialSchemaKStream =
        buildSchemaKStream(ksqlStream, mockKStream, mockGroupedFactory, mockJoinedFactory);

    // When:
    initialSchemaKStream.groupBy(leftSerde, groupByExpressions, GROUP_OP_NAME);

    // Then:
    verify(mockKStream, mockGroupedFactory);
  }

  private void expectCreateJoined(final String opName, final Serde<GenericRow> rightSerde) {
    expect(mockJoinedFactory.create(anyObject(StringSerde.class), same(leftSerde), same(rightSerde), eq(opName)))
        .andReturn(joined);
    replay(mockJoinedFactory);
  }

  private void expectCreateJoined(final String opName) {
    expectCreateJoined(opName, rightSerde);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void shouldPerformStreamToStreamLeftJoin() {
    final SchemaKStream initialSchemaKStream =
        buildSchemaKStreamForJoin(ksqlStream, mockKStream, mockGroupedFactory, mockJoinedFactory);
    final JoinWindows joinWindow = JoinWindows.of(Duration.ofMillis(10L));
    expect(mockKStream.leftJoin(eq(secondSchemaKStream.kstream),
                                anyObject(SchemaKStream.KsqlValueJoiner.class),
                                eq(joinWindow),
                                same(joined)))
        .andReturn(mockKStream);
    expectCreateJoined(JOIN_OP_NAME);

    replay(mockKStream);

    final SchemaKStream joinedKStream = initialSchemaKStream
        .leftJoin(secondSchemaKStream,
                  joinSchema,
                  joinSchema.fields().get(0),
                  joinWindow,
                  leftSerde,
                  rightSerde,
                  JOIN_OP_NAME);

    verify(mockKStream, mockJoinedFactory);
    assertThat(joinedKStream, instanceOf(SchemaKStream.class));
    assertEquals(SchemaKStream.Type.JOIN, joinedKStream.type);
    assertEquals(joinSchema, joinedKStream.schema);
    assertEquals(joinSchema.fields().get(0), joinedKStream.keyField);
    assertEquals(Arrays.asList(initialSchemaKStream, secondSchemaKStream),
                 joinedKStream.sourceSchemaKStreams);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void shouldPerformStreamToStreamInnerJoin() {
    final SchemaKStream initialSchemaKStream =
        buildSchemaKStreamForJoin(ksqlStream, mockKStream, mockGroupedFactory, mockJoinedFactory);
    final JoinWindows joinWindow = JoinWindows.of(Duration.ofMillis(10L));
    expect(mockKStream.join(eq(secondSchemaKStream.kstream),
                            anyObject(SchemaKStream.KsqlValueJoiner.class),
                            eq(joinWindow),
                            same(joined)))
        .andReturn(mockKStream);
    expectCreateJoined(JOIN_OP_NAME);

    replay(mockKStream);

    final SchemaKStream joinedKStream = initialSchemaKStream
        .join(secondSchemaKStream,
              joinSchema,
              joinSchema.fields().get(0),
              joinWindow,
              leftSerde,
              rightSerde,
              "JOIN");

    verify(mockKStream, mockJoinedFactory);
    assertThat(joinedKStream, instanceOf(SchemaKStream.class));
    assertEquals(SchemaKStream.Type.JOIN, joinedKStream.type);
    assertEquals(joinSchema, joinedKStream.schema);
    assertEquals(joinSchema.fields().get(0), joinedKStream.keyField);
    assertEquals(Arrays.asList(initialSchemaKStream, secondSchemaKStream),
                 joinedKStream.sourceSchemaKStreams);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void shouldPerformStreamToStreamOuterJoin() {
    final SchemaKStream initialSchemaKStream =
        buildSchemaKStreamForJoin(ksqlStream, mockKStream, mockGroupedFactory, mockJoinedFactory);
    final JoinWindows joinWindow = JoinWindows.of(Duration.ofMillis(10L));
    expect(mockKStream.outerJoin(eq(secondSchemaKStream.kstream),
                                 anyObject(SchemaKStream.KsqlValueJoiner.class),
                                 eq(joinWindow),
                                 same(joined)))
        .andReturn(mockKStream);
    expectCreateJoined(JOIN_OP_NAME);
    replay(mockKStream);

    final SchemaKStream joinedKStream = initialSchemaKStream
        .outerJoin(secondSchemaKStream,
                   joinSchema,
                   joinSchema.fields().get(0),
                   joinWindow,
                   leftSerde,
                   rightSerde,
                   "JOIN");

    verify(mockKStream, mockJoinedFactory);
    assertThat(joinedKStream, instanceOf(SchemaKStream.class));
    assertEquals(SchemaKStream.Type.JOIN, joinedKStream.type);
    assertEquals(joinSchema, joinedKStream.schema);
    assertEquals(joinSchema.fields().get(0), joinedKStream.keyField);
    assertEquals(Arrays.asList(initialSchemaKStream, secondSchemaKStream),
                 joinedKStream.sourceSchemaKStreams);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void shouldPerformStreamToTableLeftJoin() {
    final SchemaKStream initialSchemaKStream =
        buildSchemaKStreamForJoin(ksqlStream, mockKStream, mockGroupedFactory, mockJoinedFactory);
    expectCreateJoined(JOIN_OP_NAME, null);
    expect(mockKStream.leftJoin(eq(schemaKTable.getKtable()),
                                anyObject(SchemaKStream.KsqlValueJoiner.class),
                                same(joined)))
        .andReturn(mockKStream);

    replay(mockKStream);

    final SchemaKStream joinedKStream = initialSchemaKStream
        .leftJoin(schemaKTable, joinSchema, joinSchema.fields().get(0), leftSerde, "JOIN");

    verify(mockKStream, mockJoinedFactory);
    assertThat(joinedKStream, instanceOf(SchemaKStream.class));
    assertEquals(SchemaKStream.Type.JOIN, joinedKStream.type);
    assertEquals(joinSchema, joinedKStream.schema);
    assertEquals(joinSchema.fields().get(0), joinedKStream.keyField);
    assertEquals(Arrays.asList(initialSchemaKStream, schemaKTable),
                 joinedKStream.sourceSchemaKStreams);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void shouldPerformStreamToTableInnerJoin() {
    final SchemaKStream initialSchemaKStream =
        buildSchemaKStreamForJoin(ksqlStream, mockKStream, mockGroupedFactory, mockJoinedFactory);
    expectCreateJoined(JOIN_OP_NAME, null);
    expect(mockKStream.join(eq(schemaKTable.getKtable()),
                            anyObject(SchemaKStream.KsqlValueJoiner.class),
                            same(joined)))
        .andReturn(mockKStream);

    replay(mockKStream);

    final SchemaKStream joinedKStream = initialSchemaKStream
        .join(schemaKTable, joinSchema, joinSchema.fields().get(0), leftSerde, "JOIN");

    verify(mockKStream, mockJoinedFactory);
    assertThat(joinedKStream, instanceOf(SchemaKStream.class));
    assertEquals(SchemaKStream.Type.JOIN, joinedKStream.type);
    assertEquals(joinSchema, joinedKStream.schema);
    assertEquals(joinSchema.fields().get(0), joinedKStream.keyField);
    assertEquals(Arrays.asList(initialSchemaKStream, schemaKTable),
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
