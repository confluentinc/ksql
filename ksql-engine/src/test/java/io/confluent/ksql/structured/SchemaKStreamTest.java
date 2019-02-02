/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.structured;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
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
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.serde.KsqlTopicSerDe;
import io.confluent.ksql.serde.json.KsqlJsonTopicSerDe;
import io.confluent.ksql.streams.MaterializedFactory;
import io.confluent.ksql.streams.StreamsFactories;
import io.confluent.ksql.streams.StreamsUtil;
import io.confluent.ksql.structured.SchemaKStream.Type;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.MetaStoreFixture;
import io.confluent.ksql.util.SelectExpression;
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
import org.junit.Rule;
import org.junit.Test;
import io.confluent.ksql.streams.GroupedFactory;
import io.confluent.ksql.streams.JoinedFactory;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

@SuppressWarnings("unchecked")
public class SchemaKStreamTest {

  private SchemaKStream initialSchemaKStream;

  private final KsqlConfig ksqlConfig = new KsqlConfig(Collections.emptyMap());
  private final MetaStore metaStore = MetaStoreFixture.getNewMetaStore(new InternalFunctionRegistry());
  private final LogicalPlanBuilder planBuilder = new LogicalPlanBuilder(metaStore);

  private final Grouped grouped = Grouped.with(
      "group", Serdes.String(), Serdes.String());
  private final Joined joined = Joined.with(
      Serdes.String(), Serdes.String(), Serdes.String(), "join");

  private KStream kStream;
  private KsqlStream ksqlStream;
  private InternalFunctionRegistry functionRegistry;
  private SchemaKStream secondSchemaKStream;
  private SchemaKTable schemaKTable;
  private Serde<GenericRow> leftSerde;
  private Serde<GenericRow> rightSerde;
  private Schema joinSchema;
  private final Schema simpleSchema = SchemaBuilder.struct()
      .field("key", Schema.OPTIONAL_STRING_SCHEMA)
      .field("val", Schema.OPTIONAL_INT64_SCHEMA)
      .build();
  private final QueryContext.Stacker queryContext
      = new QueryContext.Stacker(new QueryId("query")).push("node");
  private final QueryContext parentContext = queryContext.push("parent").getQueryContext();
  private final QueryContext.Stacker childContextStacker = queryContext.push("child");

  @Mock
  private GroupedFactory mockGroupedFactory;
  @Mock
  private JoinedFactory mockJoinedFactory;
  @Mock
  private KStream mockKStream;

  @Rule
  public final MockitoRule mockitoRule = MockitoJUnit.rule();

  @Before
  public void init() {
    functionRegistry = new InternalFunctionRegistry();
    ksqlStream = (KsqlStream) metaStore.getSource("TEST1");
    final StreamsBuilder builder = new StreamsBuilder();
    kStream = builder.stream(
        ksqlStream.getKsqlTopic().getKafkaTopicName(),
        Consumed.with(Serdes.String(),
        getRowSerde(ksqlStream.getKsqlTopic(), ksqlStream.getSchema())));

    final KsqlStream secondKsqlStream = (KsqlStream) metaStore.getSource("ORDERS");
    final KStream secondKStream = builder
        .stream(secondKsqlStream.getKsqlTopic().getKafkaTopicName(),
            Consumed.with(Serdes.String(),
                getRowSerde(secondKsqlStream.getKsqlTopic(),
                    secondKsqlStream.getSchema())));

    final KsqlTable ksqlTable = (KsqlTable) metaStore.getSource("TEST2");
    final KTable kTable = builder.table(ksqlTable.getKsqlTopic().getKafkaTopicName(),
        Consumed.with(Serdes.String(),
            getRowSerde(ksqlTable.getKsqlTopic(),
                ksqlTable.getSchema())));

    secondSchemaKStream = buildSchemaKStreamForJoin(secondKsqlStream, secondKStream);

    leftSerde = getRowSerde(ksqlStream.getKsqlTopic(), ksqlStream.getSchema());
    rightSerde = getRowSerde(secondKsqlStream.getKsqlTopic(),
                                                     secondKsqlStream.getSchema());

    schemaKTable = new SchemaKTable<>(
        ksqlTable.getSchema(),
        kTable,
        ksqlTable.getKeyField(),
        new ArrayList<>(),
        Serdes.String(),
        SchemaKStream.Type.SOURCE,
        ksqlConfig,
        functionRegistry,
        parentContext);

    joinSchema = getJoinSchema(ksqlStream.getSchema(), secondKsqlStream.getSchema());

    whenCreateJoined();
  }

  private Serde<GenericRow> getRowSerde(final KsqlTopic topic, final Schema schema) {
    return topic.getKsqlTopicSerDe().getGenericRowSerde(
        schema,
        new KsqlConfig(Collections.emptyMap()),
        false,
        MockSchemaRegistryClient::new,
        "test");
  }

  @Test
  public void testSelectSchemaKStream() {
    final String selectQuery = "SELECT col0, col2, col3 FROM test1 WHERE col0 > 100;";
    final PlanNode logicalPlan = planBuilder.buildLogicalPlan(selectQuery);
    final ProjectNode projectNode = (ProjectNode) logicalPlan.getSources().get(0);
    initialSchemaKStream = buildSchemaKStream(logicalPlan.getTheSourceNode().getSchema());

    final List<SelectExpression> selectExpressions = projectNode.getProjectSelectExpressions();
    final SchemaKStream projectedSchemaKStream = initialSchemaKStream.select(
        selectExpressions,
        childContextStacker);
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
    initialSchemaKStream = buildSchemaKStream(logicalPlan.getTheSourceNode().getSchema());

    final List<SelectExpression> selectExpressions = projectNode.getProjectSelectExpressions();
    final SchemaKStream projectedSchemaKStream = initialSchemaKStream.select(
        selectExpressions,
        childContextStacker);
    assertThat(
        projectedSchemaKStream.getKeyField(),
        equalTo(new Field("NEWKEY", 0, Schema.OPTIONAL_INT64_SCHEMA)));
  }

  @Test
  public void shouldPreserveKeyOnSelectStar() {
    final String selectQuery = "SELECT * FROM test1;";
    final PlanNode logicalPlan = planBuilder.buildLogicalPlan(selectQuery);
    final ProjectNode projectNode = (ProjectNode) logicalPlan.getSources().get(0);
    initialSchemaKStream = buildSchemaKStream(logicalPlan.getTheSourceNode().getSchema());

    final List<SelectExpression> selectExpressions = projectNode.getProjectSelectExpressions();
    final SchemaKStream projectedSchemaKStream = initialSchemaKStream.select(
        selectExpressions,
        childContextStacker);
    assertThat(
        projectedSchemaKStream.getKeyField(),
        equalTo(initialSchemaKStream.getKeyField()));
  }

  @Test
  public void shouldUpdateKeyIfMovedToDifferentIndex() {
    final String selectQuery = "SELECT col2, col0, col3 FROM test1;";
    final PlanNode logicalPlan = planBuilder.buildLogicalPlan(selectQuery);
    final ProjectNode projectNode = (ProjectNode) logicalPlan.getSources().get(0);
    initialSchemaKStream = buildSchemaKStream(logicalPlan.getTheSourceNode().getSchema());

    final List<SelectExpression> selectExpressions = projectNode.getProjectSelectExpressions();
    final SchemaKStream projectedSchemaKStream = initialSchemaKStream.select(
        selectExpressions,
        childContextStacker);
    assertThat(
        projectedSchemaKStream.getKeyField(),
        equalTo(new Field("COL0", 1, Schema.OPTIONAL_INT64_SCHEMA)));
  }

  @Test
  public void shouldDropKeyIfNotSelected() {
    final String selectQuery = "SELECT col2, col3 FROM test1;";
    final PlanNode logicalPlan = planBuilder.buildLogicalPlan(selectQuery);
    final ProjectNode projectNode = (ProjectNode) logicalPlan.getSources().get(0);
    initialSchemaKStream = buildSchemaKStream(logicalPlan.getTheSourceNode().getSchema());

    final List<SelectExpression> selectExpressions = projectNode.getProjectSelectExpressions();
    final SchemaKStream projectedSchemaKStream = initialSchemaKStream.select(
        selectExpressions,
        childContextStacker);
    assertThat(projectedSchemaKStream.getKeyField(), nullValue());
  }

  @Test
  public void testSelectWithExpression() {
    final String selectQuery = "SELECT col0, LEN(UCASE(col2)), col3*3+5 FROM test1 WHERE col0 > 100;";
    final PlanNode logicalPlan = planBuilder.buildLogicalPlan(selectQuery);
    final ProjectNode projectNode = (ProjectNode) logicalPlan.getSources().get(0);
    initialSchemaKStream = buildSchemaKStream(logicalPlan.getTheSourceNode().getSchema());
    final SchemaKStream projectedSchemaKStream = initialSchemaKStream.select(
        projectNode.getProjectSelectExpressions(),
        childContextStacker);
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
    initialSchemaKStream = buildSchemaKStream(logicalPlan.getTheSourceNode().getSchema());
    final SchemaKStream filteredSchemaKStream = initialSchemaKStream.filter(
        filterNode.getPredicate(),
        childContextStacker);

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
    initialSchemaKStream = buildSchemaKStream(logicalPlan.getTheSourceNode().getSchema());
    final SchemaKStream rekeyedSchemaKStream = initialSchemaKStream.selectKey(
        initialSchemaKStream.getSchema().fields().get(3),
        true,
        childContextStacker);
    assertThat(rekeyedSchemaKStream.getKeyField().name().toUpperCase(), equalTo("TEST1.COL1"));
  }

  @Test
  public void testGroupByKey() {
    initialSchemaKStream = buildSchemaKStream(
        SchemaUtil.buildSchemaWithAlias(ksqlStream.getSchema(),
            ksqlStream.getName()));

    final Expression keyExpression = new DereferenceExpression(
        new QualifiedNameReference(QualifiedName.of("TEST1")), "COL0");
    final KsqlTopicSerDe ksqlTopicSerDe = new KsqlJsonTopicSerDe();
    final Serde<GenericRow> rowSerde = ksqlTopicSerDe.getGenericRowSerde(
        initialSchemaKStream.getSchema(), null, false, () -> null, "test");
    final List<Expression> groupByExpressions = Collections.singletonList(keyExpression);
    final SchemaKGroupedStream groupedSchemaKStream = initialSchemaKStream.groupBy(
        rowSerde,
        groupByExpressions,
        childContextStacker);

    Assert.assertEquals(groupedSchemaKStream.getKeyField().name(), "COL0");
  }

  @Test
  public void testGroupByMultipleColumns() {
    final String selectQuery = "SELECT col0, col1 FROM test1 WHERE col0 > 100;";
    final PlanNode logicalPlan = planBuilder.buildLogicalPlan(selectQuery);
    initialSchemaKStream = buildSchemaKStream(logicalPlan.getTheSourceNode().getSchema());
    final Expression col0Expression = new DereferenceExpression(
        new QualifiedNameReference(QualifiedName.of("TEST1")), "COL0");
    final Expression col1Expression = new DereferenceExpression(
        new QualifiedNameReference(QualifiedName.of("TEST1")), "COL1");
    final KsqlTopicSerDe ksqlTopicSerDe = new KsqlJsonTopicSerDe();
    final Serde<GenericRow> rowSerde = ksqlTopicSerDe.getGenericRowSerde(
        initialSchemaKStream.getSchema(),
        null,
        false,
        () -> null,
        "test");
    final List<Expression> groupByExpressions = Arrays.asList(col1Expression, col0Expression);
    final SchemaKGroupedStream groupedSchemaKStream = initialSchemaKStream.groupBy(
        rowSerde,
        groupByExpressions,
        childContextStacker);

    Assert.assertEquals(groupedSchemaKStream.getKeyField().name(), "TEST1.COL1|+|TEST1.COL0");
  }

  @Test
  public void shouldUseFactoryForGroupedWithoutRekey() {
    // Given:
    when(mockGroupedFactory.create(anyString(), any(StringSerde.class), any(Serde.class)))
        .thenReturn(grouped);
    final KGroupedStream groupedStream = mock(KGroupedStream.class);
    when(mockKStream.groupByKey(any(Grouped.class))).thenReturn(groupedStream);
    final Expression keyExpression = new DereferenceExpression(
        new QualifiedNameReference(QualifiedName.of(ksqlStream.getName())),
        ksqlStream.getKeyField().name());
    final List<Expression> groupByExpressions = Collections.singletonList(keyExpression);
    final SchemaKStream initialSchemaKStream
        = buildSchemaKStream(mockKStream, mockGroupedFactory, mockJoinedFactory);

    // When:
    initialSchemaKStream.groupBy(
        leftSerde,
        groupByExpressions,
        childContextStacker);

    // Then:
    verify(mockGroupedFactory).create(
        eq(StreamsUtil.buildOpName(childContextStacker.getQueryContext())),
        any(StringSerde.class),
        same(leftSerde)
    );
    verify(mockKStream).groupByKey(same(grouped));
  }

  @Test
  public void shouldUseFactoryForGrouped() {
    // Given:
    when(mockGroupedFactory.create(anyString(), any(StringSerde.class), any(Serde.class)))
        .thenReturn(grouped);
    when(mockKStream.filter(any(Predicate.class))).thenReturn(mockKStream);
    final KGroupedStream groupedStream = mock(KGroupedStream.class);
    when(mockKStream.groupBy(any(KeyValueMapper.class), any(Grouped.class)))
        .thenReturn(groupedStream);
    final Expression col0Expression = new DereferenceExpression(
        new QualifiedNameReference(QualifiedName.of(ksqlStream.getName())), "COL0");
    final Expression col1Expression = new DereferenceExpression(
        new QualifiedNameReference(QualifiedName.of(ksqlStream.getName())), "COL1");
    final List<Expression> groupByExpressions = Arrays.asList(col1Expression, col0Expression);
    final SchemaKStream initialSchemaKStream =
        buildSchemaKStream(mockKStream, mockGroupedFactory, mockJoinedFactory);

    // When:
    initialSchemaKStream.groupBy(
        leftSerde,
        groupByExpressions,
        childContextStacker);

    // Then:
    verify(mockGroupedFactory).create(
        eq(StreamsUtil.buildOpName(childContextStacker.getQueryContext())),
        any(StringSerde.class),
        same(leftSerde));
    verify(mockKStream).groupBy(any(KeyValueMapper.class), same(grouped));
  }

  private void whenCreateJoined() {
    when(
        mockJoinedFactory.create(
            any(StringSerde.class),
            any(Serde.class),
            any(),
            anyString())
    ).thenReturn(joined);
  }

  private void verifyCreateJoined(final Serde<GenericRow> rightSerde) {
    verify(mockJoinedFactory).create(
        any(StringSerde.class),
        same(leftSerde),
        same(rightSerde),
        eq(StreamsUtil.buildOpName(childContextStacker.getQueryContext()))
    );
  }

  @SuppressWarnings("unchecked")
  @Test
  public void shouldPerformStreamToStreamLeftJoin() {
    // Given:
    final SchemaKStream initialSchemaKStream =
        buildSchemaKStreamForJoin(ksqlStream, mockKStream, mockGroupedFactory, mockJoinedFactory);
    final JoinWindows joinWindow = JoinWindows.of(Duration.ofMillis(10L));
    when(
        mockKStream.leftJoin(
            any(KStream.class),
            any(SchemaKStream.KsqlValueJoiner.class),
            any(JoinWindows.class),
            any(Joined.class))
    ).thenReturn(mockKStream);

    // When:
    final SchemaKStream joinedKStream = initialSchemaKStream
        .leftJoin(secondSchemaKStream,
                  joinSchema,
                  joinSchema.fields().get(0),
                  joinWindow,
                  leftSerde,
                  rightSerde,
            childContextStacker);

    // Then:
    verifyCreateJoined(rightSerde);
    verify(mockKStream).leftJoin(
        eq(secondSchemaKStream.kstream),
        any(SchemaKStream.KsqlValueJoiner.class),
        eq(joinWindow),
        same(joined)
    );
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
    // Given:
    final SchemaKStream initialSchemaKStream =
        buildSchemaKStreamForJoin(ksqlStream, mockKStream, mockGroupedFactory, mockJoinedFactory);
    final JoinWindows joinWindow = JoinWindows.of(Duration.ofMillis(10L));
    when(
        mockKStream.join(
            any(KStream.class),
            any(SchemaKStream.KsqlValueJoiner.class),
            any(JoinWindows.class),
            any(Joined.class))
    ).thenReturn(mockKStream);

    // When:
    final SchemaKStream joinedKStream = initialSchemaKStream
        .join(secondSchemaKStream,
              joinSchema,
              joinSchema.fields().get(0),
              joinWindow,
              leftSerde,
              rightSerde,
            childContextStacker);

    // Then:
    verifyCreateJoined(rightSerde);
    verify(mockKStream).join(
        eq(secondSchemaKStream.kstream),
        any(SchemaKStream.KsqlValueJoiner.class),
        eq(joinWindow),
        same(joined)
    );

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
    // Given:
    final SchemaKStream initialSchemaKStream =
        buildSchemaKStreamForJoin(ksqlStream, mockKStream, mockGroupedFactory, mockJoinedFactory);
    final JoinWindows joinWindow = JoinWindows.of(Duration.ofMillis(10L));
    when(
        mockKStream.outerJoin(
            any(KStream.class),
            any(SchemaKStream.KsqlValueJoiner.class),
            any(JoinWindows.class),
            any(Joined.class))
    ).thenReturn(mockKStream);

    // When:
    final SchemaKStream joinedKStream = initialSchemaKStream
        .outerJoin(secondSchemaKStream,
                   joinSchema,
                   joinSchema.fields().get(0),
                   joinWindow,
                   leftSerde,
                   rightSerde,
            childContextStacker);

    // Then:
    verifyCreateJoined(rightSerde);
    verify(mockKStream).outerJoin(
        eq(secondSchemaKStream.kstream),
        any(SchemaKStream.KsqlValueJoiner.class),
        eq(joinWindow),
        same(joined)
    );
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
    // Given:
    final SchemaKStream initialSchemaKStream =
        buildSchemaKStreamForJoin(ksqlStream, mockKStream, mockGroupedFactory, mockJoinedFactory);
    when(
        mockKStream.leftJoin(
            any(KTable.class),
            any(SchemaKStream.KsqlValueJoiner.class),
            any(Joined.class))
    ).thenReturn(mockKStream);

    // When:
    final SchemaKStream joinedKStream = initialSchemaKStream
        .leftJoin(
            schemaKTable,
            joinSchema,
            joinSchema.fields().get(0),
            leftSerde,
            childContextStacker);

    // Then:
    verifyCreateJoined(null);
    verify(mockKStream).leftJoin(
        eq(schemaKTable.getKtable()),
        any(SchemaKStream.KsqlValueJoiner.class),
        same(joined));
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
    // Given:
    final SchemaKStream initialSchemaKStream =
        buildSchemaKStreamForJoin(ksqlStream, mockKStream, mockGroupedFactory, mockJoinedFactory);
    when(
        mockKStream.join(
            any(KTable.class),
            any(SchemaKStream.KsqlValueJoiner.class),
            any(Joined.class))
    ).thenReturn(mockKStream);

    // When:
    final SchemaKStream joinedKStream = initialSchemaKStream
        .join(
            schemaKTable,
            joinSchema,
            joinSchema.fields().get(0),
            leftSerde,
            childContextStacker);

    // Then:
    verifyCreateJoined(null);
    verify(mockKStream).join(
        eq(schemaKTable.getKtable()),
        any(SchemaKStream.KsqlValueJoiner.class),
        same(joined)
    );

    assertThat(joinedKStream, instanceOf(SchemaKStream.class));
    assertEquals(SchemaKStream.Type.JOIN, joinedKStream.type);
    assertEquals(joinSchema, joinedKStream.schema);
    assertEquals(joinSchema.fields().get(0), joinedKStream.keyField);
    assertEquals(Arrays.asList(initialSchemaKStream, schemaKTable),
                 joinedKStream.sourceSchemaKStreams);
  }

  @Test
  public void shouldSummarizeExecutionPlanCorrectly() {
    // Given:
    final SchemaKStream parentSchemaKStream = mock(SchemaKStream.class);
    when(parentSchemaKStream.getExecutionPlan(anyString()))
        .thenReturn("parent plan");
    final SchemaKStream schemaKtream = new SchemaKStream(
        simpleSchema,
        mock(KStream.class),
        simpleSchema.field("key"),
        ImmutableList.of(parentSchemaKStream),
        Serdes.String(),
        Type.SOURCE,
        ksqlConfig,
        functionRegistry,
        queryContext.push("source").getQueryContext());

    // When/Then:
    final String expected =
        " > [ SOURCE ] | Schema: [key : VARCHAR, val : BIGINT] | Logger: query.node.source\n\t"
        + "parent plan";
    assertThat(schemaKtream.getExecutionPlan(""), equalTo(expected));
  }

  @Test
  public void shouldSummarizeExecutionPlanCorrectlyForRoot() {
    // Given:
    final SchemaKStream schemaKtream = new SchemaKStream(
        simpleSchema,
        mock(KStream.class),
        simpleSchema.field("key"),
        Collections.emptyList(),
        Serdes.String(),
        Type.SOURCE,
        ksqlConfig,
        functionRegistry,
        queryContext.push("source").getQueryContext());

    // When/Then:
    final String expected =
        " > [ SOURCE ] | Schema: [key : VARCHAR, val : BIGINT] | Logger: query.node.source\n";
    assertThat(schemaKtream.getExecutionPlan(""), equalTo(expected));
  }

  @Test
  public void shouldSummarizeExecutionPlanCorrectlyWhenMultipleParents() {
    // Given:
    final SchemaKStream parentSchemaKStream1 = mock(SchemaKStream.class);
    when(parentSchemaKStream1.getExecutionPlan(anyString()))
        .thenReturn("parent 1 plan");
    final SchemaKStream parentSchemaKStream2 = mock(SchemaKStream.class);
    when(parentSchemaKStream2.getExecutionPlan(anyString()))
        .thenReturn("parent 2 plan");
    final SchemaKStream schemaKtream = new SchemaKStream(
        simpleSchema,
        mock(KStream.class),
        simpleSchema.field("key"),
        ImmutableList.of(parentSchemaKStream1, parentSchemaKStream2),
        Serdes.String(),
        Type.SOURCE,
        ksqlConfig,
        functionRegistry,
        queryContext.push("source").getQueryContext());

    // When/Then:
    final String expected =
        " > [ SOURCE ] | Schema: [key : VARCHAR, val : BIGINT] | Logger: query.node.source\n"
            + "\tparent 1 plan"
            + "\tparent 2 plan";
    assertThat(schemaKtream.getExecutionPlan(""), equalTo(expected));
  }

  private SchemaKStream buildSchemaKStream(
      final KsqlStream ksqlStream,
      final Schema schema,
      final KStream kStream,
      final StreamsFactories streamsFactories) {
    return new SchemaKStream(
        schema,
        kStream,
        ksqlStream.getKeyField(),
        new ArrayList<>(),
        Serdes.String(),
        Type.SOURCE,
        ksqlConfig,
        functionRegistry,
        streamsFactories,
        parentContext);
  }

  private SchemaKStream buildSchemaKStream(
      final KStream kStream,
      final GroupedFactory groupedFactory,
      final JoinedFactory joinedFactory) {
    return buildSchemaKStream(
        ksqlStream,
        SchemaUtil.buildSchemaWithAlias(ksqlStream.getSchema(),
            ksqlStream.getName()),
        kStream,
        new StreamsFactories(groupedFactory, joinedFactory, mock(MaterializedFactory.class)));
  }

  private SchemaKStream buildSchemaKStream(final Schema schema) {
    return buildSchemaKStream(
        ksqlStream,
        schema,
        kStream,
        StreamsFactories.create(ksqlConfig));
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
        new StreamsFactories(groupedFactory, joinedFactory, mock(MaterializedFactory.class)));
  }

  private static Schema getJoinSchema(final Schema leftSchema, final Schema rightSchema) {
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
