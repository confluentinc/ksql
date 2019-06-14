/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.structured;

import static io.confluent.ksql.metastore.model.MetaStoreMatchers.FieldMatchers.hasIndex;
import static io.confluent.ksql.metastore.model.MetaStoreMatchers.FieldMatchers.hasName;
import static io.confluent.ksql.metastore.model.MetaStoreMatchers.KeyFieldMatchers.hasLegacyName;
import static io.confluent.ksql.metastore.model.MetaStoreMatchers.KeyFieldMatchers.hasLegacySchema;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
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
import io.confluent.ksql.logging.processing.ProcessingLogContext;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.model.KeyField;
import io.confluent.ksql.metastore.model.KsqlStream;
import io.confluent.ksql.metastore.model.KsqlTable;
import io.confluent.ksql.metastore.model.KsqlTopic;
import io.confluent.ksql.metastore.model.MetaStoreMatchers.KeyFieldMatchers;
import io.confluent.ksql.metastore.model.MetaStoreMatchers.OptionalMatchers;
import io.confluent.ksql.parser.tree.DereferenceExpression;
import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.parser.tree.FunctionCall;
import io.confluent.ksql.parser.tree.QualifiedName;
import io.confluent.ksql.parser.tree.QualifiedNameReference;
import io.confluent.ksql.planner.plan.FilterNode;
import io.confluent.ksql.planner.plan.PlanNode;
import io.confluent.ksql.planner.plan.ProjectNode;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.PhysicalSchema;
import io.confluent.ksql.serde.GenericRowSerDe;
import io.confluent.ksql.serde.SerdeOption;
import io.confluent.ksql.serde.json.KsqlJsonSerdeFactory;
import io.confluent.ksql.streams.GroupedFactory;
import io.confluent.ksql.streams.JoinedFactory;
import io.confluent.ksql.streams.MaterializedFactory;
import io.confluent.ksql.streams.StreamsFactories;
import io.confluent.ksql.streams.StreamsUtil;
import io.confluent.ksql.structured.SchemaKStream.Type;
import io.confluent.ksql.testutils.AnalysisTestUtil;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.MetaStoreFixture;
import io.confluent.ksql.util.SchemaUtil;
import io.confluent.ksql.util.SelectExpression;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
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
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@SuppressWarnings("unchecked")
@RunWith(MockitoJUnitRunner.class)
public class SchemaKStreamTest {

  private static final Expression COL1 = new DereferenceExpression(
      new QualifiedNameReference(QualifiedName.of("TEST1")), "COL1");

  private final MockSchemaRegistryClient schemaRegistryClient = new MockSchemaRegistryClient();
  private SchemaKStream initialSchemaKStream;

  private final KsqlConfig ksqlConfig = new KsqlConfig(Collections.emptyMap());
  private final MetaStore metaStore = MetaStoreFixture.getNewMetaStore(new InternalFunctionRegistry());

  private final Grouped grouped = Grouped.with(
      "group", Serdes.String(), Serdes.String());
  private final Joined joined = Joined.with(
      Serdes.String(), Serdes.String(), Serdes.String(), "join");

  private final KeyField validJoinKeyField = KeyField.of(
      Optional.of("left.COL1"),
      metaStore.getSource("TEST1")
          .getKeyField()
          .legacy()
          .map(field -> SchemaUtil.buildAliasedField("left", field)));

  private KStream kStream;
  private KsqlStream<?> ksqlStream;
  private InternalFunctionRegistry functionRegistry;
  private SchemaKStream secondSchemaKStream;
  private SchemaKTable schemaKTable;
  private Serde<GenericRow> leftSerde;
  private Serde<GenericRow> rightSerde;
  private LogicalSchema joinSchema;
  private Serde<GenericRow> rowSerde;
  private final LogicalSchema simpleSchema = LogicalSchema.of(SchemaBuilder.struct()
      .field("key", Schema.OPTIONAL_STRING_SCHEMA)
      .field("val", Schema.OPTIONAL_INT64_SCHEMA)
      .build());
  private final QueryContext.Stacker queryContext
      = new QueryContext.Stacker(new QueryId("query")).push("node");
  private final QueryContext parentContext = queryContext.push("parent").getQueryContext();
  private final QueryContext.Stacker childContextStacker = queryContext.push("child");
  private final ProcessingLogContext processingLogContext = ProcessingLogContext.create();

  @Mock
  private GroupedFactory mockGroupedFactory;
  @Mock
  private JoinedFactory mockJoinedFactory;
  @Mock
  private KStream mockKStream;

  @Before
  public void init() {
    functionRegistry = new InternalFunctionRegistry();
    ksqlStream = (KsqlStream) metaStore.getSource("TEST1");
    final StreamsBuilder builder = new StreamsBuilder();
    kStream = builder.stream(
        ksqlStream.getKsqlTopic().getKafkaTopicName(),
        Consumed.with(
            Serdes.String(),
          getRowSerde(ksqlStream.getKsqlTopic(), ksqlStream.getSchema().valueSchema())
        ));

    when(mockGroupedFactory.create(anyString(), any(StringSerde.class), any(Serde.class)))
        .thenReturn(grouped);

    final KsqlStream secondKsqlStream = (KsqlStream) metaStore.getSource("ORDERS");
    final KStream secondKStream = builder
        .stream(
            secondKsqlStream.getKsqlTopic().getKafkaTopicName(),
            Consumed.with(
                Serdes.String(),
                getRowSerde(secondKsqlStream.getKsqlTopic(), secondKsqlStream.getSchema().valueSchema())
            ));

    final KsqlTable<?> ksqlTable = (KsqlTable) metaStore.getSource("TEST2");
    final KTable kTable = builder.table(ksqlTable.getKsqlTopic().getKafkaTopicName(),
        Consumed.with(
            Serdes.String(),
            getRowSerde(ksqlTable.getKsqlTopic(), ksqlTable.getSchema().valueSchema())));

    secondSchemaKStream = buildSchemaKStreamForJoin(secondKsqlStream, secondKStream);

    leftSerde = getRowSerde(ksqlStream.getKsqlTopic(), ksqlStream.getSchema().valueSchema());
    rightSerde = getRowSerde(secondKsqlStream.getKsqlTopic(), secondKsqlStream.getSchema().valueSchema());

    schemaKTable = new SchemaKTable<>(
        ksqlTable.getSchema(),
        kTable,
        ksqlTable.getKeyField(),
        new ArrayList<>(),
        Serdes::String,
        SchemaKStream.Type.SOURCE,
        ksqlConfig,
        functionRegistry,
        parentContext);

    joinSchema = getJoinSchema(ksqlStream.getSchema(), secondKsqlStream.getSchema());

    whenCreateJoined();
  }

  private static Serde<GenericRow> getRowSerde(final KsqlTopic topic, final Schema schema) {
    return GenericRowSerDe.from(
        topic.getValueSerdeFactory(),
        PhysicalSchema.from(LogicalSchema.of(schema), SerdeOption.none()),
        new KsqlConfig(Collections.emptyMap()),
        MockSchemaRegistryClient::new,
        "test",
        ProcessingLogContext.create());
  }

  @Test
  public void testSelectSchemaKStream() {
    // Given:
    final PlanNode logicalPlan = givenInitialKStreamOf(
        "SELECT col0, col2, col3 FROM test1 WHERE col0 > 100;");

    final ProjectNode projectNode = (ProjectNode) logicalPlan.getSources().get(0);

    final List<SelectExpression> selectExpressions = projectNode.getProjectSelectExpressions();

    // When:
    final SchemaKStream projectedSchemaKStream = initialSchemaKStream.select(
        selectExpressions,
        childContextStacker,
        processingLogContext);

    // Then:
    assertThat(projectedSchemaKStream.getSchema().valueFields(), contains(
        new Field("COL0", 0, Schema.OPTIONAL_INT64_SCHEMA),
        new Field("COL2", 1, Schema.OPTIONAL_STRING_SCHEMA),
        new Field("COL3", 2, Schema.OPTIONAL_FLOAT64_SCHEMA)
    ));

    assertThat(projectedSchemaKStream.getSourceSchemaKStreams().get(0), is(initialSchemaKStream));
  }

  @Test
  public void shouldUpdateKeyIfRenamed() {
    // Given:
    final PlanNode logicalPlan = givenInitialKStreamOf(
        "SELECT col0 as NEWKEY, col2, col3 FROM test1;");
    final ProjectNode projectNode = (ProjectNode) logicalPlan.getSources().get(0);
    final List<SelectExpression> selectExpressions = projectNode.getProjectSelectExpressions();

    // When:
    final SchemaKStream result = initialSchemaKStream
        .select( selectExpressions, childContextStacker, processingLogContext);

    // Then:
    assertThat(result.getKeyField(),
        is(KeyField.of("NEWKEY", new Field("NEWKEY", 0, Schema.OPTIONAL_INT64_SCHEMA))));
  }

  @Test
  public void shouldUpdateKeyIfRenamedViaFullyQualifiedName() {
    // Given:
    final PlanNode logicalPlan = givenInitialKStreamOf(
        "SELECT test1.col0 as NEWKEY, col2, col3 FROM test1;");
    final ProjectNode projectNode = (ProjectNode) logicalPlan.getSources().get(0);
    final List<SelectExpression> selectExpressions = projectNode.getProjectSelectExpressions();

    // When:
    final SchemaKStream result = initialSchemaKStream
        .select(selectExpressions, childContextStacker, processingLogContext);

    // Then:
    assertThat(result.getKeyField(),
        is(KeyField.of("NEWKEY", new Field("NEWKEY", 0, Schema.OPTIONAL_INT64_SCHEMA))));
  }

  @Test
  public void shouldUpdateKeyIfRenamedAndSourceIsAliased() {
    // Given:
    final PlanNode logicalPlan = givenInitialKStreamOf(
        "SELECT t.col0 as NEWKEY, col2, col3 FROM test1 t;");
    final ProjectNode projectNode = (ProjectNode) logicalPlan.getSources().get(0);
    final List<SelectExpression> selectExpressions = projectNode.getProjectSelectExpressions();

    // When:
    final SchemaKStream result = initialSchemaKStream
        .select(selectExpressions, childContextStacker, processingLogContext);

    // Then:
    assertThat(result.getKeyField(),
        is(KeyField.of("NEWKEY", new Field("NEWKEY", 0, Schema.OPTIONAL_INT64_SCHEMA))));
  }

  @Test
  public void shouldPreserveKeyOnSelectStar() {
    // Given:
    final PlanNode logicalPlan = givenInitialKStreamOf("SELECT * FROM test1;");
    final ProjectNode projectNode = (ProjectNode) logicalPlan.getSources().get(0);
    final List<SelectExpression> selectExpressions = projectNode.getProjectSelectExpressions();

    // When:
    final SchemaKStream result = initialSchemaKStream
        .select(selectExpressions, childContextStacker, processingLogContext);

    // Then:
    assertThat(result.getKeyField(), KeyFieldMatchers.hasName("COL0"));
    assertThat(result.getKeyField(), hasLegacyName(initialSchemaKStream.keyField.legacy().map(Field::name)));
    assertThat(result.getKeyField(), hasLegacySchema(initialSchemaKStream.keyField.legacy().map(Field::schema)));
  }

  @Test
  public void shouldUpdateKeyIfMovedToDifferentIndex() {
    // Given:
    final PlanNode logicalPlan = givenInitialKStreamOf("SELECT col2, col0, col3 FROM test1;");
    final ProjectNode projectNode = (ProjectNode) logicalPlan.getSources().get(0);
    final List<SelectExpression> selectExpressions = projectNode.getProjectSelectExpressions();

    // When:
    final SchemaKStream result = initialSchemaKStream
        .select(selectExpressions, childContextStacker, processingLogContext);

    // Then:
    assertThat(result.getKeyField(),
        equalTo(KeyField.of("COL0", new Field("COL0", 1, Schema.OPTIONAL_INT64_SCHEMA))));
  }

  @Test
  public void shouldDropKeyIfNotSelected() {
    // Given:
    final PlanNode logicalPlan = givenInitialKStreamOf("SELECT col2, col3 FROM test1;");
    final ProjectNode projectNode = (ProjectNode) logicalPlan.getSources().get(0);
    final List<SelectExpression> selectExpressions = projectNode.getProjectSelectExpressions();

    // When:
    final SchemaKStream result = initialSchemaKStream
        .select(selectExpressions, childContextStacker, processingLogContext);

    // Then:
    assertThat(result.getKeyField(), is(KeyField.none()));
  }

  @Test
  public void shouldHandleSourceWithoutKey() {
    // Given:
    final PlanNode logicalPlan = givenInitialKStreamOf("SELECT * FROM test4;");
    final ProjectNode projectNode = (ProjectNode) logicalPlan.getSources().get(0);
    final List<SelectExpression> selectExpressions = projectNode.getProjectSelectExpressions();

    // When:
    final SchemaKStream result = initialSchemaKStream
        .select(selectExpressions, childContextStacker, processingLogContext);

    // Then:
    assertThat(result.getKeyField(), is(KeyField.none()));
  }

  @Test
  public void testSelectWithExpression() {
    // Given:
    final PlanNode logicalPlan = givenInitialKStreamOf(
        "SELECT col0, LEN(UCASE(col2)), col3*3+5 FROM test1 WHERE col0 > 100;");
    final ProjectNode projectNode = (ProjectNode) logicalPlan.getSources().get(0);

    // When:
    final SchemaKStream projectedSchemaKStream = initialSchemaKStream.select(
        projectNode.getProjectSelectExpressions(),
        childContextStacker,
        processingLogContext);

    // Then:
    assertThat(projectedSchemaKStream.getSchema().valueFields(), contains(
        new Field("COL0", 0, Schema.OPTIONAL_INT64_SCHEMA),
        new Field("KSQL_COL_1", 1, Schema.OPTIONAL_INT32_SCHEMA),
        new Field("KSQL_COL_2", 2, Schema.OPTIONAL_FLOAT64_SCHEMA)
    ));

    assertThat(projectedSchemaKStream.getSourceSchemaKStreams().get(0), is(initialSchemaKStream));
  }

  @Test
  public void testFilter() {
    // Given:
    final PlanNode logicalPlan = givenInitialKStreamOf(
        "SELECT col0, col2, col3 FROM test1 WHERE col0 > 100;");
    final FilterNode filterNode = (FilterNode) logicalPlan.getSources().get(0).getSources().get(0);

    // When:
    final SchemaKStream filteredSchemaKStream = initialSchemaKStream.filter(
        filterNode.getPredicate(),
        childContextStacker,
        processingLogContext);

    // Then:
    assertThat(filteredSchemaKStream.getSchema().valueFields(), contains(
        new Field("TEST1.ROWTIME", 0, Schema.OPTIONAL_INT64_SCHEMA),
        new Field("TEST1.ROWKEY", 1, Schema.OPTIONAL_STRING_SCHEMA),
        new Field("TEST1.COL0", 2, Schema.OPTIONAL_INT64_SCHEMA),
        new Field("TEST1.COL1", 3, Schema.OPTIONAL_STRING_SCHEMA),
        new Field("TEST1.COL2", 4, Schema.OPTIONAL_STRING_SCHEMA),
        new Field("TEST1.COL3", 5, Schema.OPTIONAL_FLOAT64_SCHEMA),
        new Field("TEST1.COL4", 6, SchemaBuilder
            .array(Schema.OPTIONAL_FLOAT64_SCHEMA)
            .optional()
            .build()),
        new Field("TEST1.COL5", 7, SchemaBuilder
            .map(Schema.OPTIONAL_STRING_SCHEMA, Schema.OPTIONAL_FLOAT64_SCHEMA)
            .optional()
            .build())
    ));

    assertThat(filteredSchemaKStream.getSourceSchemaKStreams().get(0), is(initialSchemaKStream));
  }

  @Test
  public void shouldSelectKey() {
    // Given:
    givenInitialKStreamOf("SELECT col0, col2, col3 FROM test1 WHERE col0 > 100;");

    final KeyField expected = KeyField.of(
        Optional.of("TEST1.COL1"),
        initialSchemaKStream.getSchema().findValueField("TEST1.COL1")
    );

    // When:
    final SchemaKStream<?> rekeyedSchemaKStream = initialSchemaKStream.selectKey(
        "TEST1.COL1",
        true,
        childContextStacker);

    // Then:
    assertThat(rekeyedSchemaKStream.getKeyField(), is(expected));
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldThrowOnSelectKeyIfKeyNotInSchema() {
    givenInitialKStreamOf("SELECT col0, col2, col3 FROM test1 WHERE col0 > 100;");

    final SchemaKStream<?> rekeyedSchemaKStream = initialSchemaKStream.selectKey(
        "won't find me",
        true,
        childContextStacker);

    assertThat(rekeyedSchemaKStream.getKeyField(), is(validJoinKeyField));
  }

  @Test
  public void testGroupByKey() {
    // Given:
    givenInitialKStreamOf("SELECT col0, col1 FROM test1 WHERE col0 > 100;");

    final List<Expression> groupBy = Collections.singletonList(
        new DereferenceExpression(
            new QualifiedNameReference(QualifiedName.of("TEST1")), "COL0")
    );

    // When:
    final SchemaKGroupedStream groupedSchemaKStream = initialSchemaKStream.groupBy(
        rowSerde,
        groupBy,
        childContextStacker);

    // Then:
    assertThat(groupedSchemaKStream.getKeyField().name(), is(Optional.of("TEST1.COL0")));
    assertThat(groupedSchemaKStream.getKeyField().legacy(), OptionalMatchers.of(hasName("COL0")));
  }

  @Test
  public void testGroupByMultipleColumns() {
    // Given:
    givenInitialKStreamOf("SELECT col0, col1 FROM test1 WHERE col0 > 100;");

    final List<Expression> groupBy = ImmutableList.of(
        new DereferenceExpression(
            new QualifiedNameReference(QualifiedName.of("TEST1")), "COL1"),
        new DereferenceExpression(
            new QualifiedNameReference(QualifiedName.of("TEST1")), "COL0")
    );

    // When:
    final SchemaKGroupedStream groupedSchemaKStream = initialSchemaKStream.groupBy(
        rowSerde,
        groupBy,
        childContextStacker);

    // Then:
    assertThat(groupedSchemaKStream.getKeyField().name(), is(Optional.empty()));
    assertThat(groupedSchemaKStream.getKeyField().legacy(), OptionalMatchers.of(hasName("TEST1.COL1|+|TEST1.COL0")));
  }

  @Test
  public void testGroupByMoreComplexExpression() {
    // Given:
    givenInitialKStreamOf("SELECT col0, col1 FROM test1 WHERE col0 > 100;");

    final Expression groupBy = new FunctionCall(QualifiedName.of("UCASE"), ImmutableList.of(COL1));

    // When:
    final SchemaKGroupedStream groupedSchemaKStream = initialSchemaKStream.groupBy(
        rowSerde,
        ImmutableList.of(groupBy),
        childContextStacker);

    // Then:
    assertThat(groupedSchemaKStream.getKeyField().name(), is(Optional.empty()));
    assertThat(groupedSchemaKStream.getKeyField().legacy(), OptionalMatchers.of(hasName("UCASE(TEST1.COL1)")));
    assertThat(groupedSchemaKStream.getKeyField().legacy(), OptionalMatchers.of(hasIndex(-1)));
  }

  @Test
  public void shouldUseFactoryForGroupedWithoutRekey() {
    // Given:
    final KGroupedStream groupedStream = mock(KGroupedStream.class);
    when(mockKStream.groupByKey(any(Grouped.class))).thenReturn(groupedStream);
    final Expression keyExpression = new DereferenceExpression(
        new QualifiedNameReference(QualifiedName.of(ksqlStream.getName())),
        ksqlStream.getKeyField().name().get());
    final List<Expression> groupByExpressions = Collections.singletonList(keyExpression);
    givenInitialSchemaKStreamUsesMocks();

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
    when(mockKStream.filter(any(Predicate.class))).thenReturn(mockKStream);
    final KGroupedStream groupedStream = mock(KGroupedStream.class);
    when(mockKStream.groupBy(any(KeyValueMapper.class), any(Grouped.class)))
        .thenReturn(groupedStream);
    final Expression col0Expression = new DereferenceExpression(
        new QualifiedNameReference(QualifiedName.of(ksqlStream.getName())), "COL0");
    final Expression col1Expression = new DereferenceExpression(
        new QualifiedNameReference(QualifiedName.of(ksqlStream.getName())), "COL1");
    final List<Expression> groupByExpressions = Arrays.asList(col1Expression, col0Expression);
    givenInitialSchemaKStreamUsesMocks();

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
            validJoinKeyField,
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
    assertThat(joinedKStream.getKeyField(), is(validJoinKeyField));
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
            validJoinKeyField,
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
    Assert.assertThat(joinedKStream.getKeyField(), is(validJoinKeyField));
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
            validJoinKeyField,
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
    assertThat(joinedKStream.getKeyField(), is(validJoinKeyField));
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
            validJoinKeyField,
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
    assertThat(joinedKStream.getKeyField(), is(validJoinKeyField));
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
            validJoinKeyField,
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
    assertThat(joinedKStream.getKeyField(), is(validJoinKeyField));
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
        KeyField.of("key", simpleSchema.valueSchema().field("key")),
        ImmutableList.of(parentSchemaKStream),
        Serdes::String,
        Type.SOURCE,
        ksqlConfig,
        functionRegistry,
        queryContext.push("source").getQueryContext());

    // When/Then:
    final String expected =
        " > [ SOURCE ] | Schema: [key VARCHAR, val BIGINT] | Logger: query.node.source\n\t"
            + "parent plan";
    assertThat(schemaKtream.getExecutionPlan(""), equalTo(expected));
  }

  @Test
  public void shouldSummarizeExecutionPlanCorrectlyForRoot() {
    // Given:
    final SchemaKStream schemaKtream = new SchemaKStream(
        simpleSchema,
        mock(KStream.class),
        KeyField.of("key", simpleSchema.valueSchema().field("key")),
        Collections.emptyList(),
        Serdes::String,
        Type.SOURCE,
        ksqlConfig,
        functionRegistry,
        queryContext.push("source").getQueryContext());

    // When/Then:
    final String expected =
        " > [ SOURCE ] | Schema: [key VARCHAR, val BIGINT] | Logger: query.node.source\n";
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
        KeyField.of("key", simpleSchema.valueSchema().field("key")),
        ImmutableList.of(parentSchemaKStream1, parentSchemaKStream2),
        Serdes::String,
        Type.SOURCE,
        ksqlConfig,
        functionRegistry,
        queryContext.push("source").getQueryContext());

    // When/Then:
    final String expected =
        " > [ SOURCE ] | Schema: [key VARCHAR, val BIGINT] | Logger: query.node.source\n"
            + "\tparent 1 plan"
            + "\tparent 2 plan";
    assertThat(schemaKtream.getExecutionPlan(""), equalTo(expected));
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

  private SchemaKStream buildSchemaKStream(
      final LogicalSchema schema,
      final KeyField keyField,
      final KStream kStream,
      final StreamsFactories streamsFactories) {
    return new SchemaKStream(
        schema,
        kStream,
        keyField,
        new ArrayList<>(),
        Serdes::String,
        Type.SOURCE,
        ksqlConfig,
        functionRegistry,
        streamsFactories,
        parentContext);
  }

  private void givenInitialSchemaKStreamUsesMocks() {
    final LogicalSchema schema = ksqlStream.getSchema().withAlias(ksqlStream.getName());

    final Optional<String> newKeyName = ksqlStream.getKeyField().name()
        .map(name -> SchemaUtil.buildAliasedFieldName(ksqlStream.getName(), name));

    final KeyField keyFieldWithAlias = KeyField.of(newKeyName, ksqlStream.getKeyField().legacy());

    initialSchemaKStream = buildSchemaKStream(
        schema,
        keyFieldWithAlias,
        mockKStream,
        new StreamsFactories(mockGroupedFactory, mockJoinedFactory, mock(MaterializedFactory.class))
    );
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
        ksqlStream.getSchema(),
        ksqlStream.getKeyField(), kStream,
        new StreamsFactories(groupedFactory, joinedFactory, mock(MaterializedFactory.class))
    );
  }

  private static LogicalSchema getJoinSchema(
      final LogicalSchema leftSchema,
      final LogicalSchema rightSchema
  ) {
    final SchemaBuilder schemaBuilder = SchemaBuilder.struct();
    final String leftAlias = "left";
    final String rightAlias = "right";
    for (final Field field : leftSchema.valueFields()) {
      final String fieldName = leftAlias + "." + field.name();
      schemaBuilder.field(fieldName, field.schema());
    }

    for (final Field field : rightSchema.valueFields()) {
      final String fieldName = rightAlias + "." + field.name();
      schemaBuilder.field(fieldName, field.schema());
    }
    return LogicalSchema.of(schemaBuilder.build());
  }

  private PlanNode givenInitialKStreamOf(final String selectQuery) {
    final PlanNode logicalPlan = AnalysisTestUtil.buildLogicalPlan(selectQuery, metaStore);

    initialSchemaKStream = new SchemaKStream(
        logicalPlan.getTheSourceNode().getSchema(),
        kStream,
        logicalPlan.getTheSourceNode().getKeyField(),
        new ArrayList<>(),
        Serdes::String,
        SchemaKStream.Type.SOURCE,
        ksqlConfig,
        functionRegistry,
        queryContext.push("source").getQueryContext());

    rowSerde = GenericRowSerDe.from(
        new KsqlJsonSerdeFactory(),
        PhysicalSchema.from(initialSchemaKStream.getSchema(), SerdeOption.none()),
        null,
        () -> null,
        "test",
        processingLogContext);

    return logicalPlan;
  }
}
