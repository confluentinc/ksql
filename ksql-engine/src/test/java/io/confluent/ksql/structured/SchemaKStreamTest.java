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

import static io.confluent.ksql.metastore.model.MetaStoreMatchers.KeyFieldMatchers.hasLegacyName;
import static io.confluent.ksql.metastore.model.MetaStoreMatchers.KeyFieldMatchers.hasLegacyType;
import static io.confluent.ksql.metastore.model.MetaStoreMatchers.LegacyFieldMatchers.hasName;
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
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.builder.KsqlQueryBuilder;
import io.confluent.ksql.execution.context.QueryContext;
import io.confluent.ksql.execution.ddl.commands.KsqlTopic;
import io.confluent.ksql.execution.expression.tree.ColumnReferenceExp;
import io.confluent.ksql.execution.expression.tree.ComparisonExpression;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.expression.tree.FunctionCall;
import io.confluent.ksql.execution.expression.tree.LongLiteral;
import io.confluent.ksql.execution.streams.StreamJoinedFactory;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.FunctionName;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.schema.ksql.ColumnRef;
import io.confluent.ksql.execution.expression.tree.ColumnReferenceExp;
import io.confluent.ksql.execution.plan.DefaultExecutionStepProperties;
import io.confluent.ksql.execution.plan.PlanBuilder;
import io.confluent.ksql.execution.plan.ExecutionStep;
import io.confluent.ksql.execution.plan.ExecutionStepProperties;
import io.confluent.ksql.execution.plan.Formats;
import io.confluent.ksql.execution.plan.JoinType;
import io.confluent.ksql.execution.plan.KeySerdeFactory;
import io.confluent.ksql.execution.plan.KStreamHolder;
import io.confluent.ksql.execution.plan.KTableHolder;
import io.confluent.ksql.execution.plan.SelectExpression;
import io.confluent.ksql.execution.plan.StreamFilter;
import io.confluent.ksql.execution.streams.AggregateParams;
import io.confluent.ksql.execution.streams.ExecutionStepFactory;
import io.confluent.ksql.execution.streams.GroupedFactory;
import io.confluent.ksql.execution.streams.JoinedFactory;
import io.confluent.ksql.execution.streams.KSPlanBuilder;
import io.confluent.ksql.execution.streams.KsqlValueJoiner;
import io.confluent.ksql.execution.streams.MaterializedFactory;
import io.confluent.ksql.execution.streams.SqlPredicateFactory;
import io.confluent.ksql.execution.streams.StreamsUtil;
import io.confluent.ksql.function.InternalFunctionRegistry;
import io.confluent.ksql.logging.processing.ProcessingLogContext;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.model.KeyField;
import io.confluent.ksql.metastore.model.KeyField.LegacyField;
import io.confluent.ksql.metastore.model.KsqlStream;
import io.confluent.ksql.metastore.model.KsqlTable;
import io.confluent.ksql.metastore.model.MetaStoreMatchers.KeyFieldMatchers;
import io.confluent.ksql.metastore.model.MetaStoreMatchers.OptionalMatchers;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.FunctionName;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.planner.plan.FilterNode;
import io.confluent.ksql.planner.plan.PlanNode;
import io.confluent.ksql.planner.plan.ProjectNode;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.schema.ksql.Column;
import io.confluent.ksql.schema.ksql.ColumnRef;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.PersistenceSchema;
import io.confluent.ksql.schema.ksql.PhysicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.serde.Format;
import io.confluent.ksql.serde.FormatInfo;
import io.confluent.ksql.serde.GenericRowSerDe;
import io.confluent.ksql.serde.KeyFormat;
import io.confluent.ksql.serde.KeySerde;
import io.confluent.ksql.serde.SerdeOption;
import io.confluent.ksql.serde.ValueFormat;
import io.confluent.ksql.execution.streams.StreamsFactories;
import io.confluent.ksql.structured.SchemaKStream.Type;
import io.confluent.ksql.testutils.AnalysisTestUtil;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.MetaStoreFixture;
import io.confluent.ksql.util.Pair;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Schema;
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
import org.apache.kafka.streams.kstream.StreamJoined;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@SuppressWarnings({"unchecked", "OptionalGetWithoutIsPresent"})
@RunWith(MockitoJUnitRunner.class)
public class SchemaKStreamTest {

  private static final SourceName TEST1 = SourceName.of("TEST1");
  private static final Expression COL1 =
      new ColumnReferenceExp(ColumnRef.of(TEST1, ColumnName.of("COL1")));

  private SchemaKStream initialSchemaKStream;

  private final KsqlConfig ksqlConfig = new KsqlConfig(Collections.emptyMap());
  private final MetaStore metaStore = MetaStoreFixture.getNewMetaStore(new InternalFunctionRegistry());

  private final Grouped grouped = Grouped.with(
      "group", Serdes.String(), Serdes.String());
  private final Joined joined = Joined.with(
      Serdes.String(), Serdes.String(), Serdes.String(), "join");
  private final StreamJoined streamJoined = StreamJoined.with(
      Serdes.String(), Serdes.String(), Serdes.String()).withName("join");
  private final KeyField validJoinKeyField = KeyField.of(
      Optional.of(ColumnRef.of(SourceName.of("left"), ColumnName.of("COL1"))),
      metaStore.getSource(SourceName.of("TEST1"))
          .getKeyField()
          .legacy()
          .map(field -> field.withSource(SourceName.of("left"))));

  private KStream secondKStream;
  private KTable kTable;
  private KsqlStream<?> ksqlStream;
  private InternalFunctionRegistry functionRegistry;
  private SchemaKStream secondSchemaKStream;
  private SchemaKTable schemaKTable;
  private Serde<GenericRow> leftSerde;
  private Serde<GenericRow> rightSerde;
  private LogicalSchema joinSchema;
  private final KeyFormat keyFormat = KeyFormat.nonWindowed(FormatInfo.of(Format.KAFKA));
  private final ValueFormat valueFormat = ValueFormat.of(FormatInfo.of(Format.JSON));
  private final ValueFormat rightFormat = ValueFormat.of(FormatInfo.of(Format.DELIMITED));
  private final LogicalSchema simpleSchema = LogicalSchema.builder()
      .valueColumn(ColumnName.of("key"), SqlTypes.STRING)
      .valueColumn(ColumnName.of("val"), SqlTypes.BIGINT)
      .build();
  private final QueryContext.Stacker queryContext
      = new QueryContext.Stacker().push("node");
  private final QueryContext.Stacker childContextStacker = queryContext.push("child");
  private final ProcessingLogContext processingLogContext = ProcessingLogContext.create();
  private PlanBuilder planBuilder;

  @Mock
  private GroupedFactory mockGroupedFactory;
  @Mock
  private JoinedFactory mockJoinedFactory;
  @Mock
  private MaterializedFactory mockMaterializedFactory;
  @Mock
  private StreamJoinedFactory mockStreamJoinedFactory;
  @Mock
  private KStream mockKStream;
  @Mock
  private KeySerde keySerde;
  @Mock
  private KeySerde reboundKeySerde;
  @Mock
  private ExecutionStepProperties tableSourceProperties;
  @Mock
  private ExecutionStep tableSourceStep;
  @Mock
  private ExecutionStepProperties sourceProperties;
  @Mock
  private ExecutionStep sourceStep;
  @Mock
  private KsqlQueryBuilder queryBuilder;
  @Mock
  private KeySerdeFactory keySerdeFactory;

  @Before
  public void init() {
    functionRegistry = new InternalFunctionRegistry();
    ksqlStream = (KsqlStream) metaStore.getSource(SourceName.of("TEST1"));
    final StreamsBuilder builder = new StreamsBuilder();

    when(mockGroupedFactory.create(anyString(), any(Serde.class), any(Serde.class)))
        .thenReturn(grouped);
    when(mockJoinedFactory.create(any(), any(), any(), anyString())).thenReturn(joined);
    when(mockStreamJoinedFactory.create(any(), any(), any(), anyString(), anyString())).thenReturn(streamJoined);

    final KsqlStream secondKsqlStream = (KsqlStream) metaStore.getSource(SourceName.of("ORDERS"));
    secondKStream = builder
        .stream(
            secondKsqlStream.getKsqlTopic().getKafkaTopicName(),
            Consumed.with(
                Serdes.String(),
                getRowSerde(secondKsqlStream.getKsqlTopic(), secondKsqlStream.getSchema().valueConnectSchema())
            ));

    final KsqlTable<?> ksqlTable = (KsqlTable) metaStore.getSource(SourceName.of("TEST2"));
    kTable = builder.table(ksqlTable.getKsqlTopic().getKafkaTopicName(),
        Consumed.with(
            Serdes.String(),
            getRowSerde(ksqlTable.getKsqlTopic(), ksqlTable.getSchema().valueConnectSchema())));

    when(tableSourceStep.getProperties()).thenReturn(tableSourceProperties);
    when(tableSourceProperties.getSchema()).thenReturn(ksqlTable.getSchema());
    when(sourceStep.getProperties()).thenReturn(sourceProperties);

    secondSchemaKStream = buildSchemaKStreamForJoin(
        secondKsqlStream,
        mock(ExecutionStep.class),
        secondKStream
    );

    leftSerde = getRowSerde(ksqlStream.getKsqlTopic(), ksqlStream.getSchema().valueConnectSchema());
    rightSerde = getRowSerde(secondKsqlStream.getKsqlTopic(), secondKsqlStream.getSchema().valueConnectSchema());

    when(tableSourceStep.build(any())).thenReturn(
        new KTableHolder(kTable, keySerdeFactory)
    );
    schemaKTable = new SchemaKTable(
        tableSourceStep,
        keyFormat,
        keySerde,
        ksqlTable.getKeyField(),
        new ArrayList<>(),
        SchemaKStream.Type.SOURCE,
        ksqlConfig,
        functionRegistry);

    joinSchema = getJoinSchema(ksqlStream.getSchema(), secondKsqlStream.getSchema());

    when(keySerdeFactory.buildKeySerde(any(), any(), any())).thenReturn(keySerde);
    when(keySerde.rebind(any(PersistenceSchema.class))).thenReturn(reboundKeySerde);

    when(queryBuilder.getQueryId()).thenReturn(new QueryId("query"));
    when(queryBuilder.getFunctionRegistry()).thenReturn(functionRegistry);
    when(queryBuilder.getKsqlConfig()).thenReturn(ksqlConfig);
    when(queryBuilder.getProcessingLogContext()).thenReturn(processingLogContext);

    whenCreateJoined();

    planBuilder = new KSPlanBuilder(
        queryBuilder,
        mock(SqlPredicateFactory.class),
        mock(AggregateParams.Factory.class),
        new StreamsFactories(mockGroupedFactory, mockJoinedFactory, mockMaterializedFactory, mockStreamJoinedFactory)
    );
  }

  private static Serde<GenericRow> getRowSerde(final KsqlTopic topic, final Schema schema) {
    return GenericRowSerDe.from(
        topic.getValueFormat().getFormatInfo(),
        PersistenceSchema.from((ConnectSchema)schema, false),
        new KsqlConfig(Collections.emptyMap()),
        MockSchemaRegistryClient::new,
        "test",
        ProcessingLogContext.create());
  }

  @Test
  public void testSelectSchemaKStream() {
    // Given:
    final PlanNode logicalPlan = givenInitialKStreamOf(
        "SELECT col0, col2, col3 FROM test1 WHERE col0 > 100 EMIT CHANGES;");

    final ProjectNode projectNode = (ProjectNode) logicalPlan.getSources().get(0);

    final List<SelectExpression> selectExpressions = projectNode.getProjectSelectExpressions();

    // When:
    final SchemaKStream projectedSchemaKStream = initialSchemaKStream.select(
        selectExpressions,
        childContextStacker,
        queryBuilder);

    // Then:
    assertThat(projectedSchemaKStream.getSchema().value(), contains(
        Column.of(ColumnName.of("COL0"), SqlTypes.BIGINT),
        Column.of(ColumnName.of("COL2"), SqlTypes.STRING),
        Column.of(ColumnName.of("COL3"), SqlTypes.DOUBLE)
    ));

    assertThat(projectedSchemaKStream.getSourceSchemaKStreams().get(0), is(initialSchemaKStream));
  }

  @Test
  public void shouldBuildStepForSelect() {
    // Given:
    final PlanNode logicalPlan = givenInitialKStreamOf(
        "SELECT col0, col2, col3 FROM test1 WHERE col0 > 100 EMIT CHANGES;");
    final ProjectNode projectNode = (ProjectNode) logicalPlan.getSources().get(0);
    final List<SelectExpression> selectExpressions = projectNode.getProjectSelectExpressions();

    // When:
    final SchemaKStream projectedSchemaKStream = initialSchemaKStream.select(
        selectExpressions,
        childContextStacker,
        queryBuilder);

    // Then:
    assertThat(
        projectedSchemaKStream.getSourceStep(),
        equalTo(
            ExecutionStepFactory.streamMapValues(
                childContextStacker,
                initialSchemaKStream.getSourceStep(),
                selectExpressions,
                queryBuilder
            )
        )
    );
  }

  @Test
  public void shouldUpdateKeyIfRenamed() {
    // Given:
    final PlanNode logicalPlan = givenInitialKStreamOf(
        "SELECT col0 as NEWKEY, col2, col3 FROM test1 EMIT CHANGES;");
    final ProjectNode projectNode = (ProjectNode) logicalPlan.getSources().get(0);
    final List<SelectExpression> selectExpressions = projectNode.getProjectSelectExpressions();

    // When:
    final SchemaKStream result = initialSchemaKStream
        .select( selectExpressions, childContextStacker, queryBuilder);

    // Then:
    assertThat(result.getKeyField(),
        is(KeyField.of(
            ColumnRef.withoutSource(ColumnName.of("NEWKEY")),
            Column.of(ColumnName.of("NEWKEY"), SqlTypes.BIGINT))));
  }

  @Test
  public void shouldUpdateKeyIfRenamedViaFullyQualifiedName() {
    // Given:
    final PlanNode logicalPlan = givenInitialKStreamOf(
        "SELECT test1.col0 as NEWKEY, col2, col3 FROM test1 EMIT CHANGES;");
    final ProjectNode projectNode = (ProjectNode) logicalPlan.getSources().get(0);
    final List<SelectExpression> selectExpressions = projectNode.getProjectSelectExpressions();

    // When:
    final SchemaKStream result = initialSchemaKStream
        .select(selectExpressions, childContextStacker, queryBuilder);

    // Then:
    assertThat(result.getKeyField(),
        is(KeyField.of(
            ColumnRef.withoutSource(ColumnName.of("NEWKEY")),
            Column.of(ColumnName.of("NEWKEY"), SqlTypes.BIGINT))));
  }

  @Test
  public void shouldUpdateKeyIfRenamedAndSourceIsAliased() {
    // Given:
    final PlanNode logicalPlan = givenInitialKStreamOf(
        "SELECT t.col0 as NEWKEY, col2, col3 FROM test1 t EMIT CHANGES;");
    final ProjectNode projectNode = (ProjectNode) logicalPlan.getSources().get(0);
    final List<SelectExpression> selectExpressions = projectNode.getProjectSelectExpressions();

    // When:
    final SchemaKStream result = initialSchemaKStream
        .select(selectExpressions, childContextStacker, queryBuilder);

    // Then:
    assertThat(result.getKeyField(),
        is(KeyField.of(
            ColumnRef.withoutSource(ColumnName.of("NEWKEY")),
            Column.of(ColumnName.of("NEWKEY"), SqlTypes.BIGINT))));
  }

  @Test
  public void shouldPreserveKeyOnSelectStar() {
    // Given:
    final PlanNode logicalPlan = givenInitialKStreamOf("SELECT * FROM test1 EMIT CHANGES;");
    final ProjectNode projectNode = (ProjectNode) logicalPlan.getSources().get(0);
    final List<SelectExpression> selectExpressions = projectNode.getProjectSelectExpressions();

    // When:
    final SchemaKStream result = initialSchemaKStream
        .select(selectExpressions, childContextStacker, queryBuilder);

    // Then:
    assertThat(result.getKeyField(), KeyFieldMatchers.hasName("COL0"));
    assertThat(result.getKeyField(), hasLegacyName(initialSchemaKStream.keyField.legacy().map(lf -> lf.columnRef.aliasedFieldName())));
    assertThat(result.getKeyField(), hasLegacyType(initialSchemaKStream.keyField.legacy().map(LegacyField::type)));
  }

  @Test
  public void shouldUpdateKeyIfMovedToDifferentIndex() {
    // Given:
    final PlanNode logicalPlan = givenInitialKStreamOf("SELECT col2, col0, col3 FROM test1 EMIT CHANGES;");
    final ProjectNode projectNode = (ProjectNode) logicalPlan.getSources().get(0);
    final List<SelectExpression> selectExpressions = projectNode.getProjectSelectExpressions();

    // When:
    final SchemaKStream result = initialSchemaKStream
        .select(selectExpressions, childContextStacker, queryBuilder);

    // Then:
    assertThat(result.getKeyField(),
        equalTo(KeyField.of(
            ColumnRef.withoutSource(ColumnName.of("COL0")),
            Column.of(ColumnName.of("COL0"), SqlTypes.BIGINT))));
  }

  @Test
  public void shouldDropKeyIfNotSelected() {
    // Given:
    final PlanNode logicalPlan = givenInitialKStreamOf("SELECT col2, col3 FROM test1 EMIT CHANGES;");
    final ProjectNode projectNode = (ProjectNode) logicalPlan.getSources().get(0);
    final List<SelectExpression> selectExpressions = projectNode.getProjectSelectExpressions();

    // When:
    final SchemaKStream result = initialSchemaKStream
        .select(selectExpressions, childContextStacker, queryBuilder);

    // Then:
    assertThat(result.getKeyField(), is(KeyField.none()));
  }

  @Test
  public void shouldHandleSourceWithoutKey() {
    // Given:
    final PlanNode logicalPlan = givenInitialKStreamOf("SELECT * FROM test4 EMIT CHANGES;");
    final ProjectNode projectNode = (ProjectNode) logicalPlan.getSources().get(0);
    final List<SelectExpression> selectExpressions = projectNode.getProjectSelectExpressions();

    // When:
    final SchemaKStream result = initialSchemaKStream
        .select(selectExpressions, childContextStacker, queryBuilder);

    // Then:
    assertThat(result.getKeyField(), is(KeyField.none()));
  }

  @Test
  public void testSelectWithExpression() {
    // Given:
    final PlanNode logicalPlan = givenInitialKStreamOf(
        "SELECT col0, LEN(UCASE(col2)), col3*3+5 FROM test1 WHERE col0 > 100 EMIT CHANGES;");
    final ProjectNode projectNode = (ProjectNode) logicalPlan.getSources().get(0);

    // When:
    final SchemaKStream projectedSchemaKStream = initialSchemaKStream.select(
        projectNode.getProjectSelectExpressions(),
        childContextStacker,
        queryBuilder);

    // Then:
    assertThat(projectedSchemaKStream.getSchema().value(), contains(
        Column.of(ColumnName.of("COL0"), SqlTypes.BIGINT),
        Column.of(ColumnName.of("KSQL_COL_1"), SqlTypes.INTEGER),
        Column.of(ColumnName.of("KSQL_COL_2"), SqlTypes.DOUBLE)
    ));

    assertThat(projectedSchemaKStream.getSourceSchemaKStreams().get(0), is(initialSchemaKStream));
  }

  @Test
  public void shouldReturnSchemaKStreamWithCorrectSchemaForFilter() {
    // Given:
    final PlanNode logicalPlan = givenInitialKStreamOf(
        "SELECT col0, col2, col3 FROM test1 WHERE col0 > 100 EMIT CHANGES;");
    final FilterNode filterNode = (FilterNode) logicalPlan.getSources().get(0).getSources().get(0);

    // When:
    final SchemaKStream filteredSchemaKStream = initialSchemaKStream.filter(
        filterNode.getPredicate(),
        childContextStacker,
        queryBuilder);

    // Then:
    assertThat(filteredSchemaKStream.getSchema().value(), contains(
        Column.of(TEST1, ColumnName.of("ROWTIME"), SqlTypes.BIGINT),
        Column.of(TEST1, ColumnName.of("ROWKEY"), SqlTypes.STRING),
        Column.of(TEST1, ColumnName.of("COL0"), SqlTypes.BIGINT),
        Column.of(TEST1, ColumnName.of("COL1"), SqlTypes.STRING),
        Column.of(TEST1, ColumnName.of("COL2"), SqlTypes.STRING),
        Column.of(TEST1, ColumnName.of("COL3"), SqlTypes.DOUBLE),
        Column.of(TEST1, ColumnName.of("COL4"), SqlTypes.array(SqlTypes.DOUBLE)),
        Column.of(TEST1, ColumnName.of("COL5"), SqlTypes.map(SqlTypes.DOUBLE))
    ));

    assertThat(filteredSchemaKStream.getSourceSchemaKStreams().get(0), is(initialSchemaKStream));
  }

  @Test
  public void shouldRewriteTimeComparisonInFilter() {
    // Given:
    final PlanNode logicalPlan = givenInitialKStreamOf(
        "SELECT col0, col2, col3 FROM test1 "
            + "WHERE ROWTIME = '1984-01-01T00:00:00+00:00' EMIT CHANGES;");
    final FilterNode filterNode = (FilterNode) logicalPlan.getSources().get(0).getSources().get(0);

    // When:
    final SchemaKStream filteredSchemaKStream = initialSchemaKStream.filter(
        filterNode.getPredicate(),
        childContextStacker,
        queryBuilder);

    // Then:
    final StreamFilter step = (StreamFilter) filteredSchemaKStream.getSourceStep();
    assertThat(
        step.getFilterExpression(),
        equalTo(
            new ComparisonExpression(
                ComparisonExpression.Type.EQUAL,
                new ColumnReferenceExp(ColumnRef.of(TEST1, ColumnName.of("ROWTIME"))),
                new LongLiteral(441763200000L)
            )
        )
    );
  }

  @Test
  public void shouldBuildStepForFilter() {
    // Given:
    final PlanNode logicalPlan = givenInitialKStreamOf(
        "SELECT col0, col2, col3 FROM test1 WHERE col0 > 100 EMIT CHANGES;");
    final FilterNode filterNode = (FilterNode) logicalPlan.getSources().get(0).getSources().get(0);

    // When:
    final SchemaKStream filteredSchemaKStream = initialSchemaKStream.filter(
        filterNode.getPredicate(),
        childContextStacker,
        queryBuilder);

    // Then:
    assertThat(
        filteredSchemaKStream.getSourceStep(),
        equalTo(
            ExecutionStepFactory.streamFilter(
                childContextStacker,
                initialSchemaKStream.getSourceStep(),
                filterNode.getPredicate()
            )
        )
    );
  }

  @Test
  public void shouldSelectKey() {
    // Given:
    givenInitialKStreamOf("SELECT col0, col2, col3 FROM test1 WHERE col0 > 100 EMIT CHANGES;");

    final KeyField expected = KeyField.of(
        ColumnRef.of(SourceName.of("TEST1"), ColumnName.of("COL1")),
        initialSchemaKStream.getSchema().findValueColumn(ColumnRef.of(SourceName.of("TEST1"), ColumnName.of("COL1"))).get()
    );

    // When:
    final SchemaKStream<?> rekeyedSchemaKStream = initialSchemaKStream.selectKey(
        ColumnRef.of(SourceName.of("TEST1"), ColumnName.of("COL1")),
        true,
        childContextStacker);

    // Then:
    assertThat(rekeyedSchemaKStream.getKeyField(), is(expected));
    assertThat(rekeyedSchemaKStream.getKeySerde(), is(reboundKeySerde));
  }

  @Test
  public void shouldBuildStepForSelectKey() {
    // Given:
    givenInitialKStreamOf("SELECT col0, col2, col3 FROM test1 WHERE col0 > 100 EMIT CHANGES;");

    // When:
    final SchemaKStream<?> rekeyedSchemaKStream = initialSchemaKStream.selectKey(
        ColumnRef.of(SourceName.of("TEST1"), ColumnName.of("COL1")),
        true,
        childContextStacker);

    // Then:
    assertThat(
        rekeyedSchemaKStream.getSourceStep(),
        equalTo(
            ExecutionStepFactory.streamSelectKey(
                childContextStacker,
                initialSchemaKStream.getSourceStep(),
                ColumnRef.of(SourceName.of("TEST1"), ColumnName.of("COL1")),
                true
            )
        )
    );
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldThrowOnSelectKeyIfKeyNotInSchema() {
    givenInitialKStreamOf("SELECT col0, col2, col3 FROM test1 WHERE col0 > 100 EMIT CHANGES;");

    final SchemaKStream<?> rekeyedSchemaKStream = initialSchemaKStream.selectKey(
        ColumnRef.withoutSource(ColumnName.of("won't find me")),
        true,
        childContextStacker);

    assertThat(rekeyedSchemaKStream.getKeyField(), is(validJoinKeyField));
  }

  @Test
  public void testGroupByKey() {
    // Given:
    givenInitialKStreamOf("SELECT col0, col1 FROM test1 WHERE col0 > 100 EMIT CHANGES;");

    final List<Expression> groupBy = Collections.singletonList(
        new ColumnReferenceExp(ColumnRef.of(TEST1, ColumnName.of("COL0")))
    );

    // When:
    final SchemaKGroupedStream groupedSchemaKStream = initialSchemaKStream.groupBy(
        valueFormat,
        groupBy,
        childContextStacker,
        queryBuilder);

    // Then:
    assertThat(groupedSchemaKStream.getKeyField().ref(), is(Optional.of(ColumnRef.of(SourceName.of("TEST1"), ColumnName.of("COL0")))));
    assertThat(groupedSchemaKStream.getKeyField().legacy(), OptionalMatchers.of(hasName("COL0")));
  }

  @Test
  public void shouldBuildStepForGroupByKey() {
    // Given:
    givenInitialKStreamOf("SELECT col0, col1 FROM test1 WHERE col0 > 100 EMIT CHANGES;");
    final List<Expression> groupBy = Collections.singletonList(
        new ColumnReferenceExp(ColumnRef.of(TEST1, ColumnName.of("COL0")))
    );

    // When:
    final SchemaKGroupedStream groupedSchemaKStream = initialSchemaKStream.groupBy(
        valueFormat,
        groupBy,
        childContextStacker,
        queryBuilder);

    // Then:
    final KeyFormat expectedKeyFormat = KeyFormat.nonWindowed(keyFormat.getFormatInfo());
    assertThat(
        groupedSchemaKStream.getSourceStep(),
        equalTo(
            ExecutionStepFactory.streamGroupByKey(
                childContextStacker,
                initialSchemaKStream.getSourceStep(),
                Formats.of(expectedKeyFormat, valueFormat, SerdeOption.none())
            )
        )
    );
  }

  @Test
  public void shouldBuildStepForGroupBy() {
    // Given:
    givenInitialKStreamOf("SELECT col0, col1 FROM test1 WHERE col0 > 100 EMIT CHANGES;");
    final List<Expression> groupBy = Collections.singletonList(
            new ColumnReferenceExp(ColumnRef.of(TEST1, ColumnName.of("COL1")))
    );

    // When:
    final SchemaKGroupedStream groupedSchemaKStream = initialSchemaKStream.groupBy(
        valueFormat,
        groupBy,
        childContextStacker,
        queryBuilder);

    // Then:
    final KeyFormat expectedKeyFormat = KeyFormat.nonWindowed(keyFormat.getFormatInfo());
    assertThat(
        groupedSchemaKStream.getSourceStep(),
        equalTo(
            ExecutionStepFactory.streamGroupBy(
                childContextStacker,
                initialSchemaKStream.getSourceStep(),
                Formats.of(expectedKeyFormat, valueFormat, SerdeOption.none()),
                groupBy
            )
        )
    );
  }

  @Test
  public void testGroupByMultipleColumns() {
    // Given:
    givenInitialKStreamOf("SELECT col0, col1 FROM test1 WHERE col0 > 100 EMIT CHANGES;");

    final List<Expression> groupBy = ImmutableList.of(
        new ColumnReferenceExp(ColumnRef.of(TEST1, ColumnName.of("COL1"))),
        new ColumnReferenceExp(ColumnRef.of(TEST1, ColumnName.of("COL0")))
    );

    // When:
    final SchemaKGroupedStream groupedSchemaKStream = initialSchemaKStream.groupBy(
        valueFormat,
        groupBy,
        childContextStacker,
        queryBuilder);

    // Then:
    assertThat(groupedSchemaKStream.getKeyField().ref(), is(Optional.empty()));
    assertThat(groupedSchemaKStream.getKeyField().legacy().get().columnRef().name().name(), is("TEST1.COL1|+|TEST1.COL0"));
  }

  @Test
  public void testGroupByMoreComplexExpression() {
    // Given:
    givenInitialKStreamOf("SELECT col0, col1 FROM test1 WHERE col0 > 100 EMIT CHANGES;");

    final Expression groupBy = new FunctionCall(FunctionName.of("UCASE"), ImmutableList.of(COL1));

    // When:
    final SchemaKGroupedStream groupedSchemaKStream = initialSchemaKStream.groupBy(
        valueFormat,
        ImmutableList.of(groupBy),
        childContextStacker,
        queryBuilder);

    // Then:
    assertThat(groupedSchemaKStream.getKeyField().ref(), is(Optional.empty()));
    assertThat(groupedSchemaKStream.getKeyField().legacy().get().columnRef().name().name(), is("UCASE(TEST1.COL1)"));
  }

  @Test
  public void shouldUseFactoryForGroupedWithoutRekey() {
    // Given:
    final KGroupedStream groupedStream = mock(KGroupedStream.class);
    when(mockKStream.groupByKey(any(Grouped.class))).thenReturn(groupedStream);
    final Expression keyExpression = new ColumnReferenceExp(ColumnRef.of(
        ksqlStream.getName(),
        ksqlStream.getKeyField().ref().get().name()
    ));
    final List<Expression> groupByExpressions = Collections.singletonList(keyExpression);
    givenInitialSchemaKStreamUsesMocks();
    when(queryBuilder.buildKeySerde(any(), any(), any())).thenReturn(keySerde);
    when(queryBuilder.buildValueSerde(any(), any(), any())).thenReturn(leftSerde);

    // When:
    SchemaKGroupedStream result = initialSchemaKStream.groupBy(
        valueFormat,
        groupByExpressions,
        childContextStacker,
        queryBuilder);

    // Then:
    result.getSourceStep().build(planBuilder);
    verify(mockGroupedFactory).create(
        eq(StreamsUtil.buildOpName(childContextStacker.getQueryContext())),
        same(keySerde),
        same(leftSerde)
    );
    verify(mockKStream).groupByKey(same(grouped));
    final LogicalSchema logicalSchema = ksqlStream.getSchema().withAlias(ksqlStream.getName());
    verify(queryBuilder).buildKeySerde(
        FormatInfo.of(Format.KAFKA),
        PhysicalSchema.from(logicalSchema, SerdeOption.none()),
        childContextStacker.getQueryContext()
    );
    verify(queryBuilder).buildValueSerde(
        valueFormat.getFormatInfo(),
        PhysicalSchema.from(logicalSchema, SerdeOption.none()),
        childContextStacker.getQueryContext()
    );
  }

  @Test
  public void shouldUseFactoryForGrouped() {
    // Given:
    when(mockKStream.filter(any(Predicate.class))).thenReturn(mockKStream);
    final KGroupedStream groupedStream = mock(KGroupedStream.class);
    when(mockKStream.groupBy(any(KeyValueMapper.class), any(Grouped.class)))
        .thenReturn(groupedStream);
    final Expression col0Expression =
        new ColumnReferenceExp(ColumnRef.of(ksqlStream.getName(), ColumnName.of("COL0")));
    final Expression col1Expression =
        new ColumnReferenceExp(ColumnRef.of(ksqlStream.getName(), ColumnName.of("COL1")));
    final List<Expression> groupByExpressions = Arrays.asList(col1Expression, col0Expression);
    givenInitialSchemaKStreamUsesMocks();
    when(queryBuilder.buildKeySerde(any(), any(), any())).thenReturn(reboundKeySerde);
    when(queryBuilder.buildValueSerde(any(), any(), any())).thenReturn(leftSerde);

    // When:
    final SchemaKGroupedStream result = initialSchemaKStream.groupBy(
        valueFormat,
        groupByExpressions,
        childContextStacker,
        queryBuilder);

    // Then:
    result.getSourceStep().build(planBuilder);
    verify(mockGroupedFactory).create(
        eq(StreamsUtil.buildOpName(childContextStacker.getQueryContext())),
        same(reboundKeySerde),
        same(leftSerde));
    verify(mockKStream).groupBy(any(KeyValueMapper.class), same(grouped));
    final LogicalSchema logicalSchema = ksqlStream.getSchema().withAlias(ksqlStream.getName());
    verify(queryBuilder).buildKeySerde(
        FormatInfo.of(Format.KAFKA),
        PhysicalSchema.from(logicalSchema, SerdeOption.none()),
        childContextStacker.getQueryContext()
    );
    verify(queryBuilder).buildValueSerde(
        valueFormat.getFormatInfo(),
        PhysicalSchema.from(logicalSchema, SerdeOption.none()),
        childContextStacker.getQueryContext()
    );
  }

  @Test
  public void shouldBuildStepForToTable() {
    // Given:
    givenInitialSchemaKStreamUsesMocks();

    // When:
    final SchemaKTable result = initialSchemaKStream.toTable(
        keyFormat,
        valueFormat,
        childContextStacker,
        queryBuilder
    );

    // Then:
    assertThat(
        result.getSourceTableStep(),
        equalTo(
            ExecutionStepFactory.streamToTable(
                childContextStacker,
                Formats.of(keyFormat, valueFormat, SerdeOption.none()),
                sourceStep
            )
        )
    );
  }

  @Test
  public void shouldConvertToTableWithCorrectProperties() {
    // Given:
    givenInitialSchemaKStreamUsesMocks();

    // When:
    final SchemaKTable result = initialSchemaKStream.toTable(
        keyFormat,
        valueFormat,
        childContextStacker,
        queryBuilder
    );

    // Then:
    assertThat(result.getSchema(), is(initialSchemaKStream.getSchema()));
    assertThat(result.getKeyField(), is(initialSchemaKStream.getKeyField()));
    assertThat(result.getKeySerde(), is(initialSchemaKStream.getKeySerde()));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void shouldPerformStreamToStreamLeftJoin() {
    // Given:
    final SchemaKStream initialSchemaKStream = buildSchemaKStreamForJoin(ksqlStream, mockKStream);
    final JoinWindows joinWindow = JoinWindows.of(Duration.ofMillis(10L));
    when(
        mockKStream.leftJoin(
            any(KStream.class),
            any(KsqlValueJoiner.class),
            any(JoinWindows.class),
            any(StreamJoined.class))
    ).thenReturn(mockKStream);
    when(queryBuilder.buildValueSerde(any(), any(), any()))
        .thenReturn(leftSerde)
        .thenReturn(rightSerde);

    // When:
    final SchemaKStream joinedKStream = initialSchemaKStream
        .leftJoin(
            secondSchemaKStream,
            joinSchema,
            validJoinKeyField,
            joinWindow,
            valueFormat,
            valueFormat,
            childContextStacker,
            queryBuilder
        );

    // Then:
    joinedKStream.getSourceStep().build(planBuilder);
    verifyCreateStreamJoined(rightSerde);
    verify(mockKStream).leftJoin(
        eq(secondKStream),
        any(KsqlValueJoiner.class),
        eq(joinWindow),
        same(streamJoined)
    );
    assertThat(joinedKStream, instanceOf(SchemaKStream.class));
    assertEquals(SchemaKStream.Type.JOIN, joinedKStream.type);
    assertEquals(joinSchema, joinedKStream.getSchema());
    assertThat(joinedKStream.getKeyField(), is(validJoinKeyField));
    assertEquals(Arrays.asList(initialSchemaKStream, secondSchemaKStream),
                 joinedKStream.sourceSchemaKStreams);
  }

  @FunctionalInterface
  private interface StreamStreamJoin {
    SchemaKStream join(
        SchemaKStream otherSchemaKStream,
        LogicalSchema joinSchema,
        KeyField keyField,
        JoinWindows joinWindows,
        ValueFormat leftFormat,
        ValueFormat rightFormat,
        QueryContext.Stacker contextStacker,
        KsqlQueryBuilder queryBuilder
    );
  }

  @Test
  public void shouldBuildStepForStreamStreamJoin() {
    // Given:
    final SchemaKStream initialSchemaKStream = buildSchemaKStreamForJoin(ksqlStream, mockKStream);
    final JoinWindows joinWindow = JoinWindows.of(Duration.ofMillis(10L));

    final List<Pair<JoinType, StreamStreamJoin>> cases = ImmutableList.of(
        Pair.of(JoinType.LEFT, initialSchemaKStream::leftJoin),
        Pair.of(JoinType.INNER, initialSchemaKStream::join),
        Pair.of(JoinType.OUTER, initialSchemaKStream::outerJoin)
    );

    for (final Pair<JoinType, StreamStreamJoin> testcase : cases) {
      final SchemaKStream joinedKStream = testcase.right.join(
          secondSchemaKStream,
          joinSchema,
          validJoinKeyField,
          joinWindow,
          valueFormat,
          rightFormat,
          childContextStacker,
          queryBuilder
      );

      // Then:
      assertThat(
          joinedKStream.getSourceStep(),
          equalTo(
              ExecutionStepFactory.streamStreamJoin(
                  childContextStacker,
                  testcase.left,
                  Formats.of(keyFormat, valueFormat, SerdeOption.none()),
                  Formats.of(keyFormat, rightFormat, SerdeOption.none()),
                  initialSchemaKStream.getSourceStep(),
                  secondSchemaKStream.getSourceStep(),
                  joinSchema,
                  joinWindow
              )
          )
      );
    }
  }

  @SuppressWarnings("unchecked")
  @Test
  public void shouldPerformStreamToStreamInnerJoin() {
    // Given:
    final SchemaKStream initialSchemaKStream = buildSchemaKStreamForJoin(ksqlStream, mockKStream);
    final JoinWindows joinWindow = JoinWindows.of(Duration.ofMillis(10L));
    when(
        mockKStream.join(
            any(KStream.class),
            any(KsqlValueJoiner.class),
            any(JoinWindows.class),
            any(StreamJoined.class))
    ).thenReturn(mockKStream);
    when(queryBuilder.buildValueSerde(any(), any(), any()))
        .thenReturn(leftSerde)
        .thenReturn(rightSerde);

    // When:
    final SchemaKStream joinedKStream = initialSchemaKStream
        .join(
            secondSchemaKStream,
            joinSchema,
            validJoinKeyField,
            joinWindow,
            valueFormat,
            valueFormat,
            childContextStacker,
            queryBuilder
        );

    // Then:
    joinedKStream.getSourceStep().build(planBuilder);
    verifyCreateStreamJoined(rightSerde);
    verify(mockKStream).join(
        eq(secondKStream),
        any(KsqlValueJoiner.class),
        eq(joinWindow),
        same(streamJoined)
    );

    assertThat(joinedKStream, instanceOf(SchemaKStream.class));
    assertEquals(SchemaKStream.Type.JOIN, joinedKStream.type);
    assertEquals(joinSchema, joinedKStream.getSchema());
    Assert.assertThat(joinedKStream.getKeyField(), is(validJoinKeyField));
    assertEquals(Arrays.asList(initialSchemaKStream, secondSchemaKStream),
                 joinedKStream.sourceSchemaKStreams);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void shouldPerformStreamToStreamOuterJoin() {
    // Given:
    final SchemaKStream initialSchemaKStream = buildSchemaKStreamForJoin(ksqlStream, mockKStream);
    final JoinWindows joinWindow = JoinWindows.of(Duration.ofMillis(10L));
    when(
        mockKStream.outerJoin(
            any(KStream.class),
            any(KsqlValueJoiner.class),
            any(JoinWindows.class),
            any(StreamJoined.class))
    ).thenReturn(mockKStream);
    when(queryBuilder.buildValueSerde(any(), any(), any()))
        .thenReturn(leftSerde)
        .thenReturn(rightSerde);

    // When:
    final SchemaKStream joinedKStream = initialSchemaKStream
        .outerJoin(
            secondSchemaKStream,
            joinSchema,
            validJoinKeyField,
            joinWindow,
            valueFormat,
            valueFormat,
            childContextStacker,
            queryBuilder
        );

    // Then:
    joinedKStream.getSourceStep().build(planBuilder);
    verifyCreateStreamJoined(rightSerde);
    verify(mockKStream).outerJoin(
        eq(secondKStream),
        any(KsqlValueJoiner.class),
        eq(joinWindow),
        same(streamJoined)
    );
    assertThat(joinedKStream, instanceOf(SchemaKStream.class));
    assertEquals(SchemaKStream.Type.JOIN, joinedKStream.type);
    assertEquals(joinSchema, joinedKStream.getSchema());
    assertThat(joinedKStream.getKeyField(), is(validJoinKeyField));
    assertEquals(Arrays.asList(initialSchemaKStream, secondSchemaKStream),
                 joinedKStream.sourceSchemaKStreams);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void shouldPerformStreamToTableLeftJoin() {
    // Given:
    final SchemaKStream initialSchemaKStream = buildSchemaKStreamForJoin(ksqlStream, mockKStream);
    when(
        mockKStream.leftJoin(
            any(KTable.class),
            any(KsqlValueJoiner.class),
            any(Joined.class))
    ).thenReturn(mockKStream);
    when(queryBuilder.buildValueSerde(any(), any(), any())).thenReturn(leftSerde);

    // When:
    final SchemaKStream joinedKStream = initialSchemaKStream
        .leftJoin(
            schemaKTable,
            joinSchema,
            validJoinKeyField,
            valueFormat,
            childContextStacker,
            queryBuilder
        );

    // Then:
    joinedKStream.getSourceStep().build(planBuilder);
    verifyCreateJoined(null);
    verify(mockKStream).leftJoin(
        eq(kTable),
        any(KsqlValueJoiner.class),
        same(joined));
    assertThat(joinedKStream, instanceOf(SchemaKStream.class));
    assertEquals(SchemaKStream.Type.JOIN, joinedKStream.type);
    assertEquals(joinSchema, joinedKStream.getSchema());
    assertThat(joinedKStream.getKeyField(), is(validJoinKeyField));
    assertEquals(Arrays.asList(initialSchemaKStream, schemaKTable),
                 joinedKStream.sourceSchemaKStreams);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void shouldPerformStreamToTableInnerJoin() {
    // Given:
    final SchemaKStream initialSchemaKStream = buildSchemaKStreamForJoin(ksqlStream, mockKStream);
    when(
        mockKStream.join(
            any(KTable.class),
            any(KsqlValueJoiner.class),
            any(Joined.class))
    ).thenReturn(mockKStream);
    when(queryBuilder.buildValueSerde(any(), any(), any())).thenReturn(leftSerde);

    // When:
    final SchemaKStream joinedKStream = initialSchemaKStream
        .join(
            schemaKTable,
            joinSchema,
            validJoinKeyField,
            valueFormat,
            childContextStacker,
            queryBuilder
        );

    // Then:
    joinedKStream.getSourceStep().build(planBuilder);
    verifyCreateJoined(null);
    verify(mockKStream).join(
        eq(kTable),
        any(KsqlValueJoiner.class),
        same(joined)
    );

    assertThat(joinedKStream, instanceOf(SchemaKStream.class));
    assertEquals(SchemaKStream.Type.JOIN, joinedKStream.type);
    assertEquals(joinSchema, joinedKStream.getSchema());
    assertThat(joinedKStream.getKeyField(), is(validJoinKeyField));
    assertEquals(Arrays.asList(initialSchemaKStream, schemaKTable),
                 joinedKStream.sourceSchemaKStreams);
  }
  @FunctionalInterface
  private interface StreamTableJoin {
    SchemaKStream join(
        SchemaKTable other,
        LogicalSchema joinSchema,
        KeyField keyField,
        ValueFormat leftFormat,
        QueryContext.Stacker contextStacker,
        KsqlQueryBuilder queryBuilder
    );
  }

  @Test
  public void shouldBuildStepForStreamTableJoin() {
    // Given:
    final SchemaKStream initialSchemaKStream = buildSchemaKStreamForJoin(ksqlStream, mockKStream);

    final List<Pair<JoinType, StreamTableJoin>> cases = ImmutableList.of(
        Pair.of(JoinType.LEFT, initialSchemaKStream::leftJoin),
        Pair.of(JoinType.INNER, initialSchemaKStream::join)
    );

    for (final Pair<JoinType, StreamTableJoin> testcase : cases) {
      final SchemaKStream joinedKStream = testcase.right.join(
          schemaKTable,
          joinSchema,
          validJoinKeyField,
          valueFormat,
          childContextStacker,
          queryBuilder
      );

      // Then:
      assertThat(
          joinedKStream.getSourceStep(),
          equalTo(
              ExecutionStepFactory.streamTableJoin(
                  childContextStacker,
                  testcase.left,
                  Formats.of(keyFormat, valueFormat, SerdeOption.none()),
                  initialSchemaKStream.getSourceStep(),
                  schemaKTable.getSourceTableStep(),
                  joinSchema
              )
          )
      );
    }
  }

  @Test
  public void shouldSummarizeExecutionPlanCorrectly() {
    // Given:
    when(sourceProperties.getSchema()).thenReturn(simpleSchema);
    final SchemaKStream parentSchemaKStream = mock(SchemaKStream.class);
    when(parentSchemaKStream.getExecutionPlan(any(), anyString()))
        .thenReturn("parent plan");
    when(sourceProperties.getQueryContext()).thenReturn(
        queryContext.push("source").getQueryContext());
    final SchemaKStream schemaKtream = new SchemaKStream(
        sourceStep,
        keyFormat,
        keySerde,
        KeyField.of(ColumnRef.withoutSource(ColumnName.of("key")),
            simpleSchema.findValueColumn(ColumnRef.withoutSource(ColumnName.of("key"))).get()),
        ImmutableList.of(parentSchemaKStream),
        Type.SOURCE,
        ksqlConfig,
        functionRegistry
    );

    // When/Then:
    assertThat(schemaKtream.getExecutionPlan(new QueryId("query"), ""), equalTo(
        " > [ SOURCE ] | Schema: [ROWKEY STRING KEY, key STRING, val BIGINT] | "
            + "Logger: query.node.source\n"
            + "\tparent plan"));
  }

  @Test
  public void shouldSummarizeExecutionPlanCorrectlyForRoot() {
    // Given:
    when(sourceProperties.getSchema()).thenReturn(simpleSchema);
    when(sourceProperties.getQueryContext()).thenReturn(
        queryContext.push("source").getQueryContext());
    final SchemaKStream schemaKtream = new SchemaKStream(
        sourceStep,
        keyFormat,
        keySerde,
        KeyField.of(
            ColumnRef.withoutSource(ColumnName.of("key")),
            simpleSchema.findValueColumn(ColumnRef.withoutSource(ColumnName.of("key"))).get()),
        Collections.emptyList(),
        Type.SOURCE,
        ksqlConfig,
        functionRegistry
    );

    // When/Then:
    assertThat(schemaKtream.getExecutionPlan(new QueryId("query"), ""), equalTo(
        " > [ SOURCE ] | Schema: [ROWKEY STRING KEY, key STRING, val BIGINT] | "
            + "Logger: query.node.source\n"));
  }

  @Test
  public void shouldSummarizeExecutionPlanCorrectlyWhenMultipleParents() {
    // Given:
    final SchemaKStream parentSchemaKStream1 = mock(SchemaKStream.class);
    when(parentSchemaKStream1.getExecutionPlan(any(), anyString()))
        .thenReturn("parent 1 plan");
    final SchemaKStream parentSchemaKStream2 = mock(SchemaKStream.class);
    when(parentSchemaKStream2.getExecutionPlan(any(), anyString()))
        .thenReturn("parent 2 plan");
    when(sourceProperties.getSchema()).thenReturn(simpleSchema);
    when(sourceProperties.getQueryContext()).thenReturn(
        queryContext.push("source").getQueryContext());
    final SchemaKStream schemaKtream = new SchemaKStream(
        sourceStep,
        keyFormat,
        keySerde,
        KeyField.of(
            ColumnRef.withoutSource(ColumnName.of("key")),
            simpleSchema.findValueColumn(ColumnRef.withoutSource(ColumnName.of("key"))).get()),
        ImmutableList.of(parentSchemaKStream1, parentSchemaKStream2),
        Type.SOURCE,
        ksqlConfig,
        functionRegistry
    );

    // When/Then:
    assertThat(schemaKtream.getExecutionPlan(new QueryId("query"), ""), equalTo(
        " > [ SOURCE ] | Schema: [ROWKEY STRING KEY, key STRING, val BIGINT] | "
            + "Logger: query.node.source\n"
            + "\tparent 1 plan"
            + "\tparent 2 plan"));
  }

  private void whenCreateJoined() {
    when(
        mockJoinedFactory.create(
            any(Serde.class),
            any(Serde.class),
            any(),
            anyString())
    ).thenReturn(joined);
  }

  private void verifyCreateJoined(final Serde<GenericRow> rightSerde) {
    verify(mockJoinedFactory).create(
        same(keySerde),
        same(leftSerde),
        same(rightSerde),
        eq(StreamsUtil.buildOpName(childContextStacker.getQueryContext()))
    );
  }

  private void givenSourcePropertiesWithSchema(
      final ExecutionStep sourceStep,
      final LogicalSchema schema) {
    reset(sourceProperties);
    when(sourceStep.getSchema()).thenReturn(schema);
    when(sourceProperties.getSchema()).thenReturn(schema);
    when(sourceProperties.withQueryContext(any())).thenAnswer(
        i -> new DefaultExecutionStepProperties(schema, (QueryContext) i.getArguments()[0])
    );
  }

  private void verifyCreateStreamJoined(final Serde<GenericRow> rightSerde) {
    verify(mockStreamJoinedFactory).create(
        same(keySerde),
        same(leftSerde),
        same(rightSerde),
        eq(StreamsUtil.buildOpName(childContextStacker.getQueryContext())),
        eq(StreamsUtil.buildOpName(childContextStacker.getQueryContext()))
    );
  }

  private SchemaKStream buildSchemaKStream(
      final LogicalSchema schema,
      final KeyField keyField,
      final ExecutionStep sourceStep,
      final KStream kStream) {
    givenSourcePropertiesWithSchema(sourceStep, schema);
    when(sourceStep.build(any()))
        .thenReturn(new KStreamHolder(kStream, keySerdeFactory));
    return new SchemaKStream(
        sourceStep,
        sourceProperties,
        keyFormat,
        keySerde,
        keyField,
        new ArrayList<>(),
        Type.SOURCE,
        ksqlConfig,
        functionRegistry
    );
  }

  private void givenInitialSchemaKStreamUsesMocks() {
    final LogicalSchema schema = ksqlStream.getSchema().withAlias(ksqlStream.getName());

    final Optional<ColumnRef> newKeyName = ksqlStream.getKeyField()
        .ref()
        .map(ref -> ref.withSource(ksqlStream.getName()));

    final KeyField keyFieldWithAlias = KeyField.of(
        newKeyName,
        ksqlStream.getKeyField().legacy());

    initialSchemaKStream = buildSchemaKStream(
        schema,
        keyFieldWithAlias,
        sourceStep,
        mockKStream
    );
  }

  private SchemaKStream buildSchemaKStreamForJoin(
      final KsqlStream ksqlStream,
      final KStream kStream) {
    return buildSchemaKStream(
        ksqlStream.getSchema().withAlias(SourceName.of("test")),
        ksqlStream.getKeyField().withAlias(SourceName.of("test")),
        sourceStep,
        kStream
    );
  }

  private SchemaKStream buildSchemaKStreamForJoin(
      final KsqlStream ksqlStream,
      final ExecutionStep sourceStep,
      final KStream kStream) {
    return buildSchemaKStream(
        ksqlStream.getSchema().withAlias(SourceName.of("test")),
        ksqlStream.getKeyField().withAlias(SourceName.of("test")),
        sourceStep,
        kStream
    );
  }

  private static LogicalSchema getJoinSchema(
      final LogicalSchema leftSchema,
      final LogicalSchema rightSchema
  ) {
    final LogicalSchema.Builder schemaBuilder = LogicalSchema.builder();
    final String leftAlias = "left";
    final String rightAlias = "right";
    for (final Column field : leftSchema.value()) {
      schemaBuilder.valueColumn(Column.of(SourceName.of(leftAlias), field.name(), field.type()));
    }

    for (final Column field : rightSchema.value()) {
      schemaBuilder.valueColumn(Column.of(SourceName.of(rightAlias), field.name(), field.type()));
    }
    return schemaBuilder.build();
  }

  private PlanNode givenInitialKStreamOf(final String selectQuery) {
    final PlanNode logicalPlan = AnalysisTestUtil.buildLogicalPlan(
        ksqlConfig,
        selectQuery,
        metaStore
    );
    givenSourcePropertiesWithSchema(sourceStep, logicalPlan.getTheSourceNode().getSchema());
    initialSchemaKStream = new SchemaKStream(
        sourceStep,
        keyFormat,
        keySerde,
        logicalPlan.getTheSourceNode().getKeyField(),
        new ArrayList<>(),
        SchemaKStream.Type.SOURCE,
        ksqlConfig,
        functionRegistry
    );

    return logicalPlan;
  }
}
