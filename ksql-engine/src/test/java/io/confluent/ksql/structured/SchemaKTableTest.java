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

import static io.confluent.ksql.schema.ksql.ColumnMatchers.valueColumn;
import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.capture;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.mock;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.same;
import static org.easymock.EasyMock.verify;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
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
import io.confluent.ksql.execution.expression.tree.LongLiteral;
import io.confluent.ksql.execution.plan.ExecutionStep;
import io.confluent.ksql.execution.plan.Formats;
import io.confluent.ksql.execution.plan.JoinType;
import io.confluent.ksql.execution.plan.KTableHolder;
import io.confluent.ksql.execution.plan.KeySerdeFactory;
import io.confluent.ksql.execution.plan.PlanBuilder;
import io.confluent.ksql.execution.plan.SelectExpression;
import io.confluent.ksql.execution.plan.TableFilter;
import io.confluent.ksql.execution.streams.AggregateParamsFactory;
import io.confluent.ksql.execution.streams.ConsumedFactory;
import io.confluent.ksql.execution.streams.ExecutionStepFactory;
import io.confluent.ksql.execution.streams.GroupedFactory;
import io.confluent.ksql.execution.streams.JoinedFactory;
import io.confluent.ksql.execution.streams.KSPlanBuilder;
import io.confluent.ksql.execution.streams.KsqlValueJoiner;
import io.confluent.ksql.execution.streams.MaterializedFactory;
import io.confluent.ksql.execution.streams.SqlPredicateFactory;
import io.confluent.ksql.execution.streams.StepSchemaResolver;
import io.confluent.ksql.execution.streams.StreamJoinedFactory;
import io.confluent.ksql.execution.streams.StreamsFactories;
import io.confluent.ksql.execution.streams.StreamsUtil;
import io.confluent.ksql.execution.util.StructKeyUtil;
import io.confluent.ksql.function.InternalFunctionRegistry;
import io.confluent.ksql.logging.processing.ProcessingLogContext;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.model.KeyField;
import io.confluent.ksql.metastore.model.KsqlTable;
import io.confluent.ksql.metastore.model.MetaStoreMatchers.KeyFieldMatchers;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.planner.plan.FilterNode;
import io.confluent.ksql.planner.plan.PlanNode;
import io.confluent.ksql.planner.plan.ProjectNode;
import io.confluent.ksql.schema.ksql.Column;
import io.confluent.ksql.schema.ksql.ColumnRef;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.PersistenceSchema;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.serde.Format;
import io.confluent.ksql.serde.FormatInfo;
import io.confluent.ksql.serde.GenericRowSerDe;
import io.confluent.ksql.serde.KeyFormat;
import io.confluent.ksql.serde.SerdeOption;
import io.confluent.ksql.serde.ValueFormat;
import io.confluent.ksql.testutils.AnalysisTestUtil;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.MetaStoreFixture;
import io.confluent.ksql.util.Pair;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KGroupedTable;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Predicate;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

@SuppressWarnings("unchecked")
@RunWith(MockitoJUnitRunner.class)
public class SchemaKTableTest {

  private final KsqlConfig ksqlConfig = new KsqlConfig(Collections.emptyMap());
  private final MetaStore metaStore = MetaStoreFixture.getNewMetaStore(new InternalFunctionRegistry());
  private final GroupedFactory groupedFactory = mock(GroupedFactory.class);
  private final Grouped grouped = Grouped.with(
      "group", Serdes.String(), Serdes.String());

  private SchemaKTable initialSchemaKTable;
  private KTable kTable;
  private KTable secondKTable;
  private KsqlTable<?> ksqlTable;
  private KsqlTable<?> secondKsqlTable;
  private InternalFunctionRegistry functionRegistry;
  private KTable mockKTable;
  private KeyField validKeyField;
  private SchemaKTable firstSchemaKTable;
  private SchemaKTable secondSchemaKTable;
  private LogicalSchema joinSchema;
  private StepSchemaResolver schemaResolver;
  private final QueryContext.Stacker queryContext
      = new QueryContext.Stacker().push("node");
  private final QueryContext.Stacker childContextStacker = queryContext.push("child");
  private final ProcessingLogContext processingLogContext = ProcessingLogContext.create();
  private static final Expression TEST_2_COL_1 =
      new ColumnReferenceExp(ColumnRef.of(SourceName.of("TEST2"), ColumnName.of("COL1")));
  private static final Expression TEST_2_COL_2 =
      new ColumnReferenceExp(ColumnRef.of(SourceName.of("TEST2"), ColumnName.of("COL2")));
  private static final KeyFormat keyFormat = KeyFormat.nonWindowed(FormatInfo.of(Format.JSON));
  private static final ValueFormat valueFormat = ValueFormat.of(FormatInfo.of(Format.JSON));

  private PlanBuilder planBuilder;

  @Mock
  private KsqlQueryBuilder queryBuilder;
  @Mock
  private KeySerdeFactory<Struct> keySerdeFactory;

  @Before
  public void init() {
    functionRegistry = new InternalFunctionRegistry();
    schemaResolver = new StepSchemaResolver(ksqlConfig, functionRegistry);
    ksqlTable = (KsqlTable) metaStore.getSource(SourceName.of("TEST2"));
    final StreamsBuilder builder = new StreamsBuilder();
    kTable = builder.table(
        ksqlTable.getKsqlTopic().getKafkaTopicName(),
        Consumed.with(
            Serdes.String(),
            getRowSerde(ksqlTable.getKsqlTopic(), ksqlTable.getSchema().valueConnectSchema())
        ));

    secondKsqlTable = (KsqlTable) metaStore.getSource(SourceName.of("TEST3"));
    secondKTable = builder.table(
        secondKsqlTable.getKsqlTopic().getKafkaTopicName(),
        Consumed.with(
            Serdes.String(),
            getRowSerde(secondKsqlTable.getKsqlTopic(), secondKsqlTable.getSchema().valueConnectSchema())
        ));

    mockKTable = EasyMock.niceMock(KTable.class);
    validKeyField = KeyField
        .of(Optional.of(ColumnRef.of(ksqlTable.getName(), ColumnName.of("COL1"))));
    firstSchemaKTable = buildSchemaKTableForJoin(ksqlTable, mockKTable);
    secondSchemaKTable = buildSchemaKTableForJoin(secondKsqlTable, secondKTable);
    joinSchema = getJoinSchema(ksqlTable.getSchema(), secondKsqlTable.getSchema());

    when(queryBuilder.getKsqlConfig()).thenReturn(ksqlConfig);
    when(queryBuilder.getFunctionRegistry()).thenReturn(functionRegistry);
    planBuilder = new KSPlanBuilder(
        queryBuilder,
        mock(SqlPredicateFactory.class),
        mock(AggregateParamsFactory.class),
        new StreamsFactories(
            groupedFactory,
            mock(JoinedFactory.class),
            mock(MaterializedFactory.class),
            mock(StreamJoinedFactory.class),
            mock(ConsumedFactory.class)
        )
    );
  }

  private ExecutionStep buildSourceStep(final LogicalSchema schema, final KTable kTable) {
    final ExecutionStep sourceStep = Mockito.mock(ExecutionStep.class);
    when(sourceStep.build(any())).thenReturn(
        KTableHolder.unmaterialized(kTable, schema, keySerdeFactory));
    return sourceStep;
  }

  private SchemaKTable buildSchemaKTable(
      final LogicalSchema schema,
      final KeyField keyField,
      final KTable kTable) {
    return new SchemaKTable(
        buildSourceStep(schema, kTable),
        schema,
        keyFormat,
        keyField,
        ksqlConfig,
        functionRegistry
    );
  }

  private SchemaKTable buildSchemaKTableFromPlan(final PlanNode logicalPlan) {
    return new SchemaKTable(
        buildSourceStep(logicalPlan.getTheSourceNode().getSchema(), kTable),
        logicalPlan.getTheSourceNode().getSchema(),
        keyFormat,
        logicalPlan.getTheSourceNode().getKeyField(),
        ksqlConfig,
        functionRegistry
    );
  }

  private SchemaKTable buildSchemaKTable(final KsqlTable ksqlTable, final KTable kTable) {
    final LogicalSchema schema = ksqlTable.getSchema().withAlias(ksqlTable.getName());

    final Optional<ColumnRef> newKeyName = ksqlTable.getKeyField().ref()
        .map(ref -> ref.withSource(ksqlTable.getName()));

    final KeyField keyFieldWithAlias = KeyField.of(newKeyName);

    return buildSchemaKTable(
        schema,
        keyFieldWithAlias,
        kTable
    );
  }

  private SchemaKTable buildSchemaKTableForJoin(final KsqlTable ksqlTable, final KTable kTable) {
    return buildSchemaKTable(
        ksqlTable.getSchema().withAlias(ksqlTable.getName()),
        ksqlTable.getKeyField().withAlias(ksqlTable.getName()),
        kTable
    );
  }

  private Serde<GenericRow> getRowSerde(final KsqlTopic topic, final ConnectSchema schema) {
    return GenericRowSerDe.from(
        topic.getValueFormat().getFormatInfo(),
        PersistenceSchema.from(schema, false),
        new KsqlConfig(Collections.emptyMap()),
        MockSchemaRegistryClient::new,
        "test",
        processingLogContext);
  }

  @Test
  public void testSelectSchemaKStream() {
    // Given:
    final String selectQuery = "SELECT col0, col2, col3 FROM test2 WHERE col0 > 100 EMIT CHANGES;";
    final PlanNode logicalPlan = buildLogicalPlan(selectQuery);
    final ProjectNode projectNode = (ProjectNode) logicalPlan.getSources().get(0);
    initialSchemaKTable = buildSchemaKTableFromPlan(logicalPlan);

    // When:
    final SchemaKTable projectedSchemaKStream = initialSchemaKTable.select(
        projectNode.getSelectExpressions(),
        childContextStacker,
        queryBuilder
    );

    // Then:
    assertThat(projectedSchemaKStream.getSchema().value(), contains(
        valueColumn(ColumnName.of("COL0"), SqlTypes.BIGINT),
        valueColumn(ColumnName.of("COL2"), SqlTypes.STRING),
        valueColumn(ColumnName.of("COL3"), SqlTypes.DOUBLE)
    ));
  }

  @Test
  public void shouldBuildStepForSelect() {
    // Given:
    final String selectQuery = "SELECT col0, col2, col3 FROM test2 WHERE col0 > 100 EMIT CHANGES;";
    final PlanNode logicalPlan = buildLogicalPlan(selectQuery);
    final ProjectNode projectNode = (ProjectNode) logicalPlan.getSources().get(0);
    initialSchemaKTable = buildSchemaKTableFromPlan(logicalPlan);

    // When:
    final SchemaKTable projectedSchemaKStream = initialSchemaKTable.select(
        projectNode.getSelectExpressions(),
        childContextStacker,
        queryBuilder
    );

    // Then:
    assertThat(
        projectedSchemaKStream.getSourceTableStep(),
        equalTo(
            ExecutionStepFactory.tableMapValues(
                childContextStacker,
                initialSchemaKTable.getSourceTableStep(),
                projectNode.getSelectExpressions()
            )
        )
    );
  }

  @Test
  public void shouldBuildSchemaForSelect() {
    // Given:
    final String selectQuery = "SELECT col0, col2, col3 FROM test2 WHERE col0 > 100 EMIT CHANGES;";
    final PlanNode logicalPlan = buildLogicalPlan(selectQuery);
    final ProjectNode projectNode = (ProjectNode) logicalPlan.getSources().get(0);
    initialSchemaKTable = buildSchemaKTableFromPlan(logicalPlan);

    // When:
    final SchemaKTable projectedSchemaKStream = initialSchemaKTable.select(
        projectNode.getSelectExpressions(),
        childContextStacker,
        queryBuilder
    );

    // Then:
    assertThat(
        projectedSchemaKStream.getSchema(),
        is(schemaResolver.resolve(
            projectedSchemaKStream.getSourceStep(), initialSchemaKTable.getSchema()))
    );
  }

  @Test
  public void testSelectWithExpression() {
    // Given:
    final String selectQuery = "SELECT col0, LEN(UCASE(col2)), col3*3+5 FROM test2 WHERE col0 > 100 EMIT CHANGES;";
    final PlanNode logicalPlan = buildLogicalPlan(selectQuery);
    final ProjectNode projectNode = (ProjectNode) logicalPlan.getSources().get(0);
    initialSchemaKTable = buildSchemaKTableFromPlan(logicalPlan);

    // When:
    final SchemaKTable projectedSchemaKStream = initialSchemaKTable.select(
        projectNode.getSelectExpressions(),
        childContextStacker,
        queryBuilder
    );

    // Then:
    assertThat(projectedSchemaKStream.getSchema().value(), contains(
        valueColumn(ColumnName.of("COL0"), SqlTypes.BIGINT),
        valueColumn(ColumnName.of("KSQL_COL_1"), SqlTypes.INTEGER),
        valueColumn(ColumnName.of("KSQL_COL_2"), SqlTypes.DOUBLE)
    ));
  }

  @Test
  public void shouldBuildSchemaKTableWithCorrectSchemaForFilter() {
    // Given:
    final String selectQuery = "SELECT col0, col2, col3 FROM test2 WHERE col0 > 100 EMIT CHANGES;";
    final PlanNode logicalPlan = buildLogicalPlan(selectQuery);
    final FilterNode filterNode = (FilterNode) logicalPlan.getSources().get(0).getSources().get(0);
    initialSchemaKTable = buildSchemaKTableFromPlan(logicalPlan);

    // When:
    final SchemaKTable filteredSchemaKStream = initialSchemaKTable.filter(
        filterNode.getPredicate(),
        childContextStacker
    );

    // Then:
    final SourceName test2 = SourceName.of("TEST2");
    assertThat(filteredSchemaKStream.getSchema().value(), contains(
        valueColumn(test2, ColumnName.of("ROWTIME"), SqlTypes.BIGINT),
        valueColumn(test2, ColumnName.of("ROWKEY"), SqlTypes.STRING),
        valueColumn(test2, ColumnName.of("COL0"), SqlTypes.BIGINT),
        valueColumn(test2, ColumnName.of("COL1"), SqlTypes.STRING),
        valueColumn(test2, ColumnName.of("COL2"), SqlTypes.STRING),
        valueColumn(test2, ColumnName.of("COL3"), SqlTypes.DOUBLE),
        valueColumn(test2, ColumnName.of("COL4"), SqlTypes.BOOLEAN)
    ));
  }

  @Test
  public void shouldRewriteTimeComparisonInFilter() {
    // Given:
    final String selectQuery = "SELECT col0, col2, col3 FROM test2 "
        + "WHERE ROWTIME = '1984-01-01T00:00:00+00:00' EMIT CHANGES;";
    final PlanNode logicalPlan = buildLogicalPlan(selectQuery);
    final FilterNode filterNode = (FilterNode) logicalPlan.getSources().get(0).getSources().get(0);
    initialSchemaKTable = buildSchemaKTableFromPlan(logicalPlan);

    // When:
    final SchemaKTable filteredSchemaKTable = initialSchemaKTable.filter(
        filterNode.getPredicate(),
        childContextStacker
    );

    // Then:
    final TableFilter step = (TableFilter) filteredSchemaKTable.getSourceTableStep();
    assertThat(
        step.getFilterExpression(),
        Matchers.equalTo(
            new ComparisonExpression(
                ComparisonExpression.Type.EQUAL,
                new ColumnReferenceExp(ColumnRef.of(SourceName.of("TEST2"),
                    ColumnName.of("ROWTIME"))),
                new LongLiteral(441763200000L)
            )
        )
    );
  }

  @Test
  public void shouldBuildStepForFilter() {
    // Given:
    final String selectQuery = "SELECT col0, col2, col3 FROM test2 WHERE col0 > 100 EMIT CHANGES;";
    final PlanNode logicalPlan = buildLogicalPlan(selectQuery);
    final FilterNode filterNode = (FilterNode) logicalPlan.getSources().get(0).getSources().get(0);
    initialSchemaKTable = buildSchemaKTableFromPlan(logicalPlan);

    // When:
    final SchemaKTable filteredSchemaKStream = initialSchemaKTable.filter(
        filterNode.getPredicate(),
        childContextStacker
    );

    // Then:
    assertThat(
        filteredSchemaKStream.getSourceTableStep(),
        equalTo(
            ExecutionStepFactory.tableFilter(
                childContextStacker,
                initialSchemaKTable.getSourceTableStep(),
                filterNode.getPredicate()
            )
        )
    );
  }

  @Test
  public void testGroupBy() {
    // Given:
    final String selectQuery = "SELECT col0, col1, col2 FROM test2 EMIT CHANGES;";
    final PlanNode logicalPlan = buildLogicalPlan(selectQuery);
    initialSchemaKTable = buildSchemaKTableFromPlan(logicalPlan);
    final List<Expression> groupByExpressions = Arrays.asList(TEST_2_COL_2, TEST_2_COL_1);

    // When:
    final SchemaKGroupedTable groupedSchemaKTable = initialSchemaKTable.groupBy(
        valueFormat,
        groupByExpressions,
        childContextStacker
    );

    // Then:
    assertThat(groupedSchemaKTable, instanceOf(SchemaKGroupedTable.class));
    assertThat(groupedSchemaKTable.getKeyField().ref(), is(Optional.empty()));
  }

  @Test
  public void shouldBuildStepForGroupBy() {
    // Given:
    final String selectQuery = "SELECT col0, col1, col2 FROM test2 EMIT CHANGES;";
    final PlanNode logicalPlan = buildLogicalPlan(selectQuery);
    initialSchemaKTable = buildSchemaKTableFromPlan(logicalPlan);
    final List<Expression> groupByExpressions = Arrays.asList(TEST_2_COL_2, TEST_2_COL_1);

    // When:
    final SchemaKGroupedTable groupedSchemaKTable = initialSchemaKTable.groupBy(
        valueFormat,
        groupByExpressions,
        childContextStacker
    );

    // Then:
    assertThat(
        groupedSchemaKTable.getSourceTableStep(),
        equalTo(
            ExecutionStepFactory.tableGroupBy(
                childContextStacker,
                initialSchemaKTable.getSourceTableStep(),
                Formats.of(initialSchemaKTable.keyFormat, valueFormat, SerdeOption.none()),
                groupByExpressions
            )
        )
    );
  }

  @Test
  public void shouldBuildSchemaForGroupBy() {
    // Given:
    final String selectQuery = "SELECT col0, col1, col2 FROM test2 EMIT CHANGES;";
    final PlanNode logicalPlan = buildLogicalPlan(selectQuery);
    initialSchemaKTable = buildSchemaKTableFromPlan(logicalPlan);
    final List<Expression> groupByExpressions = Arrays.asList(TEST_2_COL_2, TEST_2_COL_1);

    // When:
    final SchemaKGroupedTable groupedSchemaKTable = initialSchemaKTable.groupBy(
        valueFormat,
        groupByExpressions,
        childContextStacker
    );

    // Then:
    assertThat(groupedSchemaKTable.schema, is(schemaResolver.resolve(
        groupedSchemaKTable.getSourceTableStep(), initialSchemaKTable.getSchema()))
    );
  }

  @Test
  public void shouldUseOpNameForGrouped() {
    // Given:
    final Serde<GenericRow> valSerde =
        getRowSerde(ksqlTable.getKsqlTopic(), ksqlTable.getSchema().valueConnectSchema());
    when(queryBuilder.buildValueSerde(any(), any(), any())).thenReturn(valSerde);
    expect(
        groupedFactory.create(
            eq(StreamsUtil.buildOpName(childContextStacker.getQueryContext())),
            anyObject(Serdes.String().getClass()),
            same(valSerde))
    ).andReturn(grouped);
    expect(mockKTable.filter(anyObject(Predicate.class))).andReturn(mockKTable);
    final KGroupedTable groupedTable = mock(KGroupedTable.class);
    expect(mockKTable.groupBy(anyObject(), same(grouped))).andReturn(groupedTable);
    replay(groupedFactory, mockKTable);

    final List<Expression> groupByExpressions = Collections.singletonList(TEST_2_COL_1);
    final SchemaKTable schemaKTable = buildSchemaKTable(ksqlTable, mockKTable);

    // When:
    final SchemaKGroupedTable result =
        schemaKTable.groupBy(valueFormat, groupByExpressions, childContextStacker);

    // Then:
    result.getSourceTableStep().build(planBuilder);
    verify(mockKTable, groupedFactory);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void shouldGroupKeysCorrectly() {
    // set up a mock KTable and KGroupedTable for the test. Set up the KTable to
    // capture the mapper that is passed in to produce new keys
    final KTable mockKTable = mock(KTable.class);
    final KGroupedTable mockKGroupedTable = mock(KGroupedTable.class);
    final Capture<KeyValueMapper> capturedKeySelector = Capture.newInstance();
    expect(mockKTable.filter(anyObject(Predicate.class))).andReturn(mockKTable);
    expect(mockKTable.groupBy(capture(capturedKeySelector), anyObject(Grouped.class)))
        .andReturn(mockKGroupedTable);
    replay(mockKTable, mockKGroupedTable);

    // Build our test object from the mocks
    final String selectQuery = "SELECT col0, col1, col2 FROM test2 EMIT CHANGES;";
    final PlanNode logicalPlan = buildLogicalPlan(selectQuery);
    initialSchemaKTable = new SchemaKTable(
        buildSourceStep(logicalPlan.getTheSourceNode().getSchema(), mockKTable),
        logicalPlan.getTheSourceNode().getSchema(),
        keyFormat,
        logicalPlan.getTheSourceNode().getKeyField(),
        ksqlConfig,
        functionRegistry
    );

    final List<Expression> groupByExpressions = Arrays.asList(TEST_2_COL_2, TEST_2_COL_1);

    // Call groupBy and extract the captured mapper
    final SchemaKGroupedTable result = initialSchemaKTable.groupBy(
        valueFormat, groupByExpressions, childContextStacker);
    result.getSourceTableStep().build(planBuilder);
    verify(mockKTable, mockKGroupedTable);
    final KeyValueMapper keySelector = capturedKeySelector.getValue();
    final GenericRow value = new GenericRow(Arrays.asList("key", 0, 100, "foo", "bar"));
    final KeyValue<String, GenericRow> keyValue =
        (KeyValue<String, GenericRow>) keySelector.apply("key", value);

    // Validate that the captured mapper produces the correct key
    assertThat(keyValue.key, equalTo(StructKeyUtil.asStructKey("bar|+|foo")));
    assertThat(keyValue.value, equalTo(value));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void shouldPerformTableToTableLeftJoin() {
    expect(mockKTable.leftJoin(eq(secondKTable),
                               anyObject(KsqlValueJoiner.class)))
        .andReturn(EasyMock.niceMock(KTable.class));

    replay(mockKTable);

    final SchemaKStream joinedKStream = firstSchemaKTable
        .leftJoin(
            secondSchemaKTable,
            validKeyField,
            childContextStacker);

    ((SchemaKTable) joinedKStream).getSourceTableStep().build(planBuilder);
    verify(mockKTable);
    assertThat(joinedKStream, instanceOf(SchemaKTable.class));
    assertEquals(joinSchema, joinedKStream.getSchema());
    assertThat(joinedKStream.getKeyField(), is(validKeyField));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void shouldPerformTableToTableInnerJoin() {
    expect(mockKTable.join(eq(secondKTable),
                           anyObject(KsqlValueJoiner.class)))
        .andReturn(EasyMock.niceMock(KTable.class));

    replay(mockKTable);

    final SchemaKStream joinedKStream = firstSchemaKTable
        .join(secondSchemaKTable,
            validKeyField,
            childContextStacker);

    ((SchemaKTable) joinedKStream).getSourceTableStep().build(planBuilder);
    verify(mockKTable);
    assertThat(joinedKStream, instanceOf(SchemaKTable.class));
    assertEquals(joinSchema, joinedKStream.getSchema());
    assertThat(joinedKStream.getKeyField(), is(validKeyField));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void shouldPerformTableToTableOuterJoin() {
    expect(mockKTable.outerJoin(eq(secondKTable),
                                anyObject(KsqlValueJoiner.class)))
        .andReturn(EasyMock.niceMock(KTable.class));

    replay(mockKTable);

    final SchemaKStream joinedKStream = firstSchemaKTable
        .outerJoin(secondSchemaKTable,
            validKeyField,
            childContextStacker);

    ((SchemaKTable) joinedKStream).getSourceTableStep().build(planBuilder);
    verify(mockKTable);
    assertThat(joinedKStream, instanceOf(SchemaKTable.class));
    assertEquals(joinSchema, joinedKStream.getSchema());
    assertThat(joinedKStream.getKeyField(), is(validKeyField));
  }

  interface Join {
    SchemaKTable join(
        SchemaKTable schemaKTable,
        KeyField keyField,
        QueryContext.Stacker contextStacker
    );
  }

  @Test
  public void shouldBuildStepForTableTableJoin() {
    // Given:
    givenJoin();
    givenOuterJoin();
    givenLeftJoin();
    final List<Pair<JoinType, Join>> cases = ImmutableList.of(
        Pair.of(JoinType.LEFT, firstSchemaKTable::leftJoin),
        Pair.of(JoinType.INNER, firstSchemaKTable::join),
        Pair.of(JoinType.OUTER, firstSchemaKTable::outerJoin)
    );

    for (final Pair<JoinType, Join> testCase : cases) {
      // When:
      final SchemaKTable result =
          testCase.right.join(secondSchemaKTable, validKeyField, childContextStacker);

      // Then:
      assertThat(
          result.getSourceTableStep(),
          equalTo(
              ExecutionStepFactory.tableTableJoin(
                  childContextStacker,
                  testCase.left,
                  firstSchemaKTable.getSourceTableStep(),
                  secondSchemaKTable.getSourceTableStep()
              )
          )
      );
    }
  }

  @Test
  public void shouldBuildSchemaForTableTableJoin() {
    // Given:
    givenJoin();
    givenOuterJoin();
    givenLeftJoin();
    final List<Pair<JoinType, Join>> cases = ImmutableList.of(
        Pair.of(JoinType.LEFT, firstSchemaKTable::leftJoin),
        Pair.of(JoinType.INNER, firstSchemaKTable::join),
        Pair.of(JoinType.OUTER, firstSchemaKTable::outerJoin)
    );

    for (final Pair<JoinType, Join> testCase : cases) {
      // When:
      final SchemaKTable result =
          testCase.right.join(secondSchemaKTable, validKeyField, childContextStacker);

      // Then:
      assertThat(result.getSchema(), is(schemaResolver.resolve(
          result.getSourceStep(), firstSchemaKTable.getSchema(), secondSchemaKTable.getSchema()))
      );
    }
  }

  @Test
  public void shouldUpdateKeyIfRenamed() {
    // Given:
    final List<SelectExpression> selectExpressions = givenInitialKTableOf(
        "SELECT col0 as NEWKEY, col2, col3 FROM test1 EMIT CHANGES;");

    // When:
    final SchemaKTable result = initialSchemaKTable
        .select(selectExpressions, childContextStacker, queryBuilder);

    assertThat(result.getKeyField(),
        is(KeyField.of(ColumnRef.withoutSource(ColumnName.of("NEWKEY")))));
  }

  @Test
  public void shouldUpdateKeyIfRenamedViaFullyQualifiedName() {
    // Given:
    final List<SelectExpression> selectExpressions = givenInitialKTableOf(
        "SELECT test1.col0 as NEWKEY, col2, col3 FROM test1 EMIT CHANGES;");

    // When:
    final SchemaKTable result = initialSchemaKTable
        .select(selectExpressions, childContextStacker, queryBuilder);

    // Then:
    assertThat(result.getKeyField(),
        is(KeyField.of(ColumnRef.withoutSource(ColumnName.of("NEWKEY")))));
  }

  @Test
  public void shouldUpdateKeyIfRenamedAndSourceIsAliased() {
    // Given:
    final List<SelectExpression> selectExpressions = givenInitialKTableOf(
        "SELECT t.col0 as NEWKEY, col2, col3 FROM test1 t EMIT CHANGES;");

    // When:
    final SchemaKTable result = initialSchemaKTable
        .select(selectExpressions, childContextStacker, queryBuilder);

    // Then:
    assertThat(result.getKeyField(),
        is(KeyField.of(ColumnRef.withoutSource(ColumnName.of("NEWKEY")))));
  }

  @Test
  public void shouldPreserveKeyOnSelectStar() {
    // Given:
    final List<SelectExpression> selectExpressions = givenInitialKTableOf(
        "SELECT * FROM test1 EMIT CHANGES;");

    // When:
    final SchemaKTable result = initialSchemaKTable
        .select(selectExpressions, childContextStacker, queryBuilder);

    // Then:
    assertThat(result.getKeyField(), KeyFieldMatchers.hasName("COL0"));
  }

  @Test
  public void shouldUpdateKeyIfMovedToDifferentIndex() {
    // Given:
    final List<SelectExpression> selectExpressions = givenInitialKTableOf(
        "SELECT col2, col0, col3 FROM test1 EMIT CHANGES;");

    // When:
    final SchemaKTable result = initialSchemaKTable
        .select(selectExpressions, childContextStacker, queryBuilder);

    // Then:
    assertThat(result.getKeyField(),
        is(KeyField.of(ColumnRef.withoutSource(ColumnName.of("COL0")))));
  }

  @Test
  public void shouldDropKeyIfNotSelected() {
    // Given:
    final List<SelectExpression> selectExpressions = givenInitialKTableOf(
        "SELECT col2, col3 FROM test1 EMIT CHANGES;");

    // When:
    final SchemaKTable result = initialSchemaKTable
        .select(selectExpressions, childContextStacker, queryBuilder);

    // Then:
    assertThat(result.getKeyField(), is(KeyField.none()));
  }

  @Test
  public void shouldHandleSourceWithoutKey() {
    // Given:
    final List<SelectExpression> selectExpressions = givenInitialKTableOf(
        "SELECT * FROM test4 EMIT CHANGES;");

    // When:
    final SchemaKTable result = initialSchemaKTable
        .select(selectExpressions, childContextStacker, queryBuilder);

    // Then:
    assertThat(result.getKeyField(), is(KeyField.none()));
  }

  @Test
  public void shouldSetKeyOnGroupBySingleExpressionThatIsInProjection() {
    // Given:
    final List<SelectExpression> selectExpressions = givenInitialKTableOf(
        "SELECT * FROM test2 EMIT CHANGES;");

    final SchemaKTable selected = initialSchemaKTable
        .select(selectExpressions, childContextStacker, queryBuilder);

    final List<Expression> groupByExprs =  ImmutableList.of(TEST_2_COL_1);

    // When:
    final SchemaKGroupedTable result = selected
        .groupBy(valueFormat, groupByExprs, childContextStacker);

    // Then:
    assertThat(result.getKeyField(),
        is(KeyField.of(ColumnRef.withoutSource(ColumnName.of("COL1")))));
  }

  private LogicalSchema getJoinSchema(
      final LogicalSchema leftSchema,
      final LogicalSchema rightSchema
  ) {
    final LogicalSchema.Builder schemaBuilder = LogicalSchema.builder();
    final SourceName leftAlias = ksqlTable.getName();
    final SourceName rightAlias = secondKsqlTable.getName();
    for (final Column field : leftSchema.value()) {
      schemaBuilder.valueColumn(leftAlias, field.name(), field.type());
    }

    for (final Column field : rightSchema.value()) {
      schemaBuilder.valueColumn(rightAlias, field.name(), field.type());
    }
    return schemaBuilder.build();
  }

  private List<SelectExpression> givenInitialKTableOf(final String selectQuery) {
    final PlanNode logicalPlan = AnalysisTestUtil.buildLogicalPlan(
        ksqlConfig,
        selectQuery,
        metaStore
    );

    initialSchemaKTable = new SchemaKTable(
        buildSourceStep(logicalPlan.getTheSourceNode().getSchema(), kTable),
        logicalPlan.getTheSourceNode().getSchema(),
        keyFormat,
        logicalPlan.getTheSourceNode().getKeyField(),
        ksqlConfig,
        functionRegistry
    );

    final ProjectNode projectNode = (ProjectNode) logicalPlan.getSources().get(0);
    return projectNode.getSelectExpressions();
  }

  private PlanNode buildLogicalPlan(final String query) {
    return AnalysisTestUtil.buildLogicalPlan(ksqlConfig, query, metaStore);
  }

  private void givenJoin() {
    final KTable resultTable = EasyMock.niceMock(KTable.class);
    expect(mockKTable.join(
        eq(secondKTable),
        anyObject(KsqlValueJoiner.class))
    ).andReturn(resultTable);
  }

  private void givenOuterJoin() {
    final KTable resultTable = EasyMock.niceMock(KTable.class);
    expect(mockKTable.outerJoin(
        eq(secondKTable),
        anyObject(KsqlValueJoiner.class))
    ).andReturn(resultTable);
  }

  private void givenLeftJoin() {
    final KTable resultTable = EasyMock.niceMock(KTable.class);
    expect(mockKTable.leftJoin(
        eq(secondKTable),
        anyObject(KsqlValueJoiner.class))
    ).andReturn(resultTable);
  }
}
