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

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.ksql.GenericKey;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.context.QueryContext;
import io.confluent.ksql.execution.ddl.commands.KsqlTopic;
import io.confluent.ksql.execution.expression.tree.ComparisonExpression;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.expression.tree.LongLiteral;
import io.confluent.ksql.execution.expression.tree.UnqualifiedColumnReferenceExp;
import io.confluent.ksql.execution.materialization.MaterializationInfo;
import io.confluent.ksql.execution.plan.ExecutionKeyFactory;
import io.confluent.ksql.execution.plan.ExecutionStep;
import io.confluent.ksql.execution.plan.Formats;
import io.confluent.ksql.execution.plan.JoinType;
import io.confluent.ksql.execution.plan.KTableHolder;
import io.confluent.ksql.execution.plan.PlanBuilder;
import io.confluent.ksql.execution.plan.PlanInfo;
import io.confluent.ksql.execution.plan.TableFilter;
import io.confluent.ksql.execution.runtime.RuntimeBuildContext;
import io.confluent.ksql.execution.streams.AggregateParamsFactory;
import io.confluent.ksql.execution.streams.ConsumedFactory;
import io.confluent.ksql.execution.streams.ExecutionStepFactory;
import io.confluent.ksql.execution.streams.GroupedFactory;
import io.confluent.ksql.execution.streams.JoinedFactory;
import io.confluent.ksql.execution.streams.KSPlanBuilder;
import io.confluent.ksql.execution.streams.KsqlValueJoiner;
import io.confluent.ksql.execution.runtime.MaterializedFactory;
import io.confluent.ksql.execution.streams.SqlPredicateFactory;
import io.confluent.ksql.execution.streams.StepSchemaResolver;
import io.confluent.ksql.execution.streams.StreamJoinedFactory;
import io.confluent.ksql.execution.streams.StreamsFactories;
import io.confluent.ksql.execution.streams.StreamsUtil;
import io.confluent.ksql.function.InternalFunctionRegistry;
import io.confluent.ksql.function.UserFunctionLoader;
import io.confluent.ksql.logging.processing.ProcessingLogContext;
import io.confluent.ksql.logging.processing.ProcessingLogger;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.model.KsqlTable;
import io.confluent.ksql.model.WindowType;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.planner.plan.FilterNode;
import io.confluent.ksql.planner.plan.PlanBuildContext;
import io.confluent.ksql.planner.plan.PlanNode;
import io.confluent.ksql.planner.plan.ProjectNode;
import io.confluent.ksql.schema.ksql.Column;
import io.confluent.ksql.schema.ksql.ColumnNames;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.PersistenceSchema;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.serde.FormatFactory;
import io.confluent.ksql.serde.FormatInfo;
import io.confluent.ksql.serde.GenericRowSerDe;
import io.confluent.ksql.serde.InternalFormats;
import io.confluent.ksql.serde.KeyFormat;
import io.confluent.ksql.serde.SerdeFeature;
import io.confluent.ksql.serde.SerdeFeatures;
import io.confluent.ksql.serde.ValueFormat;
import io.confluent.ksql.serde.WindowInfo;
import io.confluent.ksql.serde.json.JsonFormat;
import io.confluent.ksql.testutils.AnalysisTestUtil;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.MetaStoreFixture;
import io.confluent.ksql.util.Pair;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KGroupedTable;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Predicate;
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

  private static final ColumnName KEY = ColumnName.of("Bob");

  private final KsqlConfig ksqlConfig = new KsqlConfig(Collections.emptyMap());
  private InternalFunctionRegistry functionRegistry = new InternalFunctionRegistry();
  private final MetaStore metaStore =
      MetaStoreFixture.getNewMetaStore(functionRegistry);
  private final GroupedFactory groupedFactory = mock(GroupedFactory.class);

  private SchemaKTable<GenericKey> initialSchemaKTable;
  private KTable<String, GenericRow> kTable;
  private KTable<String, GenericRow> secondKTable;
  private KsqlTable<?> ksqlTable;
  private KsqlTable<?> secondKsqlTable;
  private KTable<String, GenericRow> mockKTable;
  private SchemaKTable<GenericKey> firstSchemaKTable;
  private SchemaKTable<GenericKey> secondSchemaKTable;
  private StepSchemaResolver schemaResolver;
  private final QueryContext.Stacker queryContext
      = new QueryContext.Stacker().push("node");
  private final QueryContext.Stacker childContextStacker = queryContext.push("child");
  private final ProcessingLogContext processingLogContext = ProcessingLogContext.create();
  private static final Expression TEST_2_COL_1 =
      new UnqualifiedColumnReferenceExp(ColumnName.of("COL1"));
  private static final Expression TEST_2_COL_2 =
      new UnqualifiedColumnReferenceExp(ColumnName.of("COL2"));
  private KeyFormat keyFormat = KeyFormat
      .nonWindowed(FormatInfo.of(FormatFactory.JSON.name()), SerdeFeatures.of());
  private static final ValueFormat valueFormat = ValueFormat
      .of(FormatInfo.of(FormatFactory.JSON.name()), SerdeFeatures.of());

  private PlanBuilder planBuilder;

  @Mock
  private PlanBuildContext buildContext;
  @Mock
  private RuntimeBuildContext executeContext;
  @Mock
  private ExecutionKeyFactory<Struct> executionKeyFactory;
  @Mock
  private ProcessingLogger processingLogger;
  @Mock
  private KsqlTopic topic;
  @Mock
  private PlanInfo planInfo;
  @Mock
  private MaterializationInfo.Builder materializationBuilder;
  @Mock
  private FormatInfo internalFormats;

  @Before
  public void init() {
    UserFunctionLoader.newInstance(
        ksqlConfig,
        functionRegistry,
        ".",
        new Metrics()
    ).load();
    schemaResolver = new StepSchemaResolver(ksqlConfig, functionRegistry);
    ksqlTable = (KsqlTable) metaStore.getSource(SourceName.of("TEST2"));
    final StreamsBuilder builder = new StreamsBuilder();
    kTable = builder.table(
        ksqlTable.getKsqlTopic().getKafkaTopicName(),
        Consumed.with(
            Serdes.String(),
            getRowSerde(ksqlTable.getKsqlTopic(), ksqlTable.getSchema())
        ));

    secondKsqlTable = (KsqlTable) metaStore.getSource(SourceName.of("TEST3"));
    secondKTable = builder.table(
        secondKsqlTable.getKsqlTopic().getKafkaTopicName(),
        Consumed.with(
            Serdes.String(),
            getRowSerde(secondKsqlTable.getKsqlTopic(), secondKsqlTable.getSchema())
        ));

    mockKTable = mock(KTable.class);
    firstSchemaKTable = buildSchemaKTableForJoin(ksqlTable, mockKTable);
    secondSchemaKTable = buildSchemaKTableForJoin(secondKsqlTable, secondKTable);

    when(executeContext.getKsqlConfig()).thenReturn(ksqlConfig);
    when(executeContext.getFunctionRegistry()).thenReturn(functionRegistry);
    when(executeContext.getProcessingLogger(any())).thenReturn(processingLogger);

    planBuilder = new KSPlanBuilder(
        executeContext,
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

    when(internalFormats.copyWithoutProperty(Mockito.anyString())).thenReturn(internalFormats);
  }

  private ExecutionStep buildSourceStep(final LogicalSchema schema, final KTable kTable) {
    final ExecutionStep sourceStep = mock(ExecutionStep.class);
    when(sourceStep.build(any(), eq(planInfo))).thenReturn(
        KTableHolder.materialized(kTable, schema, executionKeyFactory, materializationBuilder));
    return sourceStep;
  }

  private SchemaKTable buildSchemaKTable(
      final LogicalSchema schema,
      final KTable kTable) {
    return new SchemaKTable<>(
        buildSourceStep(schema, kTable),
        schema,
        keyFormat,
        ksqlConfig,
        functionRegistry
    );
  }

  private SchemaKTable buildSchemaKTableFromPlan(final PlanNode logicalPlan) {
    return new SchemaKTable(
        buildSourceStep(logicalPlan.getLeftmostSourceNode().getSchema(), kTable),
        logicalPlan.getLeftmostSourceNode().getSchema(),
        keyFormat,
        ksqlConfig,
        functionRegistry
    );
  }

  private SchemaKTable buildSchemaKTable(final KsqlTable ksqlTable, final KTable kTable) {
    final LogicalSchema schema = ksqlTable.getSchema();

    return buildSchemaKTable(
        schema,
        kTable
    );
  }

  private LogicalSchema buildJoinSchema(final KsqlTable table) {
    final LogicalSchema.Builder builder = LogicalSchema.builder();
    builder.keyColumns(table.getSchema().key());
    for (final Column c : table.getSchema().value()) {
      builder.valueColumn(ColumnNames.generatedJoinColumnAlias(table.getName(), c.name()), c.type());
    }
    return builder.build();
  }

  private SchemaKTable buildSchemaKTableForJoin(final KsqlTable ksqlTable, final KTable kTable) {
    return buildSchemaKTable(
        buildJoinSchema(ksqlTable),
        kTable
    );
  }

  private Serde<GenericRow> getRowSerde(final KsqlTopic topic, final LogicalSchema schema) {
    return GenericRowSerDe.from(
        topic.getValueFormat().getFormatInfo(),
        PersistenceSchema.from(schema.value(), SerdeFeatures.of()),
        new KsqlConfig(Collections.emptyMap()),
        MockSchemaRegistryClient::new,
        "test",
        processingLogContext);
  }

  @Test
  public void shouldBuildSchemaForSelect() {
    // Given:
    final String selectQuery = "SELECT col0 AS K, col2, col3 FROM test2 WHERE col0 > 100 EMIT CHANGES;";
    final PlanNode logicalPlan = buildLogicalPlan(selectQuery);
    final ProjectNode projectNode = (ProjectNode) logicalPlan.getSources().get(0);
    initialSchemaKTable = buildSchemaKTableFromPlan(logicalPlan);

    // When:
    final SchemaKTable<?> projectedSchemaKStream = initialSchemaKTable.select(
        ImmutableList.of(ColumnName.of("K")),
        projectNode.getSelectExpressions(),
        childContextStacker,
        buildContext,
        internalFormats
    );

    // Then:
    assertThat(
        projectedSchemaKStream.getSchema(),
        is(schemaResolver.resolve(
            projectedSchemaKStream.getSourceStep(), initialSchemaKTable.getSchema()))
    );
  }

  @Test
  public void shouldBuildSchemaForSelectKey() {
    // Given:
    final String selectQuery = "SELECT col0 AS K, col2, col3 FROM test2 WHERE col0 > 100 EMIT CHANGES;";
    final PlanNode logicalPlan = buildLogicalPlan(selectQuery);
    initialSchemaKTable = buildSchemaKTableFromPlan(logicalPlan);

    // When:
    final SchemaKTable<?> resultSchemaKTable = initialSchemaKTable.selectKey(
        valueFormat.getFormatInfo(),
        ImmutableList.of(new UnqualifiedColumnReferenceExp(ColumnName.of("COL0"))),
        Optional.empty(),
        childContextStacker,
        true
    );

    // Then:
    assertThat(
        resultSchemaKTable.getSchema(),
        is(schemaResolver.resolve(
            resultSchemaKTable.getSourceStep(), initialSchemaKTable.getSchema()))
    );
  }

  @Test
  public void shouldBuildStepForSelectKey() {
    // Given:
    final String selectQuery = "SELECT col0, col2, col3 FROM test2 WHERE col0 > 100 EMIT CHANGES;";
    final PlanNode logicalPlan = buildLogicalPlan(selectQuery);
    initialSchemaKTable = buildSchemaKTableFromPlan(logicalPlan);

    // When:
    final SchemaKTable<?> resultSchemaKTable = initialSchemaKTable.selectKey(
        valueFormat.getFormatInfo(),
        ImmutableList.of(new UnqualifiedColumnReferenceExp(ColumnName.of("COL0"))),
        Optional.empty(),
        childContextStacker,
        true
    );

    // Then:
    assertThat(
        resultSchemaKTable.getSourceTableStep(),
        equalTo(
            ExecutionStepFactory.tableSelectKey(
                childContextStacker,
                initialSchemaKTable.getSourceTableStep(),
                InternalFormats.of(
                    keyFormat.withSerdeFeatures(SerdeFeatures.of(SerdeFeature.UNWRAP_SINGLES)),
                    valueFormat.getFormatInfo()),
                ImmutableList.of(new UnqualifiedColumnReferenceExp(ColumnName.of("COL0")))
            )
        )
    );
  }

  @Test
  public void shouldFailSelectKeyForceRepartitionOnNonKeyColumn() {
    // Given:
    final String selectQuery = "SELECT col0, col2, col3 FROM test2 WHERE col0 > 100 EMIT CHANGES;";
    final PlanNode logicalPlan = buildLogicalPlan(selectQuery);
    initialSchemaKTable = buildSchemaKTableFromPlan(logicalPlan);

    // When:
    final UnsupportedOperationException e = assertThrows(
        UnsupportedOperationException.class,
        () -> initialSchemaKTable.selectKey(
            valueFormat.getFormatInfo(),
            ImmutableList.of(new UnqualifiedColumnReferenceExp(ColumnName.of("COL1"))),
            Optional.empty(),
            childContextStacker,
            true
    ));

    // Then:
    assertThat(e.getMessage(), containsString("Cannot repartition a TABLE source."));
  }

  @Test
  public void shouldFailSelectKeyIfNotForced() {
    // Given:
    final String selectQuery = "SELECT col0 AS K, col2, col3 FROM test2 WHERE col0 > 100 EMIT CHANGES;";
    final PlanNode logicalPlan = buildLogicalPlan(selectQuery);
    initialSchemaKTable = buildSchemaKTableFromPlan(logicalPlan);

    // When:
    final Exception e = assertThrows(
        UnsupportedOperationException.class,
        () -> initialSchemaKTable.selectKey(
            valueFormat.getFormatInfo(),
            ImmutableList.of(new UnqualifiedColumnReferenceExp(ColumnName.of("COL1"))),
            Optional.empty(),
            childContextStacker,
            false
    ));

    // Then:
    assertThat(
        e.getMessage(), containsString("Cannot repartition a TABLE source.")
    );
  }

  @Test
  public void testSelectWithExpression() {
    // Given:
    final String selectQuery = "SELECT col0, col3*3+5 FROM test2 WHERE col0 > 100 EMIT CHANGES;";
    final PlanNode logicalPlan = buildLogicalPlan(selectQuery);
    final ProjectNode projectNode = (ProjectNode) logicalPlan.getSources().get(0);
    initialSchemaKTable = buildSchemaKTableFromPlan(logicalPlan);

    // When:
    final SchemaKTable<?> projectedSchemaKStream = initialSchemaKTable.select(ImmutableList.of(),
        projectNode.getSelectExpressions(), childContextStacker, buildContext, internalFormats);

    // Then:
    assertThat(projectedSchemaKStream.getSchema(),
        is(LogicalSchema.builder().keyColumn(ColumnName.of("COL0"), SqlTypes.BIGINT)
            .valueColumn(ColumnName.of("COL0"), SqlTypes.BIGINT)
            .valueColumn(ColumnName.of("KSQL_COL_0"), SqlTypes.DOUBLE).build()));
  }

  @Test
  public void testSelectWithFunctions() {
    // Given:
    final String selectQuery =
        "SELECT col0, LEN(UCASE(col2)) FROM test2 WHERE col0 > 100 EMIT CHANGES;";
    final PlanNode logicalPlan = buildLogicalPlan(selectQuery);
    final ProjectNode projectNode = (ProjectNode) logicalPlan.getSources().get(0);
    initialSchemaKTable = buildSchemaKTableFromPlan(logicalPlan);

    // When:
    final SchemaKTable<?> projectedSchemaKStream = initialSchemaKTable.select(
        ImmutableList.of(),
        projectNode.getSelectExpressions(),
        childContextStacker,
        buildContext,
        internalFormats
    );

    // Then:
    assertThat(projectedSchemaKStream.getSchema(), is(LogicalSchema.builder()
        .keyColumn(ColumnName.of("COL0"), SqlTypes.BIGINT)
        .valueColumn(ColumnName.of("COL0"), SqlTypes.BIGINT)
        .valueColumn(ColumnName.of("KSQL_COL_0"), SqlTypes.INTEGER)
        .build()
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
    final SchemaKTable<?> filteredSchemaKStream = initialSchemaKTable.filter(
        filterNode.getPredicate(),
        childContextStacker
    );

    // Then:
    assertThat(filteredSchemaKStream.getSchema(), is(LogicalSchema.builder()
        .keyColumn(ColumnName.of("COL0"), SqlTypes.BIGINT)
        .valueColumn(ColumnName.of("COL1"), SqlTypes.STRING)
        .valueColumn(ColumnName.of("COL2"), SqlTypes.STRING)
        .valueColumn(ColumnName.of("COL3"), SqlTypes.DOUBLE)
        .valueColumn(ColumnName.of("COL4"), SqlTypes.BOOLEAN)
        .valueColumn(ColumnName.of("ROWTIME"), SqlTypes.BIGINT)
        .valueColumn(ColumnName.of("ROWPARTITION"), SqlTypes.INTEGER)
        .valueColumn(ColumnName.of("ROWOFFSET"), SqlTypes.BIGINT)
        .valueColumn(ColumnName.of("COL0"), SqlTypes.BIGINT)
        .build()
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
    final SchemaKTable<?> filteredSchemaKTable = initialSchemaKTable.filter(
        filterNode.getPredicate(),
        childContextStacker
    );

    // Then:
    final TableFilter<?> step = (TableFilter) filteredSchemaKTable.getSourceTableStep();
    assertThat(
        step.getFilterExpression(),
        Matchers.equalTo(
            new ComparisonExpression(
                ComparisonExpression.Type.EQUAL,
                new UnqualifiedColumnReferenceExp(ColumnName.of("ROWTIME")),
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
    final SchemaKTable<?> filteredSchemaKStream = initialSchemaKTable.filter(
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
        valueFormat.getFormatInfo(),
        groupByExpressions,
        childContextStacker
    );

    // Then:
    assertThat(groupedSchemaKTable, instanceOf(SchemaKGroupedTable.class));
  }

  @Test
  public void shouldBuildStepForGroupBy() {
    // Given:
    keyFormat = KeyFormat
        .nonWindowed(FormatInfo.of(FormatFactory.KAFKA.name()), SerdeFeatures.of());
    final String selectQuery = "SELECT col0, col1, col2 FROM test2 EMIT CHANGES;";
    final PlanNode logicalPlan = buildLogicalPlan(selectQuery);
    initialSchemaKTable = buildSchemaKTableFromPlan(logicalPlan);
    final List<Expression> groupByExpressions = Arrays.asList(TEST_2_COL_2, TEST_2_COL_1);

    // When:
    final SchemaKGroupedTable groupedSchemaKTable = initialSchemaKTable.groupBy(
        valueFormat.getFormatInfo(),
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
                Formats.of(
                    FormatInfo.of(JsonFormat.NAME), // key format is updated to supported multiple grouping expressions
                    valueFormat.getFormatInfo(),
                    SerdeFeatures.of(),
                    SerdeFeatures.of()
                ),
                groupByExpressions
            )
        )
    );
  }

  @Test
  public void shouldBuildStepForGroupByBasedOnKeySerdeFeatures() {
    // Given:
    keyFormat = KeyFormat.nonWindowed(FormatInfo.of(FormatFactory.JSON.name()), SerdeFeatures.of(SerdeFeature.UNWRAP_SINGLES));
    final String selectQuery = "SELECT col0, col1, col2 FROM test2 EMIT CHANGES;";
    final PlanNode logicalPlan = buildLogicalPlan(selectQuery);
    initialSchemaKTable = buildSchemaKTableFromPlan(logicalPlan);
    final List<Expression> groupByExpressions = Arrays.asList(TEST_2_COL_2, TEST_2_COL_1);

    // When:
    final SchemaKGroupedTable groupedSchemaKTable = initialSchemaKTable.groupBy(
        valueFormat.getFormatInfo(),
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
                Formats.of(
                    initialSchemaKTable.keyFormat.getFormatInfo(),
                    valueFormat.getFormatInfo(),
                    SerdeFeatures.of(),
                    SerdeFeatures.of()
                ),
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
        valueFormat.getFormatInfo(),
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
        getRowSerde(ksqlTable.getKsqlTopic(), ksqlTable.getSchema());
    when(executeContext.buildValueSerde(any(), any(), any())).thenReturn(valSerde);
    final Grouped<Object, GenericRow> grouped = mock(Grouped.class);
    when(
        groupedFactory.create(
            eq(StreamsUtil.buildOpName(childContextStacker.getQueryContext())),
            any(),
            same(valSerde))
    ).thenReturn(grouped);
    when(mockKTable.filter(any(Predicate.class))).thenReturn(mockKTable);
    final KGroupedTable<Object, GenericRow> groupedTable = mock(KGroupedTable.class);
    when(mockKTable.groupBy(any(), same(grouped))).thenReturn(groupedTable);

    final List<Expression> groupByExpressions = Collections.singletonList(TEST_2_COL_1);
    final SchemaKTable<?> schemaKTable = buildSchemaKTable(ksqlTable, mockKTable);

    // When:
    final SchemaKGroupedTable result =
        schemaKTable.groupBy(valueFormat.getFormatInfo(), groupByExpressions, childContextStacker);

    // Then:
    result.getSourceTableStep().build(planBuilder, planInfo);
    verify(groupedFactory).create(
        eq(StreamsUtil.buildOpName(childContextStacker.getQueryContext())),
        any(),
        same(valSerde));
    verify(mockKTable).filter(any(Predicate.class));
    verify(mockKTable).groupBy(any(), same(grouped));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void shouldPerformTableToTableLeftJoin() {
    when(mockKTable.leftJoin(eq(secondKTable),
        any(KsqlValueJoiner.class)))
        .thenReturn(mock(KTable.class));

    final SchemaKStream<?> joinedKStream = firstSchemaKTable
        .leftJoin(secondSchemaKTable, KEY, childContextStacker);

    ((SchemaKTable) joinedKStream).getSourceTableStep().build(planBuilder, planInfo);
    verify(mockKTable).leftJoin(eq(secondKTable),
        any(KsqlValueJoiner.class));
    assertThat(joinedKStream, instanceOf(SchemaKTable.class));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void shouldPerformTableToTableInnerJoin() {
    when(mockKTable.join(eq(secondKTable),
        any(KsqlValueJoiner.class)))
        .thenReturn(mock(KTable.class));

    final SchemaKStream<?> joinedKStream = firstSchemaKTable
        .innerJoin(secondSchemaKTable, KEY, childContextStacker);

    ((SchemaKTable) joinedKStream).getSourceTableStep().build(planBuilder, planInfo);
    verify(mockKTable).join(eq(secondKTable),
        any(KsqlValueJoiner.class));
    assertThat(joinedKStream, instanceOf(SchemaKTable.class));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void shouldPerformTableToTableOuterJoin() {
    when(mockKTable.outerJoin(eq(secondKTable),
        any(KsqlValueJoiner.class)))
        .thenReturn(mock(KTable.class));

    final SchemaKStream<?> joinedKStream = firstSchemaKTable
        .outerJoin(secondSchemaKTable, ColumnName.of("KEY"), childContextStacker);

    ((SchemaKTable) joinedKStream).getSourceTableStep().build(planBuilder, planInfo);
    verify(mockKTable).outerJoin(eq(secondKTable),
        any(KsqlValueJoiner.class));
    assertThat(joinedKStream, instanceOf(SchemaKTable.class));
  }

  @FunctionalInterface
  private interface Join {

    SchemaKTable join(
        SchemaKTable schemaKTable,
        ColumnName keyColName,
        QueryContext.Stacker contextStacker
    );
  }

  @Test
  public void shouldBuildStepForTableTableJoin() {
    // Given:
    final List<Pair<JoinType, Join>> cases = ImmutableList.of(
        Pair.of(JoinType.LEFT, firstSchemaKTable::leftJoin),
        Pair.of(JoinType.INNER, firstSchemaKTable::innerJoin)
    );

    for (final Pair<JoinType, Join> testCase : cases) {
      // When:
      final SchemaKTable<?> result = testCase.right
          .join(secondSchemaKTable, KEY, childContextStacker);

      // Then:
      assertThat(
          result.getSourceTableStep(),
          equalTo(
              ExecutionStepFactory.tableTableJoin(
                  childContextStacker,
                  testCase.left,
                  KEY,
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
    final List<Pair<JoinType, Join>> cases = ImmutableList.of(
        Pair.of(JoinType.LEFT, firstSchemaKTable::leftJoin),
        Pair.of(JoinType.INNER, firstSchemaKTable::innerJoin)
    );

    for (final Pair<JoinType, Join> testCase : cases) {
      // When:
      final SchemaKTable<?> result = testCase.right
          .join(secondSchemaKTable, KEY, childContextStacker);

      // Then:
      assertThat(result.getSchema(), is(schemaResolver.resolve(
          result.getSourceStep(), firstSchemaKTable.getSchema(), secondSchemaKTable.getSchema()))
      );
    }
  }

  @Test
  public void shouldThrowOnIntoIfKeyFormatWindowInfoIsDifferent() {
    // Given:
    final SchemaKTable<?> table = buildSchemaKTable(ksqlTable, mockKTable);

    when(topic.getKeyFormat()).thenReturn(KeyFormat.windowed(
        keyFormat.getFormatInfo(),
        SerdeFeatures.of(),
        WindowInfo.of(WindowType.SESSION, Optional.empty(), Optional.empty())
    ));

    // When:
    assertThrows(
        IllegalArgumentException.class,
        () -> table.into(topic, childContextStacker, Optional.empty())
    );
  }

  private PlanNode buildLogicalPlan(final String query) {
    return AnalysisTestUtil.buildLogicalPlan(ksqlConfig, query, metaStore);
  }
}
