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

import static java.util.Collections.emptyList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.builder.KsqlQueryBuilder;
import io.confluent.ksql.execution.context.QueryContext;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.expression.tree.FunctionCall;
import io.confluent.ksql.execution.expression.tree.QualifiedName;
import io.confluent.ksql.execution.expression.tree.QualifiedNameReference;
import io.confluent.ksql.execution.plan.DefaultExecutionStepProperties;
import io.confluent.ksql.execution.plan.ExecutionStep;
import io.confluent.ksql.execution.plan.Formats;
import io.confluent.ksql.execution.streams.ExecutionStepFactory;
import io.confluent.ksql.execution.streams.MaterializedFactory;
import io.confluent.ksql.execution.streams.StreamsUtil;
import io.confluent.ksql.function.InternalFunctionRegistry;
import io.confluent.ksql.function.KsqlAggregateFunction;
import io.confluent.ksql.function.TableAggregationFunction;
import io.confluent.ksql.function.udaf.KudafInitializer;
import io.confluent.ksql.logging.processing.NoopProcessingLogContext;
import io.confluent.ksql.logging.processing.ProcessingLogContext;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.model.KeyField;
import io.confluent.ksql.metastore.model.KsqlTable;
import io.confluent.ksql.parser.tree.WindowExpression;
import io.confluent.ksql.planner.plan.PlanNode;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.schema.ksql.Column;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.PersistenceSchema;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.serde.Format;
import io.confluent.ksql.serde.FormatInfo;
import io.confluent.ksql.serde.GenericRowSerDe;
import io.confluent.ksql.serde.KeyFormat;
import io.confluent.ksql.serde.KeySerde;
import io.confluent.ksql.serde.SerdeOption;
import io.confluent.ksql.serde.ValueFormat;
import io.confluent.ksql.testutils.AnalysisTestUtil;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.MetaStoreFixture;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Initializer;
import org.apache.kafka.streams.kstream.KGroupedTable;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@SuppressWarnings("unchecked")
@RunWith(MockitoJUnitRunner.class)
public class SchemaKGroupedTableTest {
  private final KsqlConfig ksqlConfig = new KsqlConfig(Collections.emptyMap());
  private final InternalFunctionRegistry functionRegistry = new InternalFunctionRegistry();
  private final ProcessingLogContext processingLogContext = ProcessingLogContext.create();
  private final KGroupedTable mockKGroupedTable = mock(KGroupedTable.class);
  private final LogicalSchema schema = LogicalSchema.builder()
      .valueColumn("GROUPING_COLUMN", SqlTypes.STRING)
      .valueColumn("AGG_VALUE", SqlTypes.INTEGER)
      .build();
  private final MaterializedFactory materializedFactory = mock(MaterializedFactory.class);
  private final MetaStore metaStore = MetaStoreFixture.getNewMetaStore(new InternalFunctionRegistry());
  private final QueryContext.Stacker queryContext
      = new QueryContext.Stacker(new QueryId("query")).push("node");
  private final ValueFormat valueFormat = ValueFormat.of(FormatInfo.of(Format.JSON));
  private final KeyFormat keyFormat = KeyFormat.nonWindowed(FormatInfo.of(Format.JSON));

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Mock
  private KeySerde<Struct> keySerde;
  @Mock
  private LogicalSchema aggregateSchema;
  @Mock
  private Initializer initializer;
  @Mock
  private Serde<GenericRow> topicValueSerDe;
  @Mock
  private FunctionCall aggCall1;
  @Mock
  private FunctionCall aggCall2;
  @Mock
  private Column field;
  @Mock
  private KsqlAggregateFunction otherFunc;
  @Mock
  private TableAggregationFunction tableFunc;
  @Mock
  private KsqlQueryBuilder queryBuilder;
  @Mock
  private KTable table;

  private KTable kTable;
  private KsqlTable<?> ksqlTable;
  private Map<Integer, KsqlAggregateFunction> someUdfs;

  @Before
  public void init() {
    ksqlTable = (KsqlTable) metaStore.getSource("TEST2");
    final StreamsBuilder builder = new StreamsBuilder();

    final Serde<GenericRow> rowSerde = GenericRowSerDe.from(
        ksqlTable.getKsqlTopic().getValueFormat().getFormatInfo(),
        PersistenceSchema.from(ksqlTable.getSchema().valueConnectSchema(), false),
        new KsqlConfig(Collections.emptyMap()),
        MockSchemaRegistryClient::new,
        "",
        NoopProcessingLogContext.INSTANCE
    );

    kTable = builder.table(
        ksqlTable.getKsqlTopic().getKafkaTopicName(),
        Consumed.with(Serdes.String(), rowSerde)
    );

    when(queryBuilder.getFunctionRegistry()).thenReturn(functionRegistry);
    when(queryBuilder.getKsqlConfig()).thenReturn(ksqlConfig);

    when(aggregateSchema.findValueColumn("GROUPING_COLUMN"))
        .thenReturn(Optional.of(Column.of("GROUPING_COLUMN", SqlTypes.STRING)));

    when(aggregateSchema.value()).thenReturn(ImmutableList.of(mock(Column.class)));

    when(mockKGroupedTable.aggregate(any(), any(), any(), any())).thenReturn(table);
    when(table.mapValues(any(ValueMapper.class))).thenReturn(table);

    someUdfs = ImmutableMap.of(0, tableFunc);
  }

  private <S> ExecutionStep<S> buildSourceTableStep(final LogicalSchema schema) {
    final ExecutionStep<S> step = mock(ExecutionStep.class);
    when(step.getProperties()).thenReturn(
        new DefaultExecutionStepProperties(schema, queryContext.getQueryContext())
    );
    when(step.getSchema()).thenReturn(schema);
    return step;
  }

  private SchemaKGroupedTable buildSchemaKGroupedTableFromQuery(
      final String query,
      final String...groupByColumns
  ) {
    when(keySerde.rebind(any(PersistenceSchema.class))).thenReturn(keySerde);

    final PlanNode logicalPlan = AnalysisTestUtil.buildLogicalPlan(ksqlConfig, query, metaStore);

    final SchemaKTable<?> initialSchemaKTable = new SchemaKTable(
        kTable,
        buildSourceTableStep(logicalPlan.getTheSourceNode().getSchema()),
        keyFormat,
        keySerde,
        logicalPlan.getTheSourceNode().getKeyField(),
        new ArrayList<>(),
        SchemaKStream.Type.SOURCE,
        ksqlConfig,
        functionRegistry);

    final List<Expression> groupByExpressions =
        Arrays.stream(groupByColumns)
            .map(c -> new QualifiedNameReference(QualifiedName.of("TEST1", c)))
            .collect(Collectors.toList());

    final SchemaKGroupedStream groupedSchemaKTable = initialSchemaKTable.groupBy(
        valueFormat, groupByExpressions, queryContext, queryBuilder);
    Assert.assertThat(groupedSchemaKTable, instanceOf(SchemaKGroupedTable.class));
    return (SchemaKGroupedTable)groupedSchemaKTable;
  }

  @Test
  public void shouldFailWindowedTableAggregation() {
    // Given:
    final WindowExpression windowExp = mock(WindowExpression.class);

    final SchemaKGroupedTable groupedTable =
        buildSchemaKGroupedTable(mockKGroupedTable, materializedFactory);

    // Then:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("Windowing not supported for table aggregations.");

    // When:
    groupedTable.aggregate(
        aggregateSchema,
        initializer,
        0,
        emptyList(),
        someUdfs,
        Optional.of(windowExp),
        valueFormat,
        topicValueSerDe,
        queryContext
    );
  }

  @Test
  public void shouldFailUnsupportedAggregateFunction() {
    final SchemaKGroupedTable kGroupedTable = buildSchemaKGroupedTableFromQuery(
        "SELECT col0, col1, col2 FROM test1 EMIT CHANGES;", "COL1", "COL2");
    final InternalFunctionRegistry functionRegistry = new InternalFunctionRegistry();
    try {
      final Map<Integer, KsqlAggregateFunction> aggValToFunctionMap = new HashMap<>();
      aggValToFunctionMap.put(
          0, functionRegistry.getAggregate("MAX", Schema.OPTIONAL_INT64_SCHEMA));
      aggValToFunctionMap.put(
          1, functionRegistry.getAggregate("MIN", Schema.OPTIONAL_INT64_SCHEMA));

      givenAggregateSchemaFieldCount(aggValToFunctionMap.size() + 1);

      kGroupedTable.aggregate(
          aggregateSchema,
          new KudafInitializer(1),
          1,
          ImmutableList.of(aggCall1, aggCall2),
          aggValToFunctionMap,
          Optional.empty(),
          valueFormat,
          GenericRowSerDe.from(
              FormatInfo.of(Format.JSON, Optional.empty()),
              PersistenceSchema.from(ksqlTable.getSchema().valueConnectSchema(), false),
              ksqlConfig,
              () -> null,
              "test",
              processingLogContext),
          queryContext
      );
      Assert.fail("Should fail to build topology for aggregation with unsupported function");
    } catch(final KsqlException e) {
      Assert.assertThat(
          e.getMessage(),
          equalTo(
              "The aggregation function(s) (MAX, MIN) cannot be applied to a table."));
    }
  }

  private SchemaKGroupedTable buildSchemaKGroupedTable(
      final KGroupedTable kGroupedTable,
      final MaterializedFactory materializedFactory
  ) {
    return new SchemaKGroupedTable(
        kGroupedTable,
        buildSourceTableStep(schema),
        keyFormat,
        keySerde,
        KeyField.of(schema.value().get(0).name(), schema.value().get(0)),
        Collections.emptyList(),
        ksqlConfig,
        functionRegistry,
        materializedFactory);
  }

  @Test
  public void shouldUseMaterializedFactoryForStateStore() {
    // Given:
    final Serde<GenericRow> valueSerde = mock(Serde.class);
    final Materialized materialized = MaterializedFactory.create(ksqlConfig).create(
        Serdes.String(),
        valueSerde,
        StreamsUtil.buildOpName(queryContext.getQueryContext()));

    when(materializedFactory.create(any(), any(), any())).thenReturn(materialized);

    final KTable mockKTable = mock(KTable.class);
    when(mockKGroupedTable.aggregate(any(), any(), any(), any())).thenReturn(mockKTable);

    final SchemaKGroupedTable groupedTable =
        buildSchemaKGroupedTable(mockKGroupedTable, materializedFactory);

    // When:
    groupedTable.aggregate(
        aggregateSchema,
        () -> null,
        0,
        emptyList(),
        someUdfs,
        Optional.empty(),
        valueFormat,
        valueSerde,
        queryContext);

    // Then:
    verify(materializedFactory).create(
        eq(keySerde),
        same(valueSerde),
        eq(StreamsUtil.buildOpName(queryContext.getQueryContext()))
    );

    verify(mockKGroupedTable).aggregate(
        any(),
        any(),
        any(),
        same(materialized)
    );
  }

  @Test
  public void shouldBuildStepForAggregate() {
    // Given:
    final Map<Integer, KsqlAggregateFunction> functions = ImmutableMap.of(1, tableFunc);
    final SchemaKGroupedTable groupedTable =
        buildSchemaKGroupedTable(mockKGroupedTable, materializedFactory);
    when(aggregateSchema.value()).thenReturn(
        ImmutableList.of(mock(Column.class), mock(Column.class)));

    // When:
    final SchemaKTable result = groupedTable.aggregate(
        aggregateSchema,
        initializer,
        1,
        ImmutableList.of(aggCall1),
        functions,
        Optional.empty(),
        valueFormat,
        topicValueSerDe,
        queryContext
    );

    // Then:
    assertThat(
        result.getSourceTableStep(),
        equalTo(
            ExecutionStepFactory.tableAggregate(
                queryContext,
                groupedTable.getSourceTableStep(),
                aggregateSchema,
                Formats.of(keyFormat, valueFormat, SerdeOption.none()),
                1,
                ImmutableList.of(aggCall1)
            )
        )
    );
  }

  @Test
  public void shouldReturnKTableWithAggregateSchema() {
    // Given:
    final SchemaKGroupedTable groupedTable =
        buildSchemaKGroupedTable(mockKGroupedTable, materializedFactory);

    // When:
    final SchemaKTable result = groupedTable.aggregate(
        aggregateSchema,
        initializer,
        0,
        emptyList(),
        someUdfs,
        Optional.empty(),
        valueFormat,
        topicValueSerDe,
        queryContext
    );

    // Then:
    assertThat(result.getSchema(), is(aggregateSchema));
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldThrowOnColumnCountMismatch() {
    // Given:
    final SchemaKGroupedTable groupedTable =
        buildSchemaKGroupedTable(mockKGroupedTable, materializedFactory);

    // Agg schema has 2 fields:
    givenAggregateSchemaFieldCount(2);

    // Where as params have 1 nonAgg and 2 agg fields:
    final Map<Integer, KsqlAggregateFunction> aggColumns = ImmutableMap.of(2, otherFunc);

    // When:
    groupedTable.aggregate(
        aggregateSchema,
        initializer,
        2,
        ImmutableList.of(aggCall1),
        aggColumns,
        Optional.empty(),
        valueFormat,
        topicValueSerDe,
        queryContext
    );
  }

  private void givenAggregateSchemaFieldCount(final int count) {
    final List<Column> valueFields = IntStream
        .range(0, count)
        .mapToObj(i -> field)
        .collect(Collectors.toList());

    when(aggregateSchema.value()).thenReturn(valueFields);
  }
}
