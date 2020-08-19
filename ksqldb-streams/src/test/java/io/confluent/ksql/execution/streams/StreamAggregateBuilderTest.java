/*
 * Copyright 2019 Confluent Inc.
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

package io.confluent.ksql.execution.streams;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.builder.KsqlQueryBuilder;
import io.confluent.ksql.execution.context.QueryContext;
import io.confluent.ksql.execution.expression.tree.FunctionCall;
import io.confluent.ksql.execution.expression.tree.UnqualifiedColumnReferenceExp;
import io.confluent.ksql.execution.function.udaf.KudafAggregator;
import io.confluent.ksql.execution.function.udaf.KudafInitializer;
import io.confluent.ksql.execution.materialization.MaterializationInfo;
import io.confluent.ksql.execution.materialization.MaterializationInfo.MapperInfo;
import io.confluent.ksql.execution.plan.ExecutionStep;
import io.confluent.ksql.execution.plan.ExecutionStepPropertiesV1;
import io.confluent.ksql.execution.plan.Formats;
import io.confluent.ksql.execution.plan.KGroupedStreamHolder;
import io.confluent.ksql.execution.plan.KTableHolder;
import io.confluent.ksql.execution.plan.KeySerdeFactory;
import io.confluent.ksql.execution.plan.PlanBuilder;
import io.confluent.ksql.execution.plan.StreamAggregate;
import io.confluent.ksql.execution.plan.StreamWindowedAggregate;
import io.confluent.ksql.execution.transform.KsqlProcessingContext;
import io.confluent.ksql.execution.transform.KsqlTransformer;
import io.confluent.ksql.execution.windows.HoppingWindowExpression;
import io.confluent.ksql.execution.windows.SessionWindowExpression;
import io.confluent.ksql.execution.windows.TumblingWindowExpression;
import io.confluent.ksql.execution.windows.WindowTimeClause;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.FunctionName;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.PhysicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.serde.FormatFactory;
import io.confluent.ksql.serde.FormatInfo;
import io.confluent.ksql.serde.SerdeOptions;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Merger;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.SessionWindowedKStream;
import org.apache.kafka.streams.kstream.SessionWindows;
import org.apache.kafka.streams.kstream.TimeWindowedKStream;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.Windows;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.SessionStore;
import org.apache.kafka.streams.state.WindowStore;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.Spy;
import org.mockito.junit.MockitoJUnitRunner;

@SuppressWarnings("unchecked")
@RunWith(MockitoJUnitRunner.class)
public class StreamAggregateBuilderTest {

  private static final LogicalSchema INPUT_SCHEMA = LogicalSchema.builder()
      .valueColumn(ColumnName.of("REQUIRED0"), SqlTypes.BIGINT)
      .valueColumn(ColumnName.of("REQUIRED1"), SqlTypes.STRING)
      .valueColumn(ColumnName.of("ARGUMENT0"), SqlTypes.INTEGER)
      .valueColumn(ColumnName.of("ARGUMENT1"), SqlTypes.DOUBLE)
      .build();
  private static final LogicalSchema AGGREGATE_SCHEMA = LogicalSchema.builder()
      .valueColumn(ColumnName.of("REQUIRED0"), SqlTypes.BIGINT)
      .valueColumn(ColumnName.of("REQUIRED1"), SqlTypes.STRING)
      .valueColumn(ColumnName.of("RESULT0"), SqlTypes.BIGINT)
      .valueColumn(ColumnName.of("RESULT1"), SqlTypes.STRING)
      .build();
  private static final LogicalSchema OUTPUT_SCHEMA = LogicalSchema.builder()
      .valueColumn(ColumnName.of("REQUIRED0"), SqlTypes.BIGINT)
      .valueColumn(ColumnName.of("REQUIRED1"), SqlTypes.STRING)
      .valueColumn(ColumnName.of("OUTPUT0"), SqlTypes.BIGINT)
      .valueColumn(ColumnName.of("OUTPUT1"), SqlTypes.STRING)
      .build();
  private static final List<ColumnName> NON_AGG_COLUMNS = ImmutableList.of(
      INPUT_SCHEMA.value().get(0).name(),
      INPUT_SCHEMA.value().get(1).name()
  );
  private static final PhysicalSchema PHYSICAL_AGGREGATE_SCHEMA = PhysicalSchema.from(
      AGGREGATE_SCHEMA,
      SerdeOptions.of()
  );
  private static final FunctionCall AGG0 = new FunctionCall(
      FunctionName.of("AGG0"),
      ImmutableList.of(new UnqualifiedColumnReferenceExp(ColumnName.of("ARGUMENT0")))
  );
  private static final FunctionCall AGG1 = new FunctionCall(
      FunctionName.of("AGG1"),
      ImmutableList.of(new UnqualifiedColumnReferenceExp(ColumnName.of("ARGUMENT1")))
  );
  private static final List<FunctionCall> FUNCTIONS = ImmutableList.of(AGG0, AGG1);
  private static final QueryContext CTX =
      new QueryContext.Stacker().push("agg").push("regate").getQueryContext();
  private static final QueryContext MATERIALIZE_CTX = QueryContext.Stacker.of(CTX)
      .push("Materialize").getQueryContext();
  private static final FormatInfo KEY_FORMAT = FormatInfo.of(FormatFactory.KAFKA.name());
  private static final FormatInfo VALUE_FORMAT = FormatInfo.of(FormatFactory.JSON.name());
  private static final Duration WINDOW = Duration.ofMillis(30000);
  private static final Duration HOP = Duration.ofMillis(10000);

  @Mock
  private KGroupedStream<Struct, GenericRow> groupedStream;
  @Mock
  private KTable<Struct, GenericRow> aggregated;
  @Mock
  private KTable<Struct, GenericRow> aggregatedWithResults;
  @Mock
  private TimeWindowedKStream<Struct, GenericRow> timeWindowedStream;
  @Mock
  private SessionWindowedKStream<Struct, GenericRow> sessionWindowedStream;
  @Mock
  private KTable<Windowed<Struct>, GenericRow> windowed;
  @Mock
  private KTable<Windowed<Struct>, GenericRow> windowedWithResults;
  @Mock
  private KTable<Windowed<Struct>, GenericRow> windowedWithWindowBounds;
  @Mock
  private KsqlQueryBuilder queryBuilder;
  @Mock
  private FunctionRegistry functionRegistry;
  @Mock
  private AggregateParamsFactory aggregateParamsFactory;
  @Mock
  private AggregateParams aggregateParams;
  @Mock
  private KudafInitializer initializer;
  @Mock
  private KudafAggregator<Struct> aggregator;
  @Mock
  private KsqlTransformer<Struct, GenericRow> resultMapper;
  @Mock
  private Merger<Struct, GenericRow> merger;
  @Mock
  private MaterializedFactory materializedFactory;
  @Mock
  private Serde<Struct> keySerde;
  @Mock
  private Serde<GenericRow> valueSerde;
  @Mock
  private Materialized<Struct, GenericRow, KeyValueStore<Bytes, byte[]>> materialized;
  @Mock
  private Materialized<Struct, GenericRow, WindowStore<Bytes, byte[]>> timeWindowMaterialized;
  @Mock
  private Materialized<Struct, GenericRow, SessionStore<Bytes, byte[]>> sessionWindowMaterialized;
  @Mock
  private ExecutionStep<KGroupedStreamHolder> sourceStep;
  @Mock
  private KsqlProcessingContext ctx;
  @Spy
  private WindowTimeClause retentionClause = new WindowTimeClause(10, TimeUnit.SECONDS);
  @Spy
  private WindowTimeClause gracePeriodClause = new WindowTimeClause(0, TimeUnit.SECONDS);

  private PlanBuilder planBuilder;
  private StreamAggregate aggregate;
  private StreamWindowedAggregate windowedAggregate;

  @SuppressWarnings("unchecked")
  @Before
  public void init() {
    when(sourceStep.build(any())).thenReturn(KGroupedStreamHolder.of(groupedStream, INPUT_SCHEMA));
    when(queryBuilder.buildKeySerde(any(), any(), any())).thenReturn(keySerde);
    when(queryBuilder.buildValueSerde(any(), any(), any())).thenReturn(valueSerde);
    when(queryBuilder.getFunctionRegistry()).thenReturn(functionRegistry);
    when(aggregateParamsFactory.create(any(), any(), any(), any(), anyBoolean()))
        .thenReturn(aggregateParams);
    when(aggregateParams.getAggregator()).thenReturn((KudafAggregator) aggregator);
    when(aggregateParams.getAggregateSchema()).thenReturn(AGGREGATE_SCHEMA);
    when(aggregateParams.getSchema()).thenReturn(OUTPUT_SCHEMA);
    when(aggregator.getMerger()).thenReturn(merger);
    when(aggregator.getResultMapper()).thenReturn(resultMapper);
    when(aggregateParams.getInitializer()).thenReturn(initializer);

    planBuilder = new KSPlanBuilder(
        queryBuilder,
        mock(SqlPredicateFactory.class),
        aggregateParamsFactory,
        new StreamsFactories(
            mock(GroupedFactory.class),
            mock(JoinedFactory.class),
            materializedFactory,
            mock(StreamJoinedFactory.class),
            mock(ConsumedFactory.class)
        )
    );
  }

  @SuppressWarnings("unchecked")
  private void givenUnwindowedAggregate() {
    when(materializedFactory.<Struct, KeyValueStore<Bytes, byte[]>>create(any(), any(), any()))
        .thenReturn(materialized);
    when(groupedStream.aggregate(any(), any(), any(Materialized.class))).thenReturn(aggregated);
    when(aggregated.transformValues(any(), any(Named.class)))
        .thenReturn((KTable) aggregatedWithResults);
    aggregate = new StreamAggregate(
        new ExecutionStepPropertiesV1(CTX),
        sourceStep,
        Formats.of(KEY_FORMAT, VALUE_FORMAT, SerdeOptions.of()),
        NON_AGG_COLUMNS,
        FUNCTIONS
    );
  }

  @SuppressWarnings("unchecked")
  private void givenTimeWindowedAggregate() {
    when(materializedFactory.<Struct, WindowStore<Bytes, byte[]>>create(any(), any(), any(), any()))
        .thenReturn(timeWindowMaterialized);
    when(groupedStream.windowedBy(any(Windows.class))).thenReturn(timeWindowedStream);
    when(timeWindowedStream.aggregate(any(), any(), any(Materialized.class)))
        .thenReturn(windowed);
    when(windowed.transformValues(any(), any(Named.class)))
        .thenReturn((KTable) windowedWithResults);
    when(windowedWithResults.transformValues(any(), any(Named.class)))
        .thenReturn((KTable) windowedWithWindowBounds);
  }

  private void givenTumblingWindowedAggregate() {
    givenTimeWindowedAggregate();
    windowedAggregate = new StreamWindowedAggregate(
        new ExecutionStepPropertiesV1(CTX),
        sourceStep,
        Formats.of(KEY_FORMAT, VALUE_FORMAT, SerdeOptions.of()),
        NON_AGG_COLUMNS,
        FUNCTIONS,
        new TumblingWindowExpression(
            Optional.empty(),
            new WindowTimeClause(WINDOW.getSeconds(), TimeUnit.SECONDS),
            Optional.of(retentionClause),
            Optional.of(gracePeriodClause)
        )
    );
  }

  private void givenHoppingWindowedAggregate() {
    givenTimeWindowedAggregate();
    windowedAggregate = new StreamWindowedAggregate(
        new ExecutionStepPropertiesV1(CTX),
        sourceStep,
        Formats.of(KEY_FORMAT, VALUE_FORMAT, SerdeOptions.of()),
        NON_AGG_COLUMNS,
        FUNCTIONS,
        new HoppingWindowExpression(
            Optional.empty(),
            new WindowTimeClause(WINDOW.getSeconds(), TimeUnit.SECONDS),
            new WindowTimeClause(HOP.getSeconds(), TimeUnit.SECONDS),
            Optional.of(retentionClause),
            Optional.of(gracePeriodClause)
        )
    );
  }

  @SuppressWarnings("unchecked")
  private void givenSessionWindowedAggregate() {
    when(materializedFactory.<Struct, SessionStore<Bytes, byte[]>>create(any(), any(), any(), any()))
        .thenReturn(sessionWindowMaterialized);
    when(groupedStream.windowedBy(any(SessionWindows.class))).thenReturn(sessionWindowedStream);
    when(sessionWindowedStream.aggregate(any(), any(), any(), any(Materialized.class)))
        .thenReturn(windowed);
    when(windowed.transformValues(any(), any(Named.class)))
        .thenReturn((KTable) windowedWithResults);
    when(windowedWithResults.transformValues(any(), any(Named.class)))
        .thenReturn((KTable) windowedWithWindowBounds);

    windowedAggregate = new StreamWindowedAggregate(
        new ExecutionStepPropertiesV1(CTX),
        sourceStep,
        Formats.of(KEY_FORMAT, VALUE_FORMAT, SerdeOptions.of()),
        NON_AGG_COLUMNS,
        FUNCTIONS,
        new SessionWindowExpression(
            Optional.empty(),
            new WindowTimeClause(WINDOW.getSeconds(), TimeUnit.SECONDS),
            Optional.of(retentionClause),
            Optional.of(gracePeriodClause)
        )
    );
  }

  @Test
  public void shouldBuildUnwindowedAggregateCorrectly() {
    // Given:
    givenUnwindowedAggregate();

    // When:
    final KTableHolder<Struct> result = aggregate.build(planBuilder);

    // Then:
    assertThat(result.getTable(), is(aggregatedWithResults));
    final InOrder inOrder = Mockito.inOrder(groupedStream, aggregated, aggregatedWithResults);
    inOrder.verify(groupedStream).aggregate(initializer, aggregator, materialized);
    inOrder.verify(aggregated).transformValues(any(), any(Named.class));
    inOrder.verifyNoMoreInteractions();
  }

  @Test
  public void shouldBuildUnwindowedAggregateWithCorrectSchema() {
    // Given:
    givenUnwindowedAggregate();

    // When:
    final KTableHolder<Struct> result = aggregate.build(planBuilder);

    // Then:
    assertThat(result.getSchema(), is(OUTPUT_SCHEMA));
  }

  @Test
  public void shouldBuildMaterializationCorrectlyForUnwindowedAggregate() {
    // Given:
    givenUnwindowedAggregate();

    // When:
    final KTableHolder<Struct> result = aggregate.build(planBuilder);

    // Then:
    assertCorrectMaterializationBuilder(result, false);
  }

  @Test
  public void shouldBuildMaterializedWithCorrectSerdesForUnwindowedAggregate() {
    // Given:
    givenUnwindowedAggregate();

    // When:
    aggregate.build(planBuilder);

    // Then:
    verify(materializedFactory).create(same(keySerde), same(valueSerde), any());
  }

  @Test
  public void shouldBuildMaterializedWithCorrectNameForUnwindowedAggregate() {
    // Given:
    givenUnwindowedAggregate();

    // When:
    aggregate.build(planBuilder);

    // Then:
    verify(materializedFactory).create(any(), any(), eq("agg-regate-Materialize"));
  }

  @Test
  public void shouldBuildKeySerdeCorrectlyForUnwindowedAggregate() {
    // Given:
    givenUnwindowedAggregate();

    // When:
    aggregate.build(planBuilder);

    // Then:
    verify(queryBuilder).buildKeySerde(KEY_FORMAT, PHYSICAL_AGGREGATE_SCHEMA, MATERIALIZE_CTX);
  }

  @Test
  public void shouldBuildValueSerdeCorrectlyForUnwindowedAggregate() {
    // Given:
    givenUnwindowedAggregate();

    // When:
    aggregate.build(planBuilder);

    // Then:
    verify(queryBuilder).buildValueSerde(
        VALUE_FORMAT,
        PHYSICAL_AGGREGATE_SCHEMA,
        MATERIALIZE_CTX
    );
  }

  @Test
  public void shouldBuildAggregatorParamsCorrectlyForUnwindowedAggregate() {
    // Given:
    givenUnwindowedAggregate();

    // When:
    aggregate.build(planBuilder);

    // Then:
    verify(aggregateParamsFactory).create(
        INPUT_SCHEMA,
        NON_AGG_COLUMNS,
        functionRegistry,
        FUNCTIONS,
        false
    );
  }

  @Test
  public void shouldBuildTumblingWindowedAggregateCorrectly() {
    // Given:
    givenTumblingWindowedAggregate();

    // When:
    final KTableHolder<Windowed<Struct>> result = windowedAggregate.build(planBuilder);

    // Then:
    assertThat(result.getTable(), is(windowedWithWindowBounds));
    verify(gracePeriodClause).toDuration();
    verify(retentionClause).toDuration();

    final InOrder inOrder = Mockito.inOrder(
        groupedStream,
        timeWindowedStream,
        windowed,
        windowedWithResults,
        windowedWithWindowBounds
    );
    inOrder.verify(groupedStream).windowedBy(TimeWindows.of(WINDOW).grace(gracePeriodClause.toDuration()));
    inOrder.verify(timeWindowedStream).aggregate(initializer, aggregator, timeWindowMaterialized);
    inOrder.verify(windowed).transformValues(any(), any(Named.class));
    inOrder.verify(windowedWithResults).transformValues(any(), any(Named.class));
    inOrder.verifyNoMoreInteractions();

    assertThat(result.getTable(), is(windowedWithWindowBounds));
  }

  @Test
  public void shouldBuildHoppingWindowedAggregateCorrectly() {
    // Given:
    givenHoppingWindowedAggregate();

    // When:
    final KTableHolder<Windowed<Struct>> result = windowedAggregate.build(planBuilder);

    // Then:
    assertThat(result.getTable(), is(windowedWithWindowBounds));
    verify(gracePeriodClause).toDuration();
    verify(retentionClause).toDuration();
    final InOrder inOrder = Mockito.inOrder(
        groupedStream,
        timeWindowedStream,
        windowed,
        windowedWithResults,
        windowedWithWindowBounds
    );

    inOrder.verify(groupedStream).windowedBy(TimeWindows.of(WINDOW).advanceBy(HOP)
        .grace(gracePeriodClause.toDuration()));
    inOrder.verify(timeWindowedStream).aggregate(initializer, aggregator, timeWindowMaterialized);
    inOrder.verify(windowed).transformValues(any(), any(Named.class));
    inOrder.verify(windowedWithResults).transformValues(any(), any(Named.class));
    inOrder.verifyNoMoreInteractions();

    assertThat(result.getTable(), is(windowedWithWindowBounds));
  }

  @Test
  public void shouldBuildSessionWindowedAggregateCorrectly() {
    // Given:
    givenSessionWindowedAggregate();

    // When:
    final KTableHolder<Windowed<Struct>> result = windowedAggregate.build(planBuilder);

    // Then:
    assertThat(result.getTable(), is(windowedWithWindowBounds));
    verify(gracePeriodClause).toDuration();
    verify(retentionClause).toDuration();
    final InOrder inOrder = Mockito.inOrder(
        groupedStream,
        sessionWindowedStream,
        windowed,
        windowedWithResults,
        windowedWithWindowBounds
    );
    inOrder.verify(groupedStream).windowedBy(SessionWindows.with(WINDOW)
        .grace(gracePeriodClause.toDuration())
    );
    inOrder.verify(sessionWindowedStream).aggregate(
        initializer,
        aggregator,
        merger,
        sessionWindowMaterialized
    );
    inOrder.verify(windowed).transformValues(any(), any(Named.class));
    inOrder.verify(windowedWithResults).transformValues(any(), any(Named.class));
    inOrder.verifyNoMoreInteractions();

    assertThat(result.getTable(), is(windowedWithWindowBounds));
  }

  @Test
  public void shouldBuildMaterializationCorrectlyForWindowedAggregate() {
    // Given:
    givenHoppingWindowedAggregate();

    // When:
    final KTableHolder<?> result = windowedAggregate.build(planBuilder);

    // Then:
    assertCorrectMaterializationBuilder(result, true);
  }

  @Test
  public void shouldBuildSchemaCorrectlyForWindowedAggregate() {
    // Given:
    givenHoppingWindowedAggregate();

    // When:
    final KTableHolder<?> result = windowedAggregate.build(planBuilder);

    // Then:
    assertThat(result.getSchema(), is(OUTPUT_SCHEMA));
  }

  private List<Runnable> given() {
    return ImmutableList.of(
        this::givenHoppingWindowedAggregate,
        this::givenTumblingWindowedAggregate,
        this::givenSessionWindowedAggregate
    );
  }

  @Test
  public void shouldBuildMaterializedWithCorrectSerdesForWindowedAggregate() {
    for (final Runnable given : given()) {
      // Given:
      reset(groupedStream, timeWindowedStream, sessionWindowedStream, aggregated, materializedFactory);
      given.run();

      // When:
      windowedAggregate.build(planBuilder);

      // Then:
      verify(materializedFactory).create(same(keySerde), same(valueSerde), any(), any());
    }
  }

  @Test
  public void shouldBuildMaterializedWithCorrectNameForWindowedAggregate() {
    for (final Runnable given : given()) {
      // Given:
      reset(groupedStream, timeWindowedStream, sessionWindowedStream, aggregated, materializedFactory);
      given.run();

      // When:
      windowedAggregate.build(planBuilder);

      // Then:
      verify(materializedFactory).create(any(), any(), eq("agg-regate-Materialize"), any());
    }
  }

  @Test
  public void shouldBuildKeySerdeCorrectlyForWindowedAggregate() {
    for (final Runnable given : given()) {
      // Given:
      clearInvocations(groupedStream, timeWindowedStream, sessionWindowedStream, aggregated, queryBuilder);
      given.run();

      // When:
      windowedAggregate.build(planBuilder);

      // Then:
      verify(queryBuilder)
          .buildKeySerde(KEY_FORMAT, PHYSICAL_AGGREGATE_SCHEMA, MATERIALIZE_CTX);
    }
  }

  @Test
  public void shouldReturnCorrectSerdeForWindowedAggregate() {
    for (final Runnable given : given()) {
      // Given:
      clearInvocations(groupedStream, timeWindowedStream, sessionWindowedStream, aggregated, queryBuilder);
      given.run();

      // When:
      final KTableHolder<Windowed<Struct>> tableHolder = windowedAggregate.build(planBuilder);

      // Then:
      final KeySerdeFactory<Windowed<Struct>> serdeFactory = tableHolder.getKeySerdeFactory();
      final FormatInfo mockFormat = mock(FormatInfo.class);
      final PhysicalSchema mockSchema = mock(PhysicalSchema.class);
      final QueryContext mockCtx = mock(QueryContext.class);
      serdeFactory.buildKeySerde(mockFormat, mockSchema, mockCtx);
      verify(queryBuilder).buildKeySerde(
          same(mockFormat),
          eq(windowedAggregate.getWindowExpression().getWindowInfo()),
          same(mockSchema),
          same(mockCtx)
      );
    }
  }

  @Test
  public void shouldBuildValueSerdeCorrectlyForWindowedAggregate() {
    for (final Runnable given : given()) {
      // Given:
      clearInvocations(groupedStream, timeWindowedStream, sessionWindowedStream, aggregated, queryBuilder);
      given.run();

      // When:
      windowedAggregate.build(planBuilder);

      // Then:
      verify(queryBuilder)
          .buildValueSerde(VALUE_FORMAT, PHYSICAL_AGGREGATE_SCHEMA, MATERIALIZE_CTX);
    }
  }

  @Test
  public void shouldBuildAggregatorParamsCorrectlyForWindowedAggregate() {
    for (final Runnable given : given()) {
      // Given:
      clearInvocations(
          groupedStream,
          timeWindowedStream,
          sessionWindowedStream,
          aggregated,
          aggregateParamsFactory
      );
      when(aggregateParamsFactory.create(any(), any(), any(), any(), anyBoolean()))
          .thenReturn(aggregateParams);
      given.run();

      // When:
      windowedAggregate.build(planBuilder);

      // Then:
      verify(aggregateParamsFactory)
          .create(INPUT_SCHEMA, NON_AGG_COLUMNS, functionRegistry, FUNCTIONS, true);
    }
  }

  private void assertCorrectMaterializationBuilder(
      final KTableHolder<?> result,
      final boolean windowed
  ) {
    assertThat(result.getMaterializationBuilder().isPresent(), is(true));

    final MaterializationInfo info = result.getMaterializationBuilder().get().build();
    assertThat(info.stateStoreName(), equalTo("agg-regate-Materialize"));
    assertThat(info.getSchema(), equalTo(OUTPUT_SCHEMA));
    assertThat(info.getStateStoreSchema(), equalTo(AGGREGATE_SCHEMA));
    assertThat(info.getTransforms(), hasSize(1 + (windowed ? 1 : 0)));

    final MapperInfo aggMapInfo = (MapperInfo) info.getTransforms().get(0);
    final KsqlTransformer<Object, GenericRow> mapper = aggMapInfo.getMapper(name -> null);

    // Given:
    final Struct key = mock(Struct.class);
    final GenericRow value = mock(GenericRow.class);

    // When:
    mapper.transform(key, value, ctx);

    // Then:
    verify(resultMapper).transform(key, value, ctx);
  }
}