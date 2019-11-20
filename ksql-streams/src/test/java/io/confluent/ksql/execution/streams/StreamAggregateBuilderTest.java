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

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.builder.KsqlQueryBuilder;
import io.confluent.ksql.execution.context.QueryContext;
import io.confluent.ksql.execution.expression.tree.ColumnReferenceExp;
import io.confluent.ksql.execution.expression.tree.FunctionCall;
import io.confluent.ksql.execution.function.udaf.KudafAggregator;
import io.confluent.ksql.execution.function.udaf.KudafInitializer;
import io.confluent.ksql.execution.function.udaf.window.WindowSelectMapper;
import io.confluent.ksql.execution.materialization.MaterializationInfo;
import io.confluent.ksql.execution.materialization.MaterializationInfo.AggregateMapInfo;
import io.confluent.ksql.execution.plan.DefaultExecutionStepProperties;
import io.confluent.ksql.execution.plan.ExecutionStep;
import io.confluent.ksql.execution.plan.Formats;
import io.confluent.ksql.execution.plan.KGroupedStreamHolder;
import io.confluent.ksql.execution.plan.KTableHolder;
import io.confluent.ksql.execution.plan.PlanBuilder;
import io.confluent.ksql.execution.plan.StreamAggregate;
import io.confluent.ksql.execution.plan.StreamWindowedAggregate;
import io.confluent.ksql.execution.windows.HoppingWindowExpression;
import io.confluent.ksql.execution.windows.SessionWindowExpression;
import io.confluent.ksql.execution.windows.TumblingWindowExpression;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.FunctionName;
import io.confluent.ksql.schema.ksql.ColumnRef;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.PhysicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.serde.Format;
import io.confluent.ksql.serde.FormatInfo;
import io.confluent.ksql.serde.KeyFormat;
import io.confluent.ksql.serde.KeySerde;
import io.confluent.ksql.serde.SerdeOption;
import io.confluent.ksql.serde.ValueFormat;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Merger;
import org.apache.kafka.streams.kstream.SessionWindowedKStream;
import org.apache.kafka.streams.kstream.SessionWindows;
import org.apache.kafka.streams.kstream.TimeWindowedKStream;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.kstream.ValueMapperWithKey;
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
import org.mockito.junit.MockitoJUnitRunner;

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
  private static final PhysicalSchema PHYSICAL_AGGREGATE_SCHEMA = PhysicalSchema.from(
      AGGREGATE_SCHEMA,
      SerdeOption.none()
  );
  private static final FunctionCall AGG0 = new FunctionCall(
      FunctionName.of("AGG0"),
      ImmutableList.of(new ColumnReferenceExp(ColumnRef.withoutSource(ColumnName.of("ARGUMENT0"))))
  );
  private static final FunctionCall AGG1 = new FunctionCall(
      FunctionName.of("AGG1"),
      ImmutableList.of(new ColumnReferenceExp(ColumnRef.withoutSource(ColumnName.of("ARGUMENT1"))))
  );
  private static final List<FunctionCall> FUNCTIONS = ImmutableList.of(AGG0, AGG1);
  private static final QueryContext CTX =
      new QueryContext.Stacker().push("agg").push("regate").getQueryContext();
  private static final KeyFormat KEY_FORMAT = KeyFormat.nonWindowed(FormatInfo.of(Format.KAFKA));
  private static final ValueFormat VALUE_FORMAT = ValueFormat.of(FormatInfo.of(Format.JSON));
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
  private KTable<Windowed<Struct>, GenericRow> windowedWithWindowBoundaries;
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
  private KudafAggregator aggregator;
  @Mock
  private ValueMapper<GenericRow, GenericRow> resultMapper;
  @Mock
  private WindowSelectMapper windowSelectMapper;
  @Mock
  private Merger<Struct, GenericRow> merger;
  @Mock
  private MaterializedFactory materializedFactory;
  @Mock
  private KeySerde<Struct> keySerde;
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

  private PlanBuilder planBuilder;
  private StreamAggregate aggregate;
  private StreamWindowedAggregate windowedAggregate;

  @Before
  public void init() {
    when(sourceStep.build(any())).thenReturn(KGroupedStreamHolder.of(groupedStream, INPUT_SCHEMA));
    when(queryBuilder.buildKeySerde(any(), any(), any())).thenReturn(keySerde);
    when(queryBuilder.buildValueSerde(any(), any(), any())).thenReturn(valueSerde);
    when(queryBuilder.getFunctionRegistry()).thenReturn(functionRegistry);
    when(aggregateParamsFactory.create(any(), anyInt(), any(), any()))
        .thenReturn(aggregateParams);
    when(aggregateParams.getAggregator()).thenReturn(aggregator);
    when(aggregateParams.getAggregateSchema()).thenReturn(AGGREGATE_SCHEMA);
    when(aggregateParams.getSchema()).thenReturn(OUTPUT_SCHEMA);
    when(aggregator.getMerger()).thenReturn(merger);
    when(aggregator.getResultMapper()).thenReturn(resultMapper);
    when(aggregateParams.getInitializer()).thenReturn(initializer);
    when(aggregateParams.getWindowSelectMapper()).thenReturn(windowSelectMapper);
    when(windowSelectMapper.hasSelects()).thenReturn(false);
    planBuilder = new KSPlanBuilder(
        queryBuilder,
        mock(SqlPredicateFactory.class),
        aggregateParamsFactory,
        new StreamsFactories(
            mock(GroupedFactory.class),
            mock(JoinedFactory.class),
            materializedFactory,
            mock(StreamJoinedFactory.class)
        )
    );
  }

  @SuppressWarnings("unchecked")
  private void givenUnwindowedAggregate() {
    when(materializedFactory.<Struct, KeyValueStore<Bytes, byte[]>>create(any(), any(), any()))
        .thenReturn(materialized);
    when(groupedStream.aggregate(any(), any(), any(Materialized.class))).thenReturn(aggregated);
    when(aggregated.mapValues(any(ValueMapper.class))).thenReturn(aggregatedWithResults);
    aggregate = new StreamAggregate(
        new DefaultExecutionStepProperties(OUTPUT_SCHEMA, CTX),
        sourceStep,
        Formats.of(KEY_FORMAT, VALUE_FORMAT, SerdeOption.none()),
        2,
        FUNCTIONS
    );
  }

  @SuppressWarnings("unchecked")
  private void givenTimeWindowedAggregate() {
    when(materializedFactory.<Struct, WindowStore<Bytes, byte[]>>create(any(), any(), any()))
        .thenReturn(timeWindowMaterialized);
    when(groupedStream.windowedBy(any(Windows.class))).thenReturn(timeWindowedStream);
    when(timeWindowedStream.aggregate(any(), any(), any(Materialized.class)))
        .thenReturn(windowed);
    when(windowed.mapValues(any(ValueMapper.class))).thenReturn(windowedWithResults);
  }

  private void givenTumblingWindowedAggregate() {
    givenTimeWindowedAggregate();
    windowedAggregate = new StreamWindowedAggregate(
        new DefaultExecutionStepProperties(OUTPUT_SCHEMA, CTX),
        sourceStep,
        Formats.of(KEY_FORMAT, VALUE_FORMAT, SerdeOption.none()),
        2,
        FUNCTIONS,
        new TumblingWindowExpression(WINDOW.getSeconds(), TimeUnit.SECONDS)
    );
  }

  private void givenHoppingWindowedAggregate() {
    givenTimeWindowedAggregate();
    windowedAggregate = new StreamWindowedAggregate(
        new DefaultExecutionStepProperties(OUTPUT_SCHEMA, CTX),
        sourceStep,
        Formats.of(KEY_FORMAT, VALUE_FORMAT, SerdeOption.none()),
        2,
        FUNCTIONS,
        new HoppingWindowExpression(
            WINDOW.getSeconds(),
            TimeUnit.SECONDS,
            HOP.getSeconds(),
            TimeUnit.SECONDS
        )
    );
  }

  @SuppressWarnings("unchecked")
  private void givenSessionWindowedAggregate() {
    when(materializedFactory.<Struct, SessionStore<Bytes, byte[]>>create(any(), any(), any()))
        .thenReturn(sessionWindowMaterialized);
    when(groupedStream.windowedBy(any(SessionWindows.class))).thenReturn(sessionWindowedStream);
    when(sessionWindowedStream.aggregate(any(), any(), any(), any(Materialized.class)))
        .thenReturn(windowed);
    when(windowed.mapValues(any(ValueMapper.class))).thenReturn(windowedWithResults);
    windowedAggregate = new StreamWindowedAggregate(
        new DefaultExecutionStepProperties(OUTPUT_SCHEMA, CTX),
        sourceStep,
        Formats.of(KEY_FORMAT, VALUE_FORMAT, SerdeOption.none()),
        2,
        FUNCTIONS,
        new SessionWindowExpression(WINDOW.getSeconds(), TimeUnit.SECONDS)
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
    inOrder.verify(aggregated).mapValues(resultMapper);
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
    assertCorrectMaterializationBuilder(result);
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
    verify(materializedFactory).create(any(), any(), eq("agg-regate"));
  }

  @Test
  public void shouldBuildKeySerdeCorrectlyForUnwindowedAggregate() {
    // Given:
    givenUnwindowedAggregate();

    // When:
    aggregate.build(planBuilder);

    // Then:
    verify(queryBuilder).buildKeySerde(KEY_FORMAT.getFormatInfo(), PHYSICAL_AGGREGATE_SCHEMA, CTX);
  }

  @Test
  public void shouldBuildValueSerdeCorrectlyForUnwindowedAggregate() {
    // Given:
    givenUnwindowedAggregate();

    // When:
    aggregate.build(planBuilder);

    // Then:
    verify(queryBuilder).buildValueSerde(
        VALUE_FORMAT.getFormatInfo(),
        PHYSICAL_AGGREGATE_SCHEMA,
        CTX
    );
  }

  @Test
  public void shouldBuildAggregatorParamsCorrectlyForUnwindowedAggregate() {
    // Given:
    givenUnwindowedAggregate();

    // When:
    aggregate.build(planBuilder);

    // Then:
    verify(aggregateParamsFactory).create(INPUT_SCHEMA, 2, functionRegistry, FUNCTIONS);
  }

  @Test
  public void shouldBuildTumblingWindowedAggregateCorrectly() {
    // Given:
    givenTumblingWindowedAggregate();

    // When:
    final KTableHolder<Windowed<Struct>> result = windowedAggregate.build(planBuilder);

    // Then:
    assertThat(result.getTable(), is(windowedWithResults));
    final InOrder inOrder = Mockito.inOrder(
        groupedStream,
        timeWindowedStream,
        windowed,
        windowedWithResults
    );
    inOrder.verify(groupedStream).windowedBy(TimeWindows.of(WINDOW));
    inOrder.verify(timeWindowedStream).aggregate(initializer, aggregator, timeWindowMaterialized);
    inOrder.verify(windowed).mapValues(resultMapper);
    inOrder.verifyNoMoreInteractions();
  }

  @Test
  public void shouldBuildHoppingWindowedAggregateCorrectly() {
    // Given:
    givenHoppingWindowedAggregate();

    // When:
    final KTableHolder<Windowed<Struct>> result = windowedAggregate.build(planBuilder);

    // Then:
    assertThat(result.getTable(), is(windowedWithResults));
    final InOrder inOrder = Mockito.inOrder(
        groupedStream,
        timeWindowedStream,
        windowed,
        windowedWithResults
    );
    inOrder.verify(groupedStream).windowedBy(TimeWindows.of(WINDOW).advanceBy(HOP));
    inOrder.verify(timeWindowedStream).aggregate(initializer, aggregator, timeWindowMaterialized);
    inOrder.verify(windowed).mapValues(resultMapper);
    inOrder.verifyNoMoreInteractions();
  }

  @Test
  public void shouldBuildSessionWindowedAggregateCorrectly() {
    // Given:
    givenSessionWindowedAggregate();

    // When:
    final KTableHolder<Windowed<Struct>> result = windowedAggregate.build(planBuilder);

    // Then:
    assertThat(result.getTable(), is(windowedWithResults));
    final InOrder inOrder = Mockito.inOrder(
        groupedStream,
        sessionWindowedStream,
        windowed,
        windowedWithResults
    );
    inOrder.verify(groupedStream).windowedBy(SessionWindows.with(WINDOW));
    inOrder.verify(sessionWindowedStream).aggregate(
        initializer,
        aggregator,
        merger,
        sessionWindowMaterialized
    );
    inOrder.verify(windowed).mapValues(resultMapper);
    inOrder.verifyNoMoreInteractions();
  }

  @Test
  public void shouldBuildMaterializationCorrectlyForWindowedAggregate() {
    // Given:
    givenHoppingWindowedAggregate();

    // When:
    final KTableHolder<?> result = windowedAggregate.build(planBuilder);

    // Then:
    assertCorrectMaterializationBuilder(result);
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
      verify(materializedFactory).create(same(keySerde), same(valueSerde), any());
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
      verify(materializedFactory).create(any(), any(), eq("agg-regate"));
    }
  }

  @Test
  public void shouldBuildKeySerdeCorrectlyForWindowedAggregate() {
    for (final Runnable given : given()) {
      // Given:
      reset(groupedStream, timeWindowedStream, sessionWindowedStream, aggregated, queryBuilder);
      given.run();

      // When:
      windowedAggregate.build(planBuilder);

      // Then:
      verify(queryBuilder)
          .buildKeySerde(KEY_FORMAT.getFormatInfo(), PHYSICAL_AGGREGATE_SCHEMA, CTX);
    }
  }

  @Test
  public void shouldBuildValueSerdeCorrectlyForWindowedAggregate() {
    for (final Runnable given : given()) {
      // Given:
      reset(groupedStream, timeWindowedStream, sessionWindowedStream, aggregated, queryBuilder);
      given.run();

      // When:
      windowedAggregate.build(planBuilder);

      // Then:
      verify(queryBuilder)
          .buildValueSerde(VALUE_FORMAT.getFormatInfo(), PHYSICAL_AGGREGATE_SCHEMA, CTX);
    }
  }

  @Test
  public void shouldBuildAggregatorParamsCorrectlyForWindowedAggregate() {
    for (final Runnable given : given()) {
      // Given:
      reset(
          groupedStream,
          timeWindowedStream,
          sessionWindowedStream,
          aggregated,
          aggregateParamsFactory
      );
      when(aggregateParamsFactory.create(any(), anyInt(), any(), any()))
          .thenReturn(aggregateParams);
      given.run();

      // When:
      windowedAggregate.build(planBuilder);

      // Then:
      verify(aggregateParamsFactory)
          .create(INPUT_SCHEMA, 2, functionRegistry, FUNCTIONS);
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void shouldAddWindowBoundariesIfSpecified() {
    for (final Runnable given : given()) {
      // Given:
      reset(
          groupedStream, timeWindowedStream, sessionWindowedStream, windowed, windowedWithResults);
      when(windowSelectMapper.hasSelects()).thenReturn(true);
      when(windowedWithResults.mapValues(any(ValueMapperWithKey.class))).thenReturn(
          windowedWithWindowBoundaries);
      given.run();

      // When:
      final KTableHolder<Windowed<Struct>> result =
          windowedAggregate.build(planBuilder);

      // Then:
      assertThat(result.getTable(), is(windowedWithWindowBoundaries));
      verify(windowedWithResults).mapValues(windowSelectMapper);
    }
  }

  private static void assertCorrectMaterializationBuilder(final KTableHolder<?> result) {
    assertThat(result.getMaterializationBuilder().isPresent(), is(true));

    final MaterializationInfo info = result.getMaterializationBuilder().get().build();
    assertThat(info.stateStoreName(), equalTo("agg-regate"));
    assertThat(info.getSchema(), equalTo(OUTPUT_SCHEMA.withoutMetaColumns()));
    assertThat(info.getStateStoreSchema(), equalTo(AGGREGATE_SCHEMA.withoutMetaColumns()));
    assertThat(info.getTransforms(), hasSize(1));

    final AggregateMapInfo aggMapInfo = (AggregateMapInfo) info.getTransforms().get(0);
    assertThat(aggMapInfo.getInfo().schema(), equalTo(INPUT_SCHEMA));
    assertThat(aggMapInfo.getInfo().aggregateFunctions(), equalTo(FUNCTIONS));
    assertThat(aggMapInfo.getInfo().startingColumnIndex(), equalTo(2));
  }
}