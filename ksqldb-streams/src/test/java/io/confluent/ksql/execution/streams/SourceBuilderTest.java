/*
 * Copyright 2021 Confluent Inc.
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

import static io.confluent.ksql.GenericRow.genericRow;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.GenericKey;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.context.QueryContext;
import io.confluent.ksql.execution.context.QueryContext.Stacker;
import io.confluent.ksql.execution.plan.ExecutionStep;
import io.confluent.ksql.execution.plan.ExecutionStepPropertiesV1;
import io.confluent.ksql.execution.plan.Formats;
import io.confluent.ksql.execution.plan.KTableHolder;
import io.confluent.ksql.execution.plan.PlanBuilder;
import io.confluent.ksql.execution.plan.PlanInfo;
import io.confluent.ksql.execution.plan.SourceStep;
import io.confluent.ksql.execution.plan.TableSource;
import io.confluent.ksql.execution.plan.WindowedTableSource;
import io.confluent.ksql.execution.runtime.RuntimeBuildContext;
import io.confluent.ksql.execution.timestamp.TimestampColumn;
import io.confluent.ksql.logging.processing.ProcessingLogger;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.PhysicalSchema;
import io.confluent.ksql.schema.ksql.SystemColumns;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.serde.FormatFactory;
import io.confluent.ksql.serde.FormatInfo;
import io.confluent.ksql.serde.SerdeFeatures;
import io.confluent.ksql.serde.StaticTopicSerde;
import io.confluent.ksql.serde.WindowInfo;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.util.KsqlConfig;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology.AutoOffsetReset;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.kstream.ValueTransformerWithKeySupplier;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.SessionWindow;
import org.apache.kafka.streams.kstream.internals.TimeWindow;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.TimestampExtractor;
import org.apache.kafka.streams.state.KeyValueStore;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

public class SourceBuilderTest {

  private static final ColumnName K0 = ColumnName.of("k0");
  private static final ColumnName K1 = ColumnName.of("k1");

  private static final LogicalSchema SOURCE_SCHEMA = LogicalSchema.builder()
      .keyColumn(K0, SqlTypes.BIGINT)
      .valueColumn(ColumnName.of("field1"), SqlTypes.STRING)
      .valueColumn(ColumnName.of("field2"), SqlTypes.BIGINT)
      .build();

  private static final LogicalSchema MULTI_COL_SOURCE_SCHEMA = LogicalSchema.builder()
      .keyColumn(K0, SqlTypes.BIGINT)
      .keyColumn(K1, SqlTypes.BIGINT)
      .valueColumn(ColumnName.of("field1"), SqlTypes.STRING)
      .valueColumn(ColumnName.of("field2"), SqlTypes.BIGINT)
      .build();

  private static final double A_KEY = 10.11;

  private static final GenericKey KEY = GenericKey.genericKey(A_KEY);

  private static final LogicalSchema SCHEMA = SOURCE_SCHEMA
      .withPseudoAndKeyColsInValue(false, SystemColumns.ROWPARTITION_ROWOFFSET_PSEUDOCOLUMN_VERSION);

  private static final LogicalSchema WINDOWED_SCHEMA = SOURCE_SCHEMA
      .withPseudoAndKeyColsInValue(true, SystemColumns.ROWPARTITION_ROWOFFSET_PSEUDOCOLUMN_VERSION);

  private static final KsqlConfig KSQL_CONFIG = new KsqlConfig(ImmutableMap.of());

  private static final Optional<TimestampColumn> TIMESTAMP_COLUMN = Optional.of(
      new TimestampColumn(
          ColumnName.of("field2"),
          Optional.empty()
      )
  );

  private static final long A_WINDOW_START = 10L;
  private static final long A_WINDOW_END = 20L;
  private static final long A_ROWTIME = 456L;
  private static final int A_ROWPARTITION = 789;
  private static final long A_ROWOFFSET = 123;

  private final SerdeFeatures KEY_FEATURES = SerdeFeatures.of();
  private final SerdeFeatures VALUE_FEATURES = SerdeFeatures.of();
  private final PhysicalSchema PHYSICAL_SCHEMA = PhysicalSchema
      .from(SOURCE_SCHEMA, KEY_FEATURES, VALUE_FEATURES);
  private static final String TOPIC_NAME = "topic";

  private final QueryContext ctx = new Stacker().push("base").push("source").getQueryContext();
  @Mock
  private RuntimeBuildContext buildContext;
  @Mock
  private StreamsBuilder streamsBuilder;
  @Mock
  private KStream kStream;
  @Mock
  private KTable kTable;
  @Mock
  private FormatInfo keyFormatInfo;
  @Mock
  private WindowInfo windowInfo;
  @Mock
  private FormatInfo valueFormatInfo;
  @Mock
  private Serde<GenericRow> valueSerde;
  @Mock
  private Serde<GenericKey> keySerde;
  @Mock
  private Serde<Windowed<GenericKey>> windowedKeySerde;
  @Mock
  private ProcessorContext processorCtx;
  @Mock
  private ConsumedFactory consumedFactory;
  @Mock
  private StreamsFactories streamsFactories;
  @Mock
  private Consumed<GenericKey, GenericRow> consumed;
  @Mock
  private Consumed<Windowed<GenericKey>, GenericRow> consumedWindowed;
  @Mock
  private MaterializedFactory materializationFactory;
  @Mock
  private Materialized<Object, GenericRow, KeyValueStore<Bytes, byte[]>> materialized;
  @Mock
  private ProcessingLogger processingLogger;
  @Mock
  private ServiceContext serviceContext;
  @Mock
  private SchemaRegistryClient srClient;
  @Mock
  private PlanInfo planInfo;
  @Captor
  private ArgumentCaptor<ValueTransformerWithKeySupplier<?, GenericRow, GenericRow>> transformSupplierCaptor;
  @Captor
  private ArgumentCaptor<ValueTransformerWithKeySupplier<?, GenericRow, GenericRow>> transformSupplierCaptor2;
  @Captor
  private ArgumentCaptor<Materialized<?, GenericRow, KeyValueStore<Bytes, byte[]>>> materializedArgumentCaptor;
  @Captor
  private ArgumentCaptor<TimestampExtractor> timestampExtractorCaptor;
  @Captor
  private ArgumentCaptor<StaticTopicSerde<GenericRow>> serdeCaptor;
  private final GenericRow row = genericRow("baz", 123);
  private PlanBuilder planBuilder;

  private TableSource tableSource;
  private WindowedTableSource windowedTableSource;

  @Rule
  public final MockitoRule mockitoRule = MockitoJUnit.rule();

  @Before
  @SuppressWarnings({"unchecked", "rawtypes"})
  public void setup() {
    when(buildContext.getApplicationId()).thenReturn("appid");
    when(buildContext.getStreamsBuilder()).thenReturn(streamsBuilder);
    when(buildContext.getProcessingLogger(any())).thenReturn(processingLogger);
    when(streamsBuilder.stream(anyString(), any(Consumed.class))).thenReturn(kStream);
    when(streamsBuilder.table(anyString(), any(), any())).thenReturn(kTable);
    when(streamsBuilder.table(anyString(), any(Consumed.class))).thenReturn(kTable);
    when(kTable.mapValues(any(ValueMapper.class))).thenReturn(kTable);
    when(kTable.mapValues(any(ValueMapper.class), any(Materialized.class))).thenReturn(kTable);
    when(kStream.transformValues(any(ValueTransformerWithKeySupplier.class))).thenReturn(kStream);
    when(kTable.transformValues(any(ValueTransformerWithKeySupplier.class))).thenReturn(kTable);
    when(kTable.transformValues(any(ValueTransformerWithKeySupplier.class), any(Materialized.class))).thenReturn(kTable);
    when(buildContext.buildKeySerde(any(), any(), any())).thenReturn(keySerde);
    when(buildContext.buildValueSerde(any(), any(), any())).thenReturn(valueSerde);
    when(buildContext.getServiceContext()).thenReturn(serviceContext);
    when(buildContext.getKsqlConfig()).thenReturn(KSQL_CONFIG);
    when(processorCtx.timestamp()).thenReturn(A_ROWTIME);
    when(processorCtx.partition()).thenReturn(A_ROWPARTITION);
    when(processorCtx.offset()).thenReturn(A_ROWOFFSET);
    when(serviceContext.getSchemaRegistryClient()).thenReturn(srClient);
    when(streamsFactories.getConsumedFactory()).thenReturn(consumedFactory);
    when(streamsFactories.getMaterializedFactory()).thenReturn(materializationFactory);
    when(materializationFactory.create(any(), any(), any()))
        .thenReturn((Materialized) materialized);
    when(valueFormatInfo.getFormat()).thenReturn(FormatFactory.AVRO.name());

    planBuilder = new KSPlanBuilder(
        buildContext,
        mock(SqlPredicateFactory.class),
        mock(AggregateParamsFactory.class),
        streamsFactories
    );
  }

  @Test
  @SuppressWarnings("unchecked")
  public void shouldBuildTableWithCorrectTimestampExtractor() {
    // Given:
    givenUnwindowedSourceTable();
    final ConsumerRecord<Object, Object> record = mock(ConsumerRecord.class);
    when(record.value()).thenReturn(GenericRow.genericRow("123", A_ROWTIME));

    // When:
    tableSource.build(planBuilder, planInfo);

    // Then:
    verify(consumed).withTimestampExtractor(timestampExtractorCaptor.capture());
    final TimestampExtractor extractor = timestampExtractorCaptor.getValue();
    assertThat(extractor.extract(record, 999), is(A_ROWTIME));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void shouldApplyCorrectTransformationsToSourceTable() {
    // Given:
    givenUnwindowedSourceTable();

    // When:
    final KTableHolder<GenericKey> builtKTable = tableSource.build(planBuilder, planInfo);

    // Then:
    assertThat(builtKTable.getTable(), is(kTable));
    final InOrder validator = inOrder(streamsBuilder, kTable);
    validator.verify(streamsBuilder).table(eq(TOPIC_NAME), eq(consumed));
    validator.verify(kTable).transformValues(any(ValueTransformerWithKeySupplier.class));
    verify(consumedFactory).create(keySerde, valueSerde);
    verify(consumed).withTimestampExtractor(any());
    verify(consumed).withOffsetResetPolicy(AutoOffsetReset.EARLIEST);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void shouldApplyCorrectTransformationsToSourceTableWithDownstreamRepartition() {
    // Given:
    givenUnwindowedSourceTable();
    final PlanInfo planInfo = givenDownstreamRepartition(tableSource);

    // When:
    final KTableHolder<GenericKey> builtKTable = tableSource.build(planBuilder, planInfo);

    // Then:
    assertThat(builtKTable.getTable(), is(kTable));
    final InOrder validator = inOrder(streamsBuilder, kTable);
    validator.verify(streamsBuilder).table(eq(TOPIC_NAME), eq(consumed));
    validator.verify(kTable, times(2)).transformValues(any(ValueTransformerWithKeySupplier.class));
    verify(consumedFactory).create(keySerde, valueSerde);
    verify(consumed).withTimestampExtractor(any());
    verify(consumed).withOffsetResetPolicy(AutoOffsetReset.EARLIEST);

    verify(kTable, never()).mapValues(any(ValueMapper.class), any(Materialized.class));
  }

  @Test
  public void shouldUseOffsetResetEarliestForTable() {
    // Given:
    givenUnwindowedSourceTable();

    // When
    tableSource.build(planBuilder, planInfo);

    // Then:
    verify(consumed).withOffsetResetPolicy(AutoOffsetReset.EARLIEST);
  }

  @Test
  public void shouldUseOffsetResetEarliestForWindowedTable() {
    // Given:
    givenWindowedSourceTable();

    // When
    windowedTableSource.build(planBuilder, planInfo);

    // Then:
    verify(consumedWindowed).withOffsetResetPolicy(AutoOffsetReset.EARLIEST);
  }

  @Test
  public void shouldUseConfiguredResetPolicyForTable() {
    // Given:
    when(buildContext.getKsqlConfig()).thenReturn(new KsqlConfig(
        ImmutableMap.of(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")
    ));
    givenUnwindowedSourceTable();

    // When
    tableSource.build(planBuilder, planInfo);

    // Then:
    verify(consumed).withOffsetResetPolicy(AutoOffsetReset.LATEST);
  }
  

  @Test
  public void shouldReturnCorrectSchemaForUnwindowedSourceTable() {
    // Given:
    givenUnwindowedSourceTable();

    // When:
    final KTableHolder<GenericKey> builtKTable = tableSource.build(planBuilder, planInfo);

    // Then:
    assertThat(builtKTable.getSchema(), is(SCHEMA));
  }

  @Test
  public void shouldBuildSourceValueSerdeCorrectly() {
    // Given:
    givenUnwindowedSourceTable();

    // When:
    tableSource.build(planBuilder, planInfo);

    // Then:
    verify(buildContext).buildValueSerde(valueFormatInfo, PHYSICAL_SCHEMA, ctx);
  }

  @Test
  public void shouldBuildSourceKeySerdeCorrectly() {
    // Given:
    givenWindowedSourceTable();

    // When:
    windowedTableSource.build(planBuilder, planInfo);

    // Then:
    verify(buildContext).buildKeySerde(
        keyFormatInfo,
        windowInfo,
        PhysicalSchema.from(SOURCE_SCHEMA, KEY_FEATURES, VALUE_FEATURES),
        ctx
    );
  }

  @Test
  public void shouldHandleMultiKeyField() {
    // Given:
    givenMultiColumnSourceTable();
    final List<ValueTransformerWithKey<GenericKey, GenericRow, GenericRow>> transformers =
        getTransformersFromTableSource(tableSource);

    final GenericKey key = GenericKey.genericKey(1d, 2d);

    // When:
    final GenericRow withKeyAndPseudoCols = applyAllTransformers(key, transformers, row);

    // Then:
    assertThat(withKeyAndPseudoCols, equalTo(GenericRow.genericRow("baz", 123, A_ROWTIME, A_ROWPARTITION, A_ROWOFFSET, 1d, 2d)));
  }

  @Test
  public void shouldHandleMultiKeyFieldWithNullCol() {
    // Given:
    givenMultiColumnSourceTable();
    final List<ValueTransformerWithKey<GenericKey, GenericRow, GenericRow>> transformers =
        getTransformersFromTableSource(tableSource);

    final GenericKey key = GenericKey.genericKey(null, 2d);

    // When:
    final GenericRow withKeyAndPseudoCols = applyAllTransformers(key, transformers, row);

    // Then:
    assertThat(withKeyAndPseudoCols, equalTo(GenericRow.genericRow("baz", 123, A_ROWTIME, A_ROWPARTITION, A_ROWOFFSET, null, 2d)));
  }

  @Test
  public void shouldHandleMultiKeyFieldEmptyGenericKey() {
    // Given:
    givenMultiColumnSourceTable();
    final List<ValueTransformerWithKey<GenericKey, GenericRow, GenericRow>> transformers =
        getTransformersFromTableSource(tableSource);

    final GenericKey key = GenericKey.genericKey(null, null);

    // When:
    final GenericRow withKeyAndPseudoCols = applyAllTransformers(key, transformers, row);

    // Then:
    assertThat(withKeyAndPseudoCols, equalTo(
        genericRow("baz", 123, A_ROWTIME, A_ROWPARTITION, A_ROWOFFSET, null, null)));
  }

  @Test
  public void shouldReturnCorrectSchemaForWindowedSourceTable() {
    // Given:
    givenWindowedSourceTable();

    // When:
    final KTableHolder<Windowed<GenericKey>> builtKTable = windowedTableSource.build(planBuilder, planInfo);

    // Then:
    assertThat(builtKTable.getSchema(), is(WINDOWED_SCHEMA));
  }

  @Test
  public void shouldAddPseudoAndRowKeyColumnsToNonWindowedTable() {
    // Given:
    givenUnwindowedSourceTable();
    final List<ValueTransformerWithKey<GenericKey, GenericRow, GenericRow>> transformers =
        getTransformersFromTableSource(tableSource);

    // When:
    final GenericRow withKeyAndPseudoCols = applyAllTransformers(KEY, transformers, row);

    // Then:
    assertThat(withKeyAndPseudoCols, equalTo(GenericRow.genericRow("baz", 123, A_ROWTIME, A_ROWPARTITION, A_ROWOFFSET, A_KEY)));
  }

  @Test
  public void shouldHandleEmptyKey() {
    // Given:
    givenUnwindowedSourceTable();
    final List<ValueTransformerWithKey<GenericKey, GenericRow, GenericRow>> transformers =
        getTransformersFromTableSource(tableSource);

    final GenericKey nullKey = GenericKey.genericKey((Object) null);

    // When:
    final GenericRow withKeyAndPseudoCols = applyAllTransformers(nullKey, transformers, row);

    // Then:
    assertThat(withKeyAndPseudoCols, equalTo(GenericRow.genericRow("baz", 123, A_ROWTIME, A_ROWPARTITION, A_ROWOFFSET, null)));
  }

  @Test
  public void shouldAddPseudoColumnsAndTimeWindowedRowKeyColumnsToTable() {
    // Given:
    givenWindowedSourceTable();
    final ValueTransformerWithKey<Windowed<GenericKey>, GenericRow, GenericRow> transformer =
        getTransformerFromMaterializedTableSource(windowedTableSource);

    final Windowed<GenericKey> key = new Windowed<>(
        KEY,
        new TimeWindow(A_WINDOW_START, A_WINDOW_END)
    );

    // When:
    final GenericRow withTimestamp = transformer.transform(key, row);

    // Then:
    assertThat(withTimestamp,
        is(GenericRow.genericRow("baz", 123, A_ROWTIME, A_ROWPARTITION, A_ROWOFFSET, A_KEY, A_WINDOW_START, A_WINDOW_END)));
  }

  @Test
  public void shouldAddRowTimeAndSessionWindowedRowKeyColumnsToTable() {
    // Given:
    givenWindowedSourceTable();
    final ValueTransformerWithKey<Windowed<GenericKey>, GenericRow, GenericRow> transformer =
        getTransformerFromMaterializedTableSource(windowedTableSource);

    final Windowed<GenericKey> key = new Windowed<>(
        KEY,
        new SessionWindow(A_WINDOW_START, A_WINDOW_END)
    );

    // When:
    final GenericRow withTimestamp = transformer.transform(key, row);

    // Then:
    assertThat(withTimestamp,
        equalTo(GenericRow.genericRow("baz", 123, A_ROWTIME, A_ROWPARTITION, A_ROWOFFSET, A_KEY, A_WINDOW_START, A_WINDOW_END)));
  }

  @Test
  public void shouldUseCorrectSerdeForWindowedKey() {
    // Given:
    givenWindowedSourceTable();

    // When:
    windowedTableSource.build(planBuilder, planInfo);

    // Then:
    verify(buildContext).buildKeySerde(
        keyFormatInfo,
        windowInfo,
        PhysicalSchema.from(SOURCE_SCHEMA, KEY_FEATURES, VALUE_FEATURES),
        ctx
    );
  }

  @Test
  public void shouldUseCorrectSerdeForNonWindowedKey() {
    // Given:
    givenUnwindowedSourceTable();

    // When:
    tableSource.build(planBuilder, planInfo);

    // Then:
    verify(buildContext).buildKeySerde(
        keyFormatInfo,
        PhysicalSchema.from(SOURCE_SCHEMA, KEY_FEATURES, VALUE_FEATURES),
        ctx
    );
  }

  @Test
  public void shouldReturnCorrectSerdeFactory() {
    // Given:
    givenUnwindowedSourceTable();

    // When:
    final KTableHolder<?> table = tableSource.build(planBuilder, planInfo);

    // Then:
    reset(buildContext);
    table.getExecutionKeyFactory().buildKeySerde(keyFormatInfo, PHYSICAL_SCHEMA, ctx);
    verify(buildContext).buildKeySerde(keyFormatInfo, PHYSICAL_SCHEMA, ctx);
  }

  @Test
  public void shouldReturnCorrectSerdeFactoryForWindowedSource() {
    // Given:
    givenWindowedSourceTable();

    // When:
    final KTableHolder<?> stream = windowedTableSource.build(planBuilder, planInfo);

    // Then:
    reset(buildContext);
    stream.getExecutionKeyFactory().buildKeySerde(keyFormatInfo, PHYSICAL_SCHEMA, ctx);
    verify(buildContext).buildKeySerde(keyFormatInfo, windowInfo, PHYSICAL_SCHEMA, ctx);
  }

  @Test
  public void shouldBuildTableWithCorrectStoreName() {
    // Given:
    givenUnwindowedSourceTable();

    // When:
    tableSource.build(planBuilder, planInfo);

    // Then:
    verify(materializationFactory).create(keySerde, valueSerde, "base-Reduce");
  }

  @SuppressWarnings("unchecked")
  private <K> List<ValueTransformerWithKey<K, GenericRow, GenericRow>> getTransformersFromTableSource(
      final SourceStep<?> streamSource
  ) {
    streamSource.build(planBuilder, planInfo);
    verify(kTable).transformValues(transformSupplierCaptor.capture());
    verify(kTable).transformValues(transformSupplierCaptor2.capture(), materializedArgumentCaptor.capture());

    final List<ValueTransformerWithKey<K, GenericRow, GenericRow>> transformers =
        Stream.concat(transformSupplierCaptor2.getAllValues().stream(), transformSupplierCaptor.getAllValues().stream())
        .map(t -> t.get())
        .map(t -> (ValueTransformerWithKey<K, GenericRow, GenericRow>) t)
        .collect(Collectors.toList());

    transformers.forEach(t -> t.init(processorCtx));
    return transformers;
  }

  @SuppressWarnings("unchecked")
  private <K> ValueTransformerWithKey<K, GenericRow, GenericRow> getTransformerFromMaterializedTableSource(
      final SourceStep<?> streamSource
  ) {
    streamSource.build(planBuilder, planInfo);
    verify(kTable).transformValues(transformSupplierCaptor.capture(), materializedArgumentCaptor.capture());
    final ValueTransformerWithKey<K, GenericRow, GenericRow> transformer =
        (ValueTransformerWithKey<K, GenericRow, GenericRow>) transformSupplierCaptor.getValue().get();
    transformer.init(processorCtx);
    return transformer;
  }

  private static GenericRow applyAllTransformers(
      final GenericKey key,
      final List<ValueTransformerWithKey<GenericKey, GenericRow, GenericRow>> transformerList,
      final GenericRow row
  ) {
    GenericRow last = row;

    for (ValueTransformerWithKey<GenericKey, GenericRow, GenericRow> transformer
        : transformerList) {
      last = transformer.transform(key, last);
    }
    return last;
  }

  private void givenWindowedSourceTable() {
    when(buildContext.buildKeySerde(any(), any(), any(), any())).thenReturn(windowedKeySerde);
    givenConsumed(consumedWindowed, windowedKeySerde);
    windowedTableSource = new WindowedTableSource(
        new ExecutionStepPropertiesV1(ctx),
        TOPIC_NAME,
        Formats.of(keyFormatInfo, valueFormatInfo, KEY_FEATURES, VALUE_FEATURES),
        windowInfo,
        TIMESTAMP_COLUMN,
        SOURCE_SCHEMA,
        OptionalInt.of(SystemColumns.ROWPARTITION_ROWOFFSET_PSEUDOCOLUMN_VERSION)
    );
  }

  private void givenUnwindowedSourceTable() {
    when(buildContext.buildKeySerde(any(), any(), any())).thenReturn(keySerde);
    givenConsumed(consumed, keySerde);
    Formats genericFormat = Formats.of(keyFormatInfo, valueFormatInfo, KEY_FEATURES, VALUE_FEATURES);
    tableSource = new TableSource(
        new ExecutionStepPropertiesV1(ctx),
        TOPIC_NAME,
        genericFormat,
        TIMESTAMP_COLUMN,
        SOURCE_SCHEMA,
        OptionalInt.of(SystemColumns.ROWPARTITION_ROWOFFSET_PSEUDOCOLUMN_VERSION),
        genericFormat
    );
  }

  private void givenMultiColumnSourceTable() {
    when(buildContext.buildKeySerde(any(), any(), any())).thenReturn(keySerde);
    givenConsumed(consumed, keySerde);

    Formats genericFormat = Formats.of(keyFormatInfo, valueFormatInfo, KEY_FEATURES, VALUE_FEATURES);
        tableSource = new TableSource(
        new ExecutionStepPropertiesV1(ctx),
        TOPIC_NAME,
        genericFormat,
        TIMESTAMP_COLUMN,
        MULTI_COL_SOURCE_SCHEMA,
        OptionalInt.of(SystemColumns.ROWPARTITION_ROWOFFSET_PSEUDOCOLUMN_VERSION),
        genericFormat
    );
  }

  private <K> void givenConsumed(final Consumed<K, GenericRow> consumed, final Serde<K> keySerde) {
    when(consumed.withValueSerde(any())).thenReturn(consumed);
    when(consumedFactory.create(keySerde, valueSerde)).thenReturn(consumed);
    when(consumed.withTimestampExtractor(any())).thenReturn(consumed);
    when(consumed.withOffsetResetPolicy(any())).thenReturn(consumed);
  }

  private static PlanInfo givenDownstreamRepartition(final ExecutionStep<?> sourceStep) {
    final PlanInfo mockPlanInfo = mock(PlanInfo.class);
    when(mockPlanInfo.isRepartitionedInPlan(sourceStep)).thenReturn(true);
    return mockPlanInfo;
  }

}
