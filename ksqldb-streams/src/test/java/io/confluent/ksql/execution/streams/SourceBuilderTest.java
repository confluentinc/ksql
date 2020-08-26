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

package io.confluent.ksql.execution.streams;

import static io.confluent.ksql.GenericRow.genericRow;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.builder.KsqlQueryBuilder;
import io.confluent.ksql.execution.context.QueryContext;
import io.confluent.ksql.execution.context.QueryContext.Stacker;
import io.confluent.ksql.execution.plan.ExecutionStepPropertiesV1;
import io.confluent.ksql.execution.plan.Formats;
import io.confluent.ksql.execution.plan.KStreamHolder;
import io.confluent.ksql.execution.plan.KTableHolder;
import io.confluent.ksql.execution.plan.PlanBuilder;
import io.confluent.ksql.execution.plan.SourceStep;
import io.confluent.ksql.execution.plan.StreamSource;
import io.confluent.ksql.execution.plan.TableSource;
import io.confluent.ksql.execution.plan.WindowedStreamSource;
import io.confluent.ksql.execution.plan.WindowedTableSource;
import io.confluent.ksql.execution.timestamp.TimestampColumn;
import io.confluent.ksql.logging.processing.ProcessingLogger;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.PhysicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.serde.FormatFactory;
import io.confluent.ksql.serde.FormatInfo;
import io.confluent.ksql.serde.SerdeOptions;
import io.confluent.ksql.serde.StaticTopicSerde;
import io.confluent.ksql.serde.WindowInfo;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.util.KsqlConfig;
import java.util.Optional;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
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

  private static final LogicalSchema SOURCE_SCHEMA = LogicalSchema.builder()
      .keyColumn(K0, SqlTypes.BIGINT)
      .valueColumn(ColumnName.of("field1"), SqlTypes.STRING)
      .valueColumn(ColumnName.of("field2"), SqlTypes.BIGINT)
      .build();

  private static final Schema KEY_SCHEMA = SchemaBuilder.struct()
      .field(K0.text(), Schema.OPTIONAL_FLOAT64_SCHEMA)
      .build();

  private static final double A_KEY = 10.11;

  private static final Struct KEY = new Struct(KEY_SCHEMA)
      .put(K0.text(), A_KEY);

  private static final LogicalSchema SCHEMA = SOURCE_SCHEMA
      .withPseudoAndKeyColsInValue(false);

  private static final LogicalSchema WINDOWED_SCHEMA = SOURCE_SCHEMA
      .withPseudoAndKeyColsInValue(true);

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

  private final SerdeOptions SERDE_OPTIONS = SerdeOptions.of();
  private final PhysicalSchema PHYSICAL_SCHEMA = PhysicalSchema.from(SOURCE_SCHEMA, SERDE_OPTIONS);
  private static final String TOPIC_NAME = "topic";

  private final QueryContext ctx = new Stacker().push("base").push("source").getQueryContext();
  @Mock
  private KsqlQueryBuilder queryBuilder;
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
  private Serde<Struct> keySerde;
  @Mock
  private Serde<Windowed<Struct>> windowedKeySerde;
  @Mock
  private ProcessorContext processorCtx;
  @Mock
  private ConsumedFactory consumedFactory;
  @Mock
  private StreamsFactories streamsFactories;
  @Mock
  private Consumed<Struct, GenericRow> consumed;
  @Mock
  private Consumed<Windowed<Struct>, GenericRow> consumedWindowed;
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
  @Captor
  private ArgumentCaptor<ValueTransformerWithKeySupplier<?, GenericRow, GenericRow>> transformSupplierCaptor;
  @Captor
  private ArgumentCaptor<TimestampExtractor> timestampExtractorCaptor;
  @Captor
  private ArgumentCaptor<StaticTopicSerde<GenericRow>> serdeCaptor;
  private final GenericRow row = genericRow("baz", 123);
  private PlanBuilder planBuilder;

  private StreamSource streamSource;
  private WindowedStreamSource windowedStreamSource;
  private TableSource tableSource;
  private WindowedTableSource windowedTableSource;

  @Rule
  public final MockitoRule mockitoRule = MockitoJUnit.rule();

  @Before
  @SuppressWarnings("unchecked")
  public void setup() {
    when(queryBuilder.getStreamsBuilder()).thenReturn(streamsBuilder);
    when(queryBuilder.getProcessingLogger(any())).thenReturn(processingLogger);
    when(streamsBuilder.stream(anyString(), any(Consumed.class))).thenReturn(kStream);
    when(streamsBuilder.table(anyString(), any(), any())).thenReturn(kTable);
    when(streamsBuilder.table(anyString(), any(Consumed.class))).thenReturn(kTable);
    when(kTable.mapValues(any(ValueMapper.class))).thenReturn(kTable);
    when(kTable.mapValues(any(ValueMapper.class), any(Materialized.class))).thenReturn(kTable);
    when(kStream.transformValues(any(ValueTransformerWithKeySupplier.class))).thenReturn(kStream);
    when(kTable.transformValues(any(ValueTransformerWithKeySupplier.class))).thenReturn(kTable);
    when(queryBuilder.buildKeySerde(any(), any(), any())).thenReturn(keySerde);
    when(queryBuilder.buildValueSerde(any(), any(), any())).thenReturn(valueSerde);
    when(queryBuilder.getServiceContext()).thenReturn(serviceContext);
    when(queryBuilder.getQueryId()).thenReturn(new QueryId("id"));
    when(queryBuilder.getKsqlConfig()).thenReturn(KSQL_CONFIG);
    when(processorCtx.timestamp()).thenReturn(A_ROWTIME);
    when(serviceContext.getSchemaRegistryClient()).thenReturn(srClient);
    when(streamsFactories.getConsumedFactory()).thenReturn(consumedFactory);
    when(streamsFactories.getMaterializedFactory()).thenReturn(materializationFactory);
    when(materializationFactory.create(any(), any(), any()))
        .thenReturn((Materialized) materialized);
    when(valueFormatInfo.getFormat()).thenReturn(FormatFactory.AVRO.name());

    planBuilder = new KSPlanBuilder(
        queryBuilder,
        mock(SqlPredicateFactory.class),
        mock(AggregateParamsFactory.class),
        streamsFactories
    );
  }

  @Test
  @SuppressWarnings("unchecked")
  public void shouldApplyCorrectTransformationsToSourceStream() {
    // Given:
    givenUnwindowedSourceStream();

    // When:
    final KStreamHolder<?> builtKstream = streamSource.build(planBuilder);

    // Then:
    assertThat(builtKstream.getStream(), is(kStream));
    final InOrder validator = inOrder(streamsBuilder, kStream);
    validator.verify(streamsBuilder).stream(TOPIC_NAME, consumed);
    validator.verify(kStream, never()).mapValues(any(ValueMapper.class));
    validator.verify(kStream).transformValues(any(ValueTransformerWithKeySupplier.class));
    verify(consumedFactory).create(keySerde, valueSerde);
    verify(consumed).withTimestampExtractor(any());
    verify(consumed).withOffsetResetPolicy(any());
  }

  @Test
  public void shouldUseOffsetResetLatestForStream() {
    // Given:
    givenUnwindowedSourceStream();

    // When
    streamSource.build(planBuilder);

    // Then:
    verify(consumed).withOffsetResetPolicy(AutoOffsetReset.LATEST);
  }

  @Test
  public void shouldUseOffsetResetLatestForWindowedStream() {
    // Given:
    givenWindowedSourceStream();

    // When
    windowedStreamSource.build(planBuilder);

    // Then:
    verify(consumedWindowed).withOffsetResetPolicy(AutoOffsetReset.LATEST);
  }

  @Test
  public void shouldUseConfiguredResetPolicyForStream() {
    // Given:
    when(queryBuilder.getKsqlConfig()).thenReturn(new KsqlConfig(
        ImmutableMap.of(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    ));
    givenUnwindowedSourceStream();

    // When
    streamSource.build(planBuilder);

    // Then:
    verify(consumed).withOffsetResetPolicy(AutoOffsetReset.EARLIEST);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void shouldBuildStreamWithCorrectTimestampExtractor() {
    // Given:
    givenUnwindowedSourceStream();
    final ConsumerRecord<Object, Object> record = mock(ConsumerRecord.class);
    when(record.value()).thenReturn(GenericRow.genericRow("123", A_ROWTIME));

    // When:
    streamSource.build(planBuilder);

    // Then:
    verify(consumed).withTimestampExtractor(timestampExtractorCaptor.capture());
    final TimestampExtractor extractor = timestampExtractorCaptor.getValue();
    assertThat(extractor.extract(record, 789), is(A_ROWTIME));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void shouldApplyCorrectTransformationsToSourceTable() {
    // Given:
    givenUnwindowedSourceTable(true);

    // When:
    final KTableHolder<Struct> builtKTable = tableSource.build(planBuilder);

    // Then:
    assertThat(builtKTable.getTable(), is(kTable));
    final InOrder validator = inOrder(streamsBuilder, kTable);
    validator.verify(streamsBuilder).table(eq(TOPIC_NAME), eq(consumed));
    validator.verify(kTable).mapValues(any(ValueMapper.class), any(Materialized.class));
    validator.verify(kTable).transformValues(any(ValueTransformerWithKeySupplier.class));
    verify(consumedFactory).create(keySerde, valueSerde);
    verify(consumed).withTimestampExtractor(any());
    verify(consumed).withOffsetResetPolicy(AutoOffsetReset.EARLIEST);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void shouldApplyCorrectTransformationsToSourceTableWithoutForcingChangelog() {
    // Given:
    givenUnwindowedSourceTable(false);
    when(consumed.withValueSerde(any())).thenReturn(consumed);

    // When:
    final KTableHolder<Struct> builtKTable = tableSource.build(planBuilder);

    // Then:
    assertThat(builtKTable.getTable(), is(kTable));
    final InOrder validator = inOrder(streamsBuilder, kTable);
    validator.verify(streamsBuilder).table(eq(TOPIC_NAME), eq(consumed), any());
    validator.verify(kTable, never()).mapValues(any(ValueMapper.class));
    validator.verify(kTable, never()).mapValues(any(ValueMapper.class), any(Materialized.class));
    validator.verify(kTable).transformValues(any(ValueTransformerWithKeySupplier.class));
    verify(consumedFactory).create(keySerde, valueSerde);
    verify(consumed).withTimestampExtractor(any());
    verify(consumed).withOffsetResetPolicy(AutoOffsetReset.EARLIEST);

    verify(consumed).withValueSerde(serdeCaptor.capture());
    final StaticTopicSerde<GenericRow> value = serdeCaptor.getValue();
    assertThat(value.getTopic(), is("_confluent-ksql-default_query_id-base-Reduce-changelog"));
  }

  @Test
  public void shouldApplyCreateSchemaRegistryCallbackIfSchemaRegistryIsEnabled() {
    // Given:
    when(queryBuilder.getKsqlConfig()).thenReturn(
        KSQL_CONFIG.cloneWithPropertyOverwrite(
            ImmutableMap.of(KsqlConfig.SCHEMA_REGISTRY_URL_PROPERTY, "foo")));
    givenUnwindowedSourceTable(false);
    when(consumed.withValueSerde(any())).thenReturn(consumed);

    // When:
    tableSource.build(planBuilder);

    // Then:
    verify(consumed).withValueSerde(serdeCaptor.capture());
    final StaticTopicSerde<GenericRow> value = serdeCaptor.getValue();
    assertThat(value.getOnFailure(), instanceOf(RegisterSchemaCallback.class));
  }


  @Test
  public void shouldUseOffsetResetEarliestForTable() {
    // Given:
    givenUnwindowedSourceTable(true);

    // When
    tableSource.build(planBuilder);

    // Then:
    verify(consumed).withOffsetResetPolicy(AutoOffsetReset.EARLIEST);
  }

  @Test
  public void shouldUseOffsetResetEarliestForWindowedTable() {
    // Given:
    givenWindowedSourceTable();

    // When
    windowedTableSource.build(planBuilder);

    // Then:
    verify(consumedWindowed).withOffsetResetPolicy(AutoOffsetReset.EARLIEST);
  }

  @Test
  public void shouldUseConfiguredResetPolicyForTable() {
    // Given:
    when(queryBuilder.getKsqlConfig()).thenReturn(new KsqlConfig(
        ImmutableMap.of(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")
    ));
    givenUnwindowedSourceTable(true);

    // When
    tableSource.build(planBuilder);

    // Then:
    verify(consumed).withOffsetResetPolicy(AutoOffsetReset.LATEST);
  }

  @Test
  public void shouldReturnCorrectSchemaForUnwindowedSourceStream() {
    // Given:
    givenUnwindowedSourceStream();

    // When:
    final KStreamHolder<?> builtKstream = streamSource.build(planBuilder);

    // Then:
    assertThat(builtKstream.getSchema(), is(SCHEMA));
  }

  @Test
  public void shouldReturnCorrectSchemaForUnwindowedSourceTable() {
    // Given:
    givenUnwindowedSourceTable(true);

    // When:
    final KTableHolder<Struct> builtKTable = tableSource.build(planBuilder);

    // Then:
    assertThat(builtKTable.getSchema(), is(SCHEMA));
  }

  @Test
  public void shouldBuildSourceValueSerdeCorrectly() {
    // Given:
    givenUnwindowedSourceStream();

    // When:
    streamSource.build(planBuilder);

    // Then:
    verify(queryBuilder).buildValueSerde(valueFormatInfo, PHYSICAL_SCHEMA, ctx);
  }

  @Test
  public void shouldBuildSourceKeySerdeCorrectly() {
    // Given:
    givenWindowedSourceStream();

    // When:
    windowedStreamSource.build(planBuilder);

    // Then:
    verify(queryBuilder).buildKeySerde(
        keyFormatInfo,
        windowInfo,
        PhysicalSchema.from(SOURCE_SCHEMA, SERDE_OPTIONS),
        ctx
    );
  }

  @Test
  public void shouldReturnCorrectSchemaForWindowedSourceStream() {
    // Given:
    givenWindowedSourceStream();

    // When:
    final KStreamHolder<?> builtKstream = windowedStreamSource.build(planBuilder);

    // Then:
    assertThat(builtKstream.getSchema(), is(WINDOWED_SCHEMA));
  }

  @Test
  public void shouldReturnCorrectSchemaForWindowedSourceTable() {
    // Given:
    givenWindowedSourceTable();

    // When:
    final KTableHolder<Windowed<Struct>> builtKTable = windowedTableSource.build(planBuilder);

    // Then:
    assertThat(builtKTable.getSchema(), is(WINDOWED_SCHEMA));
  }

  @Test
  public void shouldThrowOnMultiFieldKey() {
    // Given:
    givenUnwindowedSourceStream();
    final StreamSource streamSource = new StreamSource(
        new ExecutionStepPropertiesV1(ctx),
        TOPIC_NAME,
        Formats.of(keyFormatInfo, valueFormatInfo, SERDE_OPTIONS),
        Optional.empty(),
        LogicalSchema.builder()
            .keyColumn(ColumnName.of("f1"), SqlTypes.INTEGER)
            .keyColumn(ColumnName.of("f2"), SqlTypes.BIGINT)
            .valueColumns(SCHEMA.value())
            .build()
    );

    // When:
    assertThrows(
        IllegalStateException.class,
        () -> streamSource.build(planBuilder)
    );
  }

  @Test
  public void shouldAddRowTimeAndRowKeyColumnsToNonWindowedStream() {
    // Given:
    givenUnwindowedSourceStream();
    final ValueTransformerWithKey<Struct, GenericRow, GenericRow> transformer =
        getTransformerFromStreamSource(streamSource);

    // When:
    final GenericRow withTimestamp = transformer.transform(KEY, row);

    // Then:
    assertThat(withTimestamp, equalTo(GenericRow.genericRow("baz", 123, A_ROWTIME, A_KEY)));
  }

  @Test
  public void shouldAddRowTimeAndRowKeyColumnsToNonWindowedTable() {
    // Given:
    givenUnwindowedSourceTable(true);
    final ValueTransformerWithKey<Struct, GenericRow, GenericRow> transformer =
        getTransformerFromTableSource(tableSource);

    // When:
    final GenericRow withTimestamp = transformer.transform(KEY, row);

    // Then:
    assertThat(withTimestamp, equalTo(GenericRow.genericRow("baz", 123, A_ROWTIME, A_KEY)));
  }

  @Test
  public void shouldHandleNullKey() {
    // Given:
    givenUnwindowedSourceStream();
    final ValueTransformerWithKey<Struct, GenericRow, GenericRow> transformer =
        getTransformerFromStreamSource(streamSource);

    final Struct nullKey = new Struct(KEY_SCHEMA);

    // When:
    final GenericRow withTimestamp = transformer.transform(nullKey, row);

    // Then:
    assertThat(withTimestamp, equalTo(GenericRow.genericRow("baz", 123, A_ROWTIME, null)));
  }

  @Test
  public void shouldAddRowTimeAndTimeWindowedRowKeyColumnsToStream() {
    // Given:
    givenWindowedSourceStream();
    final ValueTransformerWithKey<Windowed<Struct>, GenericRow, GenericRow> transformer =
        getTransformerFromStreamSource(windowedStreamSource);

    final Windowed<Struct> key = new Windowed<>(
        KEY,
        new TimeWindow(A_WINDOW_START, A_WINDOW_END)
    );

    // When:
    final GenericRow withTimestamp = transformer.transform(key, row);

    // Then:
    assertThat(withTimestamp,
        equalTo(GenericRow.genericRow("baz", 123, A_ROWTIME, A_KEY, A_WINDOW_START, A_WINDOW_END)));
  }

  @Test
  public void shouldAddRowTimeAndTimeWindowedRowKeyColumnsToTable() {
    // Given:
    givenWindowedSourceTable();
    final ValueTransformerWithKey<Windowed<Struct>, GenericRow, GenericRow> transformer =
        getTransformerFromTableSource(windowedTableSource);

    final Windowed<Struct> key = new Windowed<>(
        KEY,
        new TimeWindow(A_WINDOW_START, A_WINDOW_END)
    );

    // When:
    final GenericRow withTimestamp = transformer.transform(key, row);

    // Then:
    assertThat(withTimestamp,
        is(GenericRow.genericRow("baz", 123, A_ROWTIME, A_KEY, A_WINDOW_START, A_WINDOW_END)));
  }

  @Test
  public void shouldAddRowTimeAndSessionWindowedRowKeyColumnsToStream() {
    // Given:
    givenWindowedSourceStream();
    final ValueTransformerWithKey<Windowed<Struct>, GenericRow, GenericRow> transformer =
        getTransformerFromStreamSource(windowedStreamSource);

    final Windowed<Struct> key = new Windowed<>(
        KEY,
        new SessionWindow(A_WINDOW_START, A_WINDOW_END)
    );

    // When:
    final GenericRow withTimestamp = transformer.transform(key, row);

    // Then:
    assertThat(withTimestamp,
        equalTo(GenericRow.genericRow("baz", 123, A_ROWTIME, A_KEY, A_WINDOW_START, A_WINDOW_END)));
  }

  @Test
  public void shouldAddRowTimeAndSessionWindowedRowKeyColumnsToTable() {
    // Given:
    givenWindowedSourceTable();
    final ValueTransformerWithKey<Windowed<Struct>, GenericRow, GenericRow> transformer =
        getTransformerFromTableSource(windowedTableSource);

    final Windowed<Struct> key = new Windowed<>(
        KEY,
        new SessionWindow(A_WINDOW_START, A_WINDOW_END)
    );

    // When:
    final GenericRow withTimestamp = transformer.transform(key, row);

    // Then:
    assertThat(withTimestamp,
        equalTo(GenericRow.genericRow("baz", 123, A_ROWTIME, A_KEY, A_WINDOW_START, A_WINDOW_END)));
  }

  @Test
  public void shouldUseCorrectSerdeForWindowedKey() {
    // Given:
    givenWindowedSourceStream();

    // When:
    windowedStreamSource.build(planBuilder);

    // Then:
    verify(queryBuilder).buildKeySerde(
        keyFormatInfo,
        windowInfo,
        PhysicalSchema.from(SOURCE_SCHEMA, SERDE_OPTIONS),
        ctx
    );
  }

  @Test
  public void shouldUseCorrectSerdeForNonWindowedKey() {
    // Given:
    givenUnwindowedSourceStream();

    // When:
    streamSource.build(planBuilder);

    // Then:
    verify(queryBuilder).buildKeySerde(
        keyFormatInfo,
        PhysicalSchema.from(SOURCE_SCHEMA, SERDE_OPTIONS),
        ctx
    );
  }

  @Test
  public void shouldReturnCorrectSerdeFactory() {
    // Given:
    givenUnwindowedSourceStream();

    // When:
    final KStreamHolder<?> stream = streamSource.build(planBuilder);

    // Then:
    reset(queryBuilder);
    stream.getKeySerdeFactory().buildKeySerde(keyFormatInfo, PHYSICAL_SCHEMA, ctx);
    verify(queryBuilder).buildKeySerde(keyFormatInfo, PHYSICAL_SCHEMA, ctx);
  }

  @Test
  public void shouldReturnCorrectSerdeFactoryForWindowedSource() {
    // Given:
    givenWindowedSourceStream();

    // When:
    final KStreamHolder<?> stream = windowedStreamSource.build(planBuilder);

    // Then:
    reset(queryBuilder);
    stream.getKeySerdeFactory().buildKeySerde(keyFormatInfo, PHYSICAL_SCHEMA, ctx);
    verify(queryBuilder).buildKeySerde(keyFormatInfo, windowInfo, PHYSICAL_SCHEMA, ctx);
  }

  @Test
  public void shouldBuildTableWithCorrectStoreName() {
    // Given:
    givenUnwindowedSourceTable(true);

    // When:
    tableSource.build(planBuilder);

    // Then:
    verify(materializationFactory).create(keySerde, valueSerde, "base-Reduce");
  }

  @SuppressWarnings("unchecked")
  private <K> ValueTransformerWithKey<K, GenericRow, GenericRow> getTransformerFromStreamSource(
      final SourceStep<?> streamSource
  ) {
    streamSource.build(planBuilder);
    verify(kStream).transformValues(transformSupplierCaptor.capture());
    final ValueTransformerWithKey transformer = transformSupplierCaptor.getValue().get();
    transformer.init(processorCtx);
    return transformer;
  }

  @SuppressWarnings("unchecked")
  private <K> ValueTransformerWithKey<K, GenericRow, GenericRow> getTransformerFromTableSource(
      final SourceStep<?> streamSource
  ) {
    streamSource.build(planBuilder);
    verify(kTable).transformValues(transformSupplierCaptor.capture());
    final ValueTransformerWithKey transformer = transformSupplierCaptor.getValue().get();
    transformer.init(processorCtx);
    return transformer;
  }

  private void givenWindowedSourceStream() {
    when(queryBuilder.buildKeySerde(any(), any(), any(), any())).thenReturn(windowedKeySerde);
    givenConsumed(consumedWindowed, windowedKeySerde);
    windowedStreamSource = new WindowedStreamSource(
        new ExecutionStepPropertiesV1(ctx),
        TOPIC_NAME,
        Formats.of(keyFormatInfo, valueFormatInfo, SERDE_OPTIONS),
        windowInfo,
        TIMESTAMP_COLUMN,
        SOURCE_SCHEMA
    );
  }

  private void givenUnwindowedSourceStream() {
    when(queryBuilder.buildKeySerde(any(), any(), any())).thenReturn(keySerde);
    givenConsumed(consumed, keySerde);
    streamSource = new StreamSource(
        new ExecutionStepPropertiesV1(ctx),
        TOPIC_NAME,
        Formats.of(keyFormatInfo, valueFormatInfo, SERDE_OPTIONS),
        TIMESTAMP_COLUMN,
        SOURCE_SCHEMA
    );
  }

  private void givenWindowedSourceTable() {
    when(queryBuilder.buildKeySerde(any(), any(), any(), any())).thenReturn(windowedKeySerde);
    givenConsumed(consumedWindowed, windowedKeySerde);
    givenConsumed(consumedWindowed, windowedKeySerde);
    windowedTableSource = new WindowedTableSource(
        new ExecutionStepPropertiesV1(ctx),
        TOPIC_NAME,
        Formats.of(keyFormatInfo, valueFormatInfo, SERDE_OPTIONS),
        windowInfo,
        TIMESTAMP_COLUMN,
        SOURCE_SCHEMA
    );
  }

  private void givenUnwindowedSourceTable(final Boolean forceChangelog) {
    when(queryBuilder.buildKeySerde(any(), any(), any())).thenReturn(keySerde);
    givenConsumed(consumed, keySerde);
    tableSource = new TableSource(
        new ExecutionStepPropertiesV1(ctx),
        TOPIC_NAME,
        Formats.of(keyFormatInfo, valueFormatInfo, SERDE_OPTIONS),
        TIMESTAMP_COLUMN,
        SOURCE_SCHEMA,
        Optional.of(forceChangelog)
    );
  }

  private <K> void givenConsumed(final Consumed<K, GenericRow> consumed, final Serde<K> keySerde) {
    when(consumedFactory.create(keySerde, valueSerde)).thenReturn(consumed);
    when(consumed.withTimestampExtractor(any())).thenReturn(consumed);
    when(consumed.withOffsetResetPolicy(any())).thenReturn(consumed);
  }
}
