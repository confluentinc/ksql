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

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.builder.KsqlQueryBuilder;
import io.confluent.ksql.execution.context.QueryContext;
import io.confluent.ksql.execution.context.QueryContext.Stacker;
import io.confluent.ksql.execution.plan.AbstractStreamSource;
import io.confluent.ksql.execution.plan.ExecutionStepPropertiesV1;
import io.confluent.ksql.execution.plan.Formats;
import io.confluent.ksql.execution.plan.KStreamHolder;
import io.confluent.ksql.execution.plan.KTableHolder;
import io.confluent.ksql.execution.plan.PlanBuilder;
import io.confluent.ksql.execution.plan.StreamSource;
import io.confluent.ksql.execution.plan.TableSource;
import io.confluent.ksql.execution.plan.WindowedStreamSource;
import io.confluent.ksql.execution.plan.WindowedTableSource;
import io.confluent.ksql.execution.timestamp.TimestampColumn;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.schema.ksql.ColumnRef;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.PhysicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.serde.FormatInfo;
import io.confluent.ksql.serde.SerdeOption;
import io.confluent.ksql.serde.WindowInfo;
import io.confluent.ksql.util.KsqlConfig;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Optional;
import java.util.Set;
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
import org.junit.rules.ExpectedException;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;


public class SourceBuilderTest {

  private static final LogicalSchema SOURCE_SCHEMA = LogicalSchema.builder()
      .valueColumn(ColumnName.of("field1"), SqlTypes.STRING)
      .valueColumn(ColumnName.of("field2"), SqlTypes.BIGINT)
      .build();
  private static final Schema KEY_SCHEMA = SchemaBuilder.struct()
      .field("k1", Schema.OPTIONAL_STRING_SCHEMA)
      .build();
  private static final Struct KEY = new Struct(KEY_SCHEMA).put("k1", "foo");
  private static final SourceName ALIAS = SourceName.of("alias");
  private static final LogicalSchema SCHEMA =
      SOURCE_SCHEMA.withMetaAndKeyColsInValue().withAlias(ALIAS);

  private static final KsqlConfig KSQL_CONFIG = new KsqlConfig(ImmutableMap.of());

  private static final Optional<TimestampColumn> TIMESTAMP_COLUMN = Optional.of(
      new TimestampColumn(
          ColumnRef.withoutSource(ColumnName.of("field2")),
          Optional.empty()
      )
  );

  private final Set<SerdeOption> SERDE_OPTIONS = new HashSet<>();
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
  @Captor
  private ArgumentCaptor<ValueTransformerWithKeySupplier> transformSupplierCaptor;
  @Captor
  private ArgumentCaptor<TimestampExtractor> timestampExtractorCaptor;
  private Optional<AutoOffsetReset> offsetReset = Optional.of(AutoOffsetReset.EARLIEST);
  private final GenericRow row = new GenericRow(new LinkedList<>(ImmutableList.of("baz", 123)));
  private PlanBuilder planBuilder;

  private StreamSource streamSource;
  private WindowedStreamSource windowedStreamSource;
  private TableSource tableSource;
  private WindowedTableSource windowedTableSource;

  @Rule
  public final MockitoRule mockitoRule = MockitoJUnit.rule();

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Before
  @SuppressWarnings("unchecked")
  public void setup() {
    when(queryBuilder.getStreamsBuilder()).thenReturn(streamsBuilder);
    when(streamsBuilder.stream(anyString(), any(Consumed.class))).thenReturn(kStream);
    when(streamsBuilder.table(anyString(), any(), any())).thenReturn(kTable);
    when(kStream.mapValues(any(ValueMapper.class))).thenReturn(kStream);
    when(kTable.mapValues(any(ValueMapper.class))).thenReturn(kTable);
    when(kStream.transformValues(any(ValueTransformerWithKeySupplier.class))).thenReturn(kStream);
    when(kTable.transformValues(any(ValueTransformerWithKeySupplier.class))).thenReturn(kTable);
    when(queryBuilder.buildKeySerde(any(), any(), any())).thenReturn(keySerde);
    when(queryBuilder.buildValueSerde(any(), any(), any())).thenReturn(valueSerde);
    when(queryBuilder.getKsqlConfig()).thenReturn(KSQL_CONFIG);
    when(processorCtx.timestamp()).thenReturn(456L);
    when(streamsFactories.getConsumedFactory()).thenReturn(consumedFactory);
    when(streamsFactories.getMaterializedFactory()).thenReturn(materializationFactory);
    when(materializationFactory.create(any(), any(), any()))
        .thenReturn((Materialized) materialized);

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
    verify(consumed).withOffsetResetPolicy(AutoOffsetReset.EARLIEST);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void shouldBuildStreamWithCorrectTimestampExtractor() {
    // Given:
    givenUnwindowedSourceStream();
    final ConsumerRecord<Object, Object> record = mock(ConsumerRecord.class);
    when(record.value()).thenReturn(new GenericRow("123", 456L));

    // When:
    streamSource.build(planBuilder);

    // Then:
    verify(consumed).withTimestampExtractor(timestampExtractorCaptor.capture());
    final TimestampExtractor extractor = timestampExtractorCaptor.getValue();
    assertThat(extractor.extract(record, 789), is(456L));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void shouldApplyCorrectTransformationsToSourceTable() {
    // Given:
    givenUnwindowedSourceTable();

    // When:
    final KTableHolder<Struct> builtKTable = tableSource.build(planBuilder);

    // Then:
    assertThat(builtKTable.getTable(), is(kTable));
    final InOrder validator = inOrder(streamsBuilder, kTable);
    validator.verify(streamsBuilder).table(eq(TOPIC_NAME), eq(consumed), any());
    validator.verify(kTable, never()).mapValues(any(ValueMapper.class));
    validator.verify(kTable).transformValues(any(ValueTransformerWithKeySupplier.class));
    verify(consumedFactory).create(keySerde, valueSerde);
    verify(consumed).withTimestampExtractor(any());
    verify(consumed).withOffsetResetPolicy(AutoOffsetReset.EARLIEST);
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
    givenUnwindowedSourceTable();

    // When:
    final KTableHolder<Struct> builtKTable = tableSource.build(planBuilder);

    // Then:
    assertThat(builtKTable.getSchema(), is(SCHEMA));
  }

  @Test
  public void shouldNotBuildWithOffsetResetIfNotProvided() {
    // Given:
    offsetReset = Optional.empty();
    givenUnwindowedSourceStream();

    // When
    streamSource.build(planBuilder);

    // Then:
    verify(consumedFactory).create(keySerde, valueSerde);
    verify(consumed).withTimestampExtractor(any());
    verifyNoMoreInteractions(consumed, consumedFactory);
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
    assertThat(builtKstream.getSchema(), is(SCHEMA));
  }

  @Test
  public void shouldReturnCorrectSchemaForWindowedSourceTable() {
    // Given:
    givenWindowedSourceTable();

    // When:
    final KTableHolder<Windowed<Struct>> builtKTable = windowedTableSource.build(planBuilder);

    // Then:
    assertThat(builtKTable.getSchema(), is(SCHEMA));
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
        offsetReset,
        LogicalSchema.builder()
            .keyColumn(ColumnName.of("f1"), SqlTypes.INTEGER)
            .keyColumn(ColumnName.of("f2"), SqlTypes.BIGINT)
            .valueColumns(SCHEMA.value())
            .build(),
        ALIAS
    );

    // Then:
    expectedException.expect(instanceOf(IllegalStateException.class));

    // When:
    streamSource.build(planBuilder);
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
    assertThat(withTimestamp, equalTo(new GenericRow(456L, "foo", "baz", 123)));
  }

  @Test
  public void shouldAddRowTimeAndRowKeyColumnsToNonWindowedTable() {
    // Given:
    givenUnwindowedSourceTable();
    final ValueTransformerWithKey<Struct, GenericRow, GenericRow> transformer =
        getTransformerFromTableSource(tableSource);

    // When:
    final GenericRow withTimestamp = transformer.transform(KEY, row);

    // Then:
    assertThat(withTimestamp, equalTo(new GenericRow(456L, "foo", "baz", 123)));
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
    assertThat(withTimestamp, equalTo(new GenericRow(456L, null, "baz", 123)));
  }

  @Test
  public void shouldAddRowTimeAndTimeWindowedRowKeyColumnsToStream() {
    // Given:
    givenWindowedSourceStream();
    final ValueTransformerWithKey<Windowed<Struct>, GenericRow, GenericRow> transformer =
        getTransformerFromStreamSource(windowedStreamSource);

    final Windowed<Struct> key = new Windowed<>(
        KEY,
        new TimeWindow(10L, 20L)
    );

    // When:
    final GenericRow withTimestamp = transformer.transform(key, row);

    // Then:
    assertThat(withTimestamp,
        equalTo(new GenericRow(456L, "foo : Window{start=10 end=-}", "baz", 123)));
  }

  @Test
  public void shouldAddRowTimeAndTimeWindowedRowKeyColumnsToTable() {
    // Given:
    givenWindowedSourceTable();
    final ValueTransformerWithKey<Windowed<Struct>, GenericRow, GenericRow> transformer =
        getTransformerFromTableSource(windowedTableSource);

    final Windowed<Struct> key = new Windowed<>(
        KEY,
        new TimeWindow(10L, 20L)
    );

    // When:
    final GenericRow withTimestamp = transformer.transform(key, row);

    // Then:
    assertThat(withTimestamp,
        equalTo(new GenericRow(456L, "foo : Window{start=10 end=-}", "baz", 123)));
  }

  @Test
  public void shouldAddRowTimeAndSessionWindowedRowKeyColumnsToStream() {
    // Given:
    givenWindowedSourceStream();
    final ValueTransformerWithKey<Windowed<Struct>, GenericRow, GenericRow> transformer =
        getTransformerFromStreamSource(windowedStreamSource);

    final Windowed<Struct> key = new Windowed<>(
        KEY,
        new SessionWindow(10L, 20L)
    );

    // When:
    final GenericRow withTimestamp = transformer.transform(key, row);

    // Then:
    assertThat(withTimestamp,
        equalTo(new GenericRow(456L, "foo : Window{start=10 end=20}", "baz", 123)));
  }

  @Test
  public void shouldAddRowTimeAndSessionWindowedRowKeyColumnsToTable() {
    // Given:
    givenWindowedSourceTable();
    final ValueTransformerWithKey<Windowed<Struct>, GenericRow, GenericRow> transformer =
        getTransformerFromTableSource(windowedTableSource);

    final Windowed<Struct> key = new Windowed<>(
        KEY,
        new SessionWindow(10L, 20L)
    );

    // When:
    final GenericRow withTimestamp = transformer.transform(key, row);

    // Then:
    assertThat(withTimestamp,
        equalTo(new GenericRow(456L, "foo : Window{start=10 end=20}", "baz", 123)));
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
    givenUnwindowedSourceTable();

    // When:
    tableSource.build(planBuilder);

    // Then:
    verify(materializationFactory).create(keySerde, valueSerde, "base-Reduce");
  }

  @SuppressWarnings("unchecked")
  private <K> ValueTransformerWithKey<K, GenericRow, GenericRow> getTransformerFromStreamSource(
      final AbstractStreamSource<?> streamSource
  ) {
    streamSource.build(planBuilder);
    verify(kStream).transformValues(transformSupplierCaptor.capture());
    final ValueTransformerWithKey transformer = transformSupplierCaptor.getValue().get();
    transformer.init(processorCtx);
    return transformer;
  }

  @SuppressWarnings("unchecked")
  private <K> ValueTransformerWithKey<K, GenericRow, GenericRow> getTransformerFromTableSource(
      final AbstractStreamSource<?> streamSource
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
        offsetReset,
        SOURCE_SCHEMA,
        ALIAS
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
        offsetReset,
        SOURCE_SCHEMA,
        ALIAS
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
        offsetReset,
        SOURCE_SCHEMA,
        ALIAS
    );
  }

  private void givenUnwindowedSourceTable() {
    when(queryBuilder.buildKeySerde(any(), any(), any())).thenReturn(keySerde);
    givenConsumed(consumed, keySerde);
    tableSource = new TableSource(
        new ExecutionStepPropertiesV1(ctx),
        TOPIC_NAME,
        Formats.of(keyFormatInfo, valueFormatInfo, SERDE_OPTIONS),
        TIMESTAMP_COLUMN,
        offsetReset,
        SOURCE_SCHEMA,
        ALIAS
    );
  }

  private <K> void givenConsumed(final Consumed<K, GenericRow> consumed, final Serde<K> keySerde) {
    when(consumedFactory.create(keySerde, valueSerde)).thenReturn(consumed);
    when(consumed.withTimestampExtractor(any())).thenReturn(consumed);
    when(consumed.withOffsetResetPolicy(any())).thenReturn(consumed);
  }
}
