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
import static io.confluent.ksql.schema.ksql.SystemColumns.LEGACY_PSEUDOCOLUMN_VERSION_NUMBER;
import static io.confluent.ksql.schema.ksql.SystemColumns.ROWPARTITION_ROWOFFSET_PSEUDOCOLUMN_VERSION;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.GenericKey;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.context.QueryContext;
import io.confluent.ksql.execution.context.QueryContext.Stacker;
import io.confluent.ksql.execution.plan.ExecutionStep;
import io.confluent.ksql.execution.plan.ExecutionStepPropertiesV1;
import io.confluent.ksql.execution.plan.Formats;
import io.confluent.ksql.execution.plan.KStreamHolder;
import io.confluent.ksql.execution.plan.KTableHolder;
import io.confluent.ksql.execution.plan.PlanBuilder;
import io.confluent.ksql.execution.plan.PlanInfo;
import io.confluent.ksql.execution.plan.SourceStep;
import io.confluent.ksql.execution.plan.StreamSource;
import io.confluent.ksql.execution.plan.TableSourceV1;
import io.confluent.ksql.execution.plan.WindowedStreamSource;
import io.confluent.ksql.execution.plan.WindowedTableSource;
import io.confluent.ksql.execution.runtime.MaterializedFactory;
import io.confluent.ksql.execution.runtime.RuntimeBuildContext;
import io.confluent.ksql.execution.timestamp.TimestampColumn;
import io.confluent.ksql.logging.processing.ProcessingLogger;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.PhysicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.serde.FormatFactory;
import io.confluent.ksql.serde.FormatInfo;
import io.confluent.ksql.serde.SerdeFeatures;
import io.confluent.ksql.serde.StaticTopicSerde;
import io.confluent.ksql.serde.WindowInfo;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
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
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.kstream.ValueTransformerWithKeySupplier;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.SessionWindow;
import org.apache.kafka.streams.kstream.internals.TimeWindow;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.TimestampExtractor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorSupplier;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;
import org.apache.kafka.streams.processor.api.RecordMetadata;
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

public class SourceBuilderV1Test {

  private static final ColumnName K0 = ColumnName.of("k0");
  private static final ColumnName K1 = ColumnName.of("k1");

  private static final LogicalSchema SOURCE_SCHEMA = LogicalSchema.builder()
      .keyColumn(K0, SqlTypes.BIGINT)
      .valueColumn(ColumnName.of("field1"), SqlTypes.STRING)
      .valueColumn(ColumnName.of("field2"), SqlTypes.BIGINT)
      .headerColumn(ColumnName.of("headers"), Optional.empty())
      .build();

  private static final LogicalSchema MULTI_COL_SOURCE_SCHEMA = LogicalSchema.builder()
      .keyColumn(K0, SqlTypes.BIGINT)
      .keyColumn(K1, SqlTypes.BIGINT)
      .valueColumn(ColumnName.of("field1"), SqlTypes.STRING)
      .valueColumn(ColumnName.of("field2"), SqlTypes.BIGINT)
      .headerColumn(ColumnName.of("header1"), Optional.of("a"))
      .headerColumn(ColumnName.of("header2"), Optional.of("b"))
      .headerColumn(ColumnName.of("header3"), Optional.of("c"))
      .build();

  private static final double A_KEY = 10.11;

  private static final GenericKey KEY = GenericKey.genericKey(A_KEY);

  private static final LogicalSchema SCHEMA_WITH_V0_PSEUDOCOLUMNS = SOURCE_SCHEMA
      .withPseudoAndKeyColsInValue(false, LEGACY_PSEUDOCOLUMN_VERSION_NUMBER);

  private static final LogicalSchema WINDOWED_SCHEMA_WITH_V0_PSEUDOCOLUMNS = SOURCE_SCHEMA
      .withPseudoAndKeyColsInValue(true, LEGACY_PSEUDOCOLUMN_VERSION_NUMBER);

  private static final LogicalSchema SCHEMA_WITH_V1_PSEUDOCOLUMNS = SOURCE_SCHEMA
      .withPseudoAndKeyColsInValue(false, ROWPARTITION_ROWOFFSET_PSEUDOCOLUMN_VERSION);

  private static final LogicalSchema WINDOWED_SCHEMA_WITH_V1_PSEUDOCOLUMNS = SOURCE_SCHEMA
      .withPseudoAndKeyColsInValue(true, ROWPARTITION_ROWOFFSET_PSEUDOCOLUMN_VERSION);

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
  private final Headers HEADERS = new RecordHeaders(ImmutableList.of(
      new RecordHeader("a", new byte[] {20}),
      new RecordHeader("b", new byte[] {25}))
  );
  private final ByteBuffer HEADER_A = ByteBuffer.wrap(new byte[] {20});
  private final ByteBuffer HEADER_B = ByteBuffer.wrap(new byte[] {25});
  private final List<Struct> HEADER_DATA = ImmutableList.of(
      new Struct(SchemaBuilder.struct()
          .field("KEY", Schema.OPTIONAL_STRING_SCHEMA)
          .field("VALUE", Schema.OPTIONAL_BYTES_SCHEMA)
          .optional()
          .build())
          .put("KEY", "a")
          .put("VALUE", HEADER_A),
      new Struct(SchemaBuilder.struct()
          .field("KEY", Schema.OPTIONAL_STRING_SCHEMA)
          .field("VALUE", Schema.OPTIONAL_BYTES_SCHEMA)
          .optional()
          .build())
          .put("KEY", "b")
          .put("VALUE", HEADER_B)
  );

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
  private FixedKeyProcessorContext fixedKeyProcessorContext;
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
  private ArgumentCaptor<FixedKeyProcessorSupplier<?, GenericRow, GenericRow>> fixedKeyProcessorSupplierArgumentCaptor;
  @Captor
  private ArgumentCaptor<TimestampExtractor> timestampExtractorCaptor;
  @Captor
  private ArgumentCaptor<StaticTopicSerde<GenericRow>> serdeCaptor;
  private final GenericRow row = genericRow("baz", 123);
  private PlanBuilder planBuilder;

  private StreamSource streamSource;
  private WindowedStreamSource windowedStreamSource;
  private TableSourceV1 tableSourceV1;
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
    when(kStream.processValues(any(FixedKeyProcessorSupplier.class))).thenReturn(kStream);
    when(kStream.processValues(any(FixedKeyProcessorSupplier.class), any(Named.class))).thenReturn(kStream);
    when(kTable.transformValues(any(ValueTransformerWithKeySupplier.class))).thenReturn(kTable);
    when(buildContext.buildKeySerde(any(), any(), any())).thenReturn(keySerde);
    when(buildContext.buildValueSerde(any(), any(), any())).thenReturn(valueSerde);
    when(buildContext.getServiceContext()).thenReturn(serviceContext);
    when(buildContext.getKsqlConfig()).thenReturn(KSQL_CONFIG);
    when(processorCtx.timestamp()).thenReturn(A_ROWTIME);
    when(processorCtx.partition()).thenReturn(A_ROWPARTITION);
    when(processorCtx.offset()).thenReturn(A_ROWOFFSET);
    when(processorCtx.headers()).thenReturn(HEADERS);
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
  public void shouldApplyCorrectTransformationsToSourceStream() {
    // Given:
    givenUnwindowedSourceStream();

    // When:
    final KStreamHolder<?> builtKstream = streamSource.build(planBuilder, planInfo);

    // Then:
    assertThat(builtKstream.getStream(), is(kStream));
    final InOrder validator = inOrder(streamsBuilder, kStream);
    validator.verify(streamsBuilder).stream(TOPIC_NAME, consumed);
    validator.verify(kStream, never()).mapValues(any(ValueMapper.class));
    validator.verify(kStream).processValues(any(FixedKeyProcessorSupplier.class));
    verify(consumedFactory).create(keySerde, valueSerde);
    verify(consumed).withTimestampExtractor(any());
    verify(consumed).withOffsetResetPolicy(any());
  }

  @Test
  public void shouldUseOffsetResetLatestForStream() {
    // Given:
    givenUnwindowedSourceStream();

    // When
    streamSource.build(planBuilder, planInfo);

    // Then:
    verify(consumed).withOffsetResetPolicy(AutoOffsetReset.LATEST);
  }

  @Test
  public void shouldUseOffsetResetLatestForWindowedStream() {
    // Given:
    givenWindowedSourceStream();

    // When
    windowedStreamSource.build(planBuilder, planInfo);

    // Then:
    verify(consumedWindowed).withOffsetResetPolicy(AutoOffsetReset.LATEST);
  }

  @Test
  public void shouldUseConfiguredResetPolicyForStream() {
    // Given:
    when(buildContext.getKsqlConfig()).thenReturn(new KsqlConfig(
        ImmutableMap.of(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    ));
    givenUnwindowedSourceStream();

    // When
    streamSource.build(planBuilder, planInfo);

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
    streamSource.build(planBuilder, planInfo);

    // Then:
    verify(consumed).withTimestampExtractor(timestampExtractorCaptor.capture());
    final TimestampExtractor extractor = timestampExtractorCaptor.getValue();
    assertThat(extractor.extract(record, 789), is(A_ROWTIME));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void shouldApplyCorrectTransformationsToSourceTable() {
    // Given:
    givenUnwindowedSourceTableV1(true, LEGACY_PSEUDOCOLUMN_VERSION_NUMBER);

    // When:
    final KTableHolder<GenericKey> builtKTable = tableSourceV1.build(planBuilder, planInfo);

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
    givenUnwindowedSourceTableV1(false, LEGACY_PSEUDOCOLUMN_VERSION_NUMBER);
    when(consumed.withValueSerde(any())).thenReturn(consumed);

    // When:
    final KTableHolder<GenericKey> builtKTable = tableSourceV1.build(planBuilder, planInfo);

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
    assertThat(value.getTopic(), is("appid-base-Reduce-changelog"));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void shouldApplyCorrectTransformationsToSourceTableWithDownstreamRepartition() {
    // Given:
    givenUnwindowedSourceTableV1(true, LEGACY_PSEUDOCOLUMN_VERSION_NUMBER);
    final PlanInfo planInfo = givenDownstreamRepartition(tableSourceV1);

    // When:
    final KTableHolder<GenericKey> builtKTable = tableSourceV1.build(planBuilder, planInfo);

    // Then:
    assertThat(builtKTable.getTable(), is(kTable));
    final InOrder validator = inOrder(streamsBuilder, kTable);
    validator.verify(streamsBuilder).table(eq(TOPIC_NAME), eq(consumed));
    validator.verify(kTable).transformValues(any(ValueTransformerWithKeySupplier.class));
    verify(consumedFactory).create(keySerde, valueSerde);
    verify(consumed).withTimestampExtractor(any());
    verify(consumed).withOffsetResetPolicy(AutoOffsetReset.EARLIEST);

    verify(kTable, never()).mapValues(any(ValueMapper.class), any(Materialized.class));
  }

  @Test
  public void shouldApplyCreateSchemaRegistryCallbackIfSchemaRegistryIsEnabled() {
    // Given:
    when(buildContext.getKsqlConfig()).thenReturn(
        KSQL_CONFIG.cloneWithPropertyOverwrite(
            ImmutableMap.of(KsqlConfig.SCHEMA_REGISTRY_URL_PROPERTY, "foo")));
    givenUnwindowedSourceTableV1(false, LEGACY_PSEUDOCOLUMN_VERSION_NUMBER);
    when(consumed.withValueSerde(any())).thenReturn(consumed);

    // When:
    tableSourceV1.build(planBuilder, planInfo);

    // Then:
    verify(consumed).withValueSerde(serdeCaptor.capture());
    final StaticTopicSerde<GenericRow> value = serdeCaptor.getValue();
    assertThat(value.getOnFailure(), instanceOf(RegisterSchemaCallback.class));
  }


  @Test
  public void shouldUseOffsetResetEarliestForTable() {
    // Given:
    givenUnwindowedSourceTableV1(true, LEGACY_PSEUDOCOLUMN_VERSION_NUMBER);

    // When
    tableSourceV1.build(planBuilder, planInfo);

    // Then:
    verify(consumed).withOffsetResetPolicy(AutoOffsetReset.EARLIEST);
  }

  @Test
  public void shouldUseOffsetResetEarliestForWindowedTable() {
    // Given:
    givenWindowedSourceTable(LEGACY_PSEUDOCOLUMN_VERSION_NUMBER);

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
    givenUnwindowedSourceTableV1(true, LEGACY_PSEUDOCOLUMN_VERSION_NUMBER);

    // When
    tableSourceV1.build(planBuilder, planInfo);

    // Then:
    verify(consumed).withOffsetResetPolicy(AutoOffsetReset.LATEST);
  }

  @Test
  public void shouldReturnCorrectSchemaForUnwindowedSourceStream() {
    // Given:
    givenUnwindowedSourceStream();

    // When:
    final KStreamHolder<?> builtKstream = streamSource.build(planBuilder, planInfo);

    // Then:
    assertThat(builtKstream.getSchema(), is(SCHEMA_WITH_V1_PSEUDOCOLUMNS));
  }

  @Test
  public void shouldReturnCorrectSchemaForLegacyUnwindowedSourceStream() {
    // Given:
    givenUnwindowedSourceStream(LEGACY_PSEUDOCOLUMN_VERSION_NUMBER);

    // When:
    final KStreamHolder<?> builtKstream = streamSource.build(planBuilder, planInfo);

    // Then:
    assertThat(builtKstream.getSchema(), is(SCHEMA_WITH_V0_PSEUDOCOLUMNS));
  }

  @Test
  public void shouldReturnCorrectSchemaForLegacyUnwindowedSourceTable() {
    // Given:
    givenUnwindowedSourceTableV1(true, LEGACY_PSEUDOCOLUMN_VERSION_NUMBER);

    // When:
    final KTableHolder<GenericKey> builtKTable = tableSourceV1.build(planBuilder, planInfo);

    // Then:
    assertThat(builtKTable.getSchema(), is(SCHEMA_WITH_V0_PSEUDOCOLUMNS));
  }

  @Test
  public void shouldThrowOnAttemptToCreateV1UnwindowedTableWithNewPseudoColumnVersion() {
    // Given:
    givenUnwindowedSourceTableV1(true, ROWPARTITION_ROWOFFSET_PSEUDOCOLUMN_VERSION);

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> tableSourceV1.build(planBuilder, planInfo)
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "Should not be using old execution step version with new queries"));
  }

  @Test
  public void shouldBuildSourceValueSerdeCorrectly() {
    // Given:
    givenUnwindowedSourceStream();

    // When:
    streamSource.build(planBuilder, planInfo);

    // Then:
    verify(buildContext).buildValueSerde(valueFormatInfo, PHYSICAL_SCHEMA, ctx);
  }

  @Test
  public void shouldBuildSourceKeySerdeCorrectly() {
    // Given:
    givenWindowedSourceStream();

    // When:
    windowedStreamSource.build(planBuilder, planInfo);

    // Then:
    verify(buildContext).buildKeySerde(
        keyFormatInfo,
        windowInfo,
        PhysicalSchema.from(SOURCE_SCHEMA, KEY_FEATURES, VALUE_FEATURES),
        ctx
    );
  }

  @Test
  public void shouldReturnCorrectSchemaForWindowedSourceStream() {
    // Given:
    givenWindowedSourceStream();

    // When:
    final KStreamHolder<?> builtKstream = windowedStreamSource.build(planBuilder, planInfo);

    // Then:
    assertThat(builtKstream.getSchema(), is(WINDOWED_SCHEMA_WITH_V1_PSEUDOCOLUMNS));
  }

  @Test
  public void shouldReturnCorrectSchemaForWindowedSourceTable() {
    // Given:
    givenWindowedSourceTable();

    // When:
    final KTableHolder<Windowed<GenericKey>> builtKTable = windowedTableSource.build(planBuilder, planInfo);

    // Then:
    assertThat(builtKTable.getSchema(), is(WINDOWED_SCHEMA_WITH_V1_PSEUDOCOLUMNS));
  }

  @Test
  public void shouldReturnCorrectSchemaForLegacyWindowedSourceStream() {
    // Given:
    givenWindowedSourceStream(LEGACY_PSEUDOCOLUMN_VERSION_NUMBER);

    // When:
    final KStreamHolder<?> builtKstream = windowedStreamSource.build(planBuilder, planInfo);

    // Then:
    assertThat(builtKstream.getSchema(), is(WINDOWED_SCHEMA_WITH_V0_PSEUDOCOLUMNS));
  }

  @Test
  public void shouldReturnCorrectSchemaForLegacyWindowedSourceTable() {
    // Given:
    givenWindowedSourceTable(LEGACY_PSEUDOCOLUMN_VERSION_NUMBER);

    // When:
    final KTableHolder<Windowed<GenericKey>> builtKTable = windowedTableSource
        .build(planBuilder, planInfo);

    // Then:
    assertThat(builtKTable.getSchema(), is(WINDOWED_SCHEMA_WITH_V0_PSEUDOCOLUMNS));
  }

  @Test
  public void shouldAddRowTimeAndRowKeyColumnsToLegacyNonWindowedStream() {
    // Given:
    givenUnwindowedSourceStream(LEGACY_PSEUDOCOLUMN_VERSION_NUMBER);
    final FixedKeyProcessor<GenericKey, GenericRow, GenericRow> processor =
        getProcessorFromStreamSource(streamSource);

    assertFixedKeyProcessorContextForwardsValue(
        processor,
        KEY,
        row,
        GenericRow.genericRow("baz", 123, HEADER_DATA, A_ROWTIME, A_KEY)
    );
  }

  @Test
  public void shouldAddRowPartitionAndOffsetColumnsToNonWindowedStream() {
    // Given:
    givenUnwindowedSourceStream();
    final FixedKeyProcessor<GenericKey, GenericRow, GenericRow> processor =
        getProcessorFromStreamSource(streamSource);

    assertFixedKeyProcessorContextForwardsValue(
        processor,
        KEY,
        row,
        GenericRow.genericRow(
            "baz", 123, HEADER_DATA, A_ROWTIME, A_ROWPARTITION, A_ROWOFFSET, A_KEY)
    );
  }

  @Test
  public void shouldAddRowTimeAndRowKeyColumnsToLegacyNonWindowedTable() {
    // Given:
    givenUnwindowedSourceTableV1(true, LEGACY_PSEUDOCOLUMN_VERSION_NUMBER);
    final ValueTransformerWithKey<GenericKey, GenericRow, GenericRow> transformer =
        getTransformerFromTableSource(tableSourceV1);

    // When:
    final GenericRow withTimestamp = transformer.transform(KEY, row);

    // Then:
    assertThat(withTimestamp, equalTo(GenericRow.genericRow("baz", 123, HEADER_DATA, A_ROWTIME, A_KEY)));
  }

  @Test
  public void shouldHandleNullKeyLegacy() {
    // Given:
    givenUnwindowedSourceStream(LEGACY_PSEUDOCOLUMN_VERSION_NUMBER);
    final FixedKeyProcessor<GenericKey, GenericRow, GenericRow> processor =
        getProcessorFromStreamSource(streamSource);

    final GenericKey nullKey = null;
    assertFixedKeyProcessorContextForwardsValue(
        processor,
        nullKey,
        row,
        GenericRow.genericRow("baz", 123, HEADER_DATA, A_ROWTIME, null)
    );
  }

  @Test
  public void shouldHandleNullKey() {
    // Given:
    givenUnwindowedSourceStream();
    final FixedKeyProcessor<GenericKey, GenericRow, GenericRow> processor =
        getProcessorFromStreamSource(streamSource);

    assertFixedKeyProcessorContextForwardsValue(
        processor,
        null,
        row,
        GenericRow.genericRow(
            "baz", 123, HEADER_DATA, A_ROWTIME, A_ROWPARTITION, A_ROWOFFSET, null)
    );
  }

  @Test
  public void shouldHandleEmptyKeyLegacy() {
    // Given:
    givenUnwindowedSourceStream(LEGACY_PSEUDOCOLUMN_VERSION_NUMBER);
    final FixedKeyProcessor<GenericKey, GenericRow, GenericRow> processor =
        getProcessorFromStreamSource(streamSource);

    final GenericKey nullKey = GenericKey.genericKey((Object) null);

    assertFixedKeyProcessorContextForwardsValue(
        processor,
        nullKey,
        row,
        GenericRow.genericRow("baz", 123, HEADER_DATA, A_ROWTIME, null)
    );
  }

  @Test
  public void shouldHandleEmptyKey() {
    // Given:
    givenUnwindowedSourceStream();
    final FixedKeyProcessor<GenericKey, GenericRow, GenericRow> processor =
        getProcessorFromStreamSource(streamSource);

    final GenericKey nullKey = GenericKey.genericKey((Object) null);

    assertFixedKeyProcessorContextForwardsValue(
        processor,
        nullKey,
        row,
        GenericRow.genericRow(
            "baz", 123, HEADER_DATA, A_ROWTIME, A_ROWPARTITION, A_ROWOFFSET, null)
    );
  }

  @Test
  public void shouldHandleMultiKeyFieldLegacy() {
    // Given:
    givenMultiColumnSourceStream(LEGACY_PSEUDOCOLUMN_VERSION_NUMBER);
    final FixedKeyProcessor<GenericKey, GenericRow, GenericRow> processor =
        getProcessorFromStreamSource(streamSource);

    final GenericKey key = GenericKey.genericKey(1d, 2d);

    assertFixedKeyProcessorContextForwardsValue(
        processor,
        key,
        row,
        GenericRow.genericRow("baz", 123, HEADER_A, HEADER_B, null, A_ROWTIME, 1d, 2d)
    );
  }

  @Test
  public void shouldHandleMultiKeyField() {
    // Given:
    givenMultiColumnSourceStream();
    final FixedKeyProcessor<GenericKey, GenericRow, GenericRow> processor =
        getProcessorFromStreamSource(streamSource);

    final GenericKey key = GenericKey.genericKey(1d, 2d);

    assertFixedKeyProcessorContextForwardsValue(
        processor,
        key,
        row,
        GenericRow.genericRow(
            "baz", 123, HEADER_A, HEADER_B, null,
            A_ROWTIME, A_ROWPARTITION, A_ROWOFFSET, 1d, 2d)
    );
  }

  @Test
  public void shouldHandleMultiKeyFieldWithNullColLegacy() {
    // Given:
    givenMultiColumnSourceStream(LEGACY_PSEUDOCOLUMN_VERSION_NUMBER);
    final FixedKeyProcessor<GenericKey, GenericRow, GenericRow> processor =
        getProcessorFromStreamSource(streamSource);

    final GenericKey key = GenericKey.genericKey(null, 2d);

    assertFixedKeyProcessorContextForwardsValue(
        processor,
        key,
        row,
        GenericRow.genericRow("baz", 123, HEADER_A, HEADER_B, null, A_ROWTIME, null, 2d)
    );
  }

  @Test
  public void shouldHandleMultiKeyFieldWithNullCol() {
    // Given:
    givenMultiColumnSourceStream();
    final FixedKeyProcessor<GenericKey, GenericRow, GenericRow> processor =
        getProcessorFromStreamSource(streamSource);

    final GenericKey key = GenericKey.genericKey(null, 2d);

    assertFixedKeyProcessorContextForwardsValue(
        processor,
        key,
        row,
        GenericRow.genericRow(
            "baz", 123, HEADER_A, HEADER_B, null,
            A_ROWTIME, A_ROWPARTITION, A_ROWOFFSET, null, 2d)
    );
  }

  @Test
  public void shouldHandleMultiKeyFieldEmptyGenericKeyLegacy() {
    // Given:
    givenMultiColumnSourceStream(LEGACY_PSEUDOCOLUMN_VERSION_NUMBER);
    final FixedKeyProcessor<GenericKey, GenericRow, GenericRow> processor =
        getProcessorFromStreamSource(streamSource);

    final GenericKey key = GenericKey.genericKey(null, null);

    assertFixedKeyProcessorContextForwardsValue(
        processor,
        key,
        row,
        GenericRow.genericRow("baz", 123, HEADER_A, HEADER_B, null, A_ROWTIME, null, null)
    );
  }

  @Test
  public void shouldHandleMultiKeyFieldEmptyGenericKey() {
    // Given:
    givenMultiColumnSourceStream();
    final FixedKeyProcessor<GenericKey, GenericRow, GenericRow> processor =
        getProcessorFromStreamSource(streamSource);

    final GenericKey key = GenericKey.genericKey(null, null);

    assertFixedKeyProcessorContextForwardsValue(
        processor,
        key,
        row,
        GenericRow.genericRow(
            "baz", 123, HEADER_A, HEADER_B, null,
            A_ROWTIME, A_ROWPARTITION, A_ROWOFFSET, null, null)
    );
  }

  @Test
  public void shouldHandleMultiKeyFieldEntirelyNullLegacy() {
    // Given:
    givenMultiColumnSourceStream(LEGACY_PSEUDOCOLUMN_VERSION_NUMBER);
    final FixedKeyProcessor<GenericKey, GenericRow, GenericRow> transformer =
        getProcessorFromStreamSource(streamSource);

    final GenericKey key = null;

    assertFixedKeyProcessorContextForwardsValue(
        transformer,
        key,
        row,
        GenericRow.genericRow("baz", 123,
            HEADER_A, HEADER_B, null, A_ROWTIME, null, null));
  }

  @Test
  public void shouldHandleMultiKeyFieldEntirelyNull() {
    // Given:
    givenMultiColumnSourceStream();
    final FixedKeyProcessor<GenericKey, GenericRow, GenericRow> transformer =
        getProcessorFromStreamSource(streamSource);

    final GenericKey key = null;

    assertFixedKeyProcessorContextForwardsValue(
        transformer,
        key,
        row,
        GenericRow.genericRow(
            "baz", 123, HEADER_A, HEADER_B, null,
            A_ROWTIME, A_ROWPARTITION, A_ROWOFFSET, null, null));
  }

  @Test
  public void shouldAddRowTimeAndTimeWindowedRowKeyColumnsToLegacyStream() {
    // Given:
    givenWindowedSourceStream(LEGACY_PSEUDOCOLUMN_VERSION_NUMBER);
    final FixedKeyProcessor<Windowed<GenericKey>, GenericRow, GenericRow> processor =
        getProcessorFromStreamSource(windowedStreamSource);

    final Windowed<GenericKey> key = new Windowed<>(
        KEY,
        new TimeWindow(A_WINDOW_START, A_WINDOW_END)
    );

    assertFixedKeyProcessorContextForwardsValue(
        processor,
        key,
        row,
        GenericRow.genericRow("baz", 123, HEADER_DATA, A_ROWTIME, A_KEY, A_WINDOW_START, A_WINDOW_END)
    );
  }

  @Test
  public void shouldAddPseudoColumnsAndTimeWindowedRowKeyColumnsToStream() {
    // Given:
    givenWindowedSourceStream();
    final FixedKeyProcessor<Windowed<GenericKey>, GenericRow, GenericRow> processor =
        getProcessorFromStreamSource(windowedStreamSource);

    final Windowed<GenericKey> key = new Windowed<>(
        KEY,
        new TimeWindow(A_WINDOW_START, A_WINDOW_END)
    );

    assertFixedKeyProcessorContextForwardsValue(
        processor,
        key,
        row,
        GenericRow.genericRow(
            "baz", 123, HEADER_DATA, A_ROWTIME, A_ROWPARTITION,
            A_ROWOFFSET, A_KEY, A_WINDOW_START, A_WINDOW_END)
    );
  }

  @Test
  public void shouldAddRowTimeAndTimeWindowedRowKeyColumnsToLegacyTable() {
    // Given:
    givenWindowedSourceTable(LEGACY_PSEUDOCOLUMN_VERSION_NUMBER);
    final ValueTransformerWithKey<Windowed<GenericKey>, GenericRow, GenericRow> transformer =
        getTransformerFromTableSource(windowedTableSource);

    final Windowed<GenericKey> key = new Windowed<>(
        KEY,
        new TimeWindow(A_WINDOW_START, A_WINDOW_END)
    );

    // When:
    final GenericRow withTimestamp = transformer.transform(key, row);

    // Then:
    assertThat(withTimestamp,
        is(GenericRow.genericRow("baz", 123, HEADER_DATA, A_ROWTIME, A_KEY, A_WINDOW_START, A_WINDOW_END)));
  }

  @Test
  public void shouldAddRowTimeAndSessionWindowedRowKeyColumnsToStreamLegacy() {
    // Given:
    givenWindowedSourceStream(LEGACY_PSEUDOCOLUMN_VERSION_NUMBER);
    final FixedKeyProcessor<Windowed<GenericKey>, GenericRow, GenericRow> processor =
        getProcessorFromStreamSource(windowedStreamSource);

    final Windowed<GenericKey> key = new Windowed<>(
        KEY,
        new SessionWindow(A_WINDOW_START, A_WINDOW_END)
    );

    assertFixedKeyProcessorContextForwardsValue(
        processor,
        key,
        row,
        GenericRow.genericRow("baz", 123, HEADER_DATA, A_ROWTIME,
            A_KEY, A_WINDOW_START, A_WINDOW_END)
    );
  }

  @Test
  public void shouldAddPseudoColumnsAndSessionWindowedRowKeyColumnsToStream() {
    // Given:
    givenWindowedSourceStream();
    final FixedKeyProcessor<Windowed<GenericKey>, GenericRow, GenericRow> processor =
        getProcessorFromStreamSource(windowedStreamSource);

    final Windowed<GenericKey> key = new Windowed<>(
        KEY,
        new SessionWindow(A_WINDOW_START, A_WINDOW_END)
    );

    assertFixedKeyProcessorContextForwardsValue(
        processor,
        key,
        row,
        GenericRow.genericRow(
            "baz",
            123,
            HEADER_DATA,
            A_ROWTIME,
            A_ROWPARTITION,
            A_ROWOFFSET,
            A_KEY,
            A_WINDOW_START,
            A_WINDOW_END)
    );
  }

  @Test
  public void shouldAddRowTimeAndSessionWindowedRowKeyColumnsToTableLegacy() {
    // Given:
    givenWindowedSourceTable(LEGACY_PSEUDOCOLUMN_VERSION_NUMBER);
    final ValueTransformerWithKey<Windowed<GenericKey>, GenericRow, GenericRow> transformer =
        getTransformerFromTableSource(windowedTableSource);

    final Windowed<GenericKey> key = new Windowed<>(
        KEY,
        new SessionWindow(A_WINDOW_START, A_WINDOW_END)
    );

    // When:
    final GenericRow withTimestamp = transformer.transform(key, row);

    // Then:
    assertThat(withTimestamp,
        equalTo(GenericRow.genericRow("baz", 123, HEADER_DATA, A_ROWTIME, A_KEY, A_WINDOW_START, A_WINDOW_END)));
  }

  @Test
  public void shouldUseCorrectSerdeForWindowedKey() {
    // Given:
    givenWindowedSourceStream();

    // When:
    windowedStreamSource.build(planBuilder, planInfo);

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
    givenUnwindowedSourceStream();

    // When:
    streamSource.build(planBuilder, planInfo);

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
    givenUnwindowedSourceStream();

    // When:
    final KStreamHolder<?> stream = streamSource.build(planBuilder, planInfo);

    // Then:
    reset(buildContext);
    stream.getExecutionKeyFactory().buildKeySerde(keyFormatInfo, PHYSICAL_SCHEMA, ctx);
    verify(buildContext).buildKeySerde(keyFormatInfo, PHYSICAL_SCHEMA, ctx);
  }

  @Test
  public void shouldReturnCorrectSerdeFactoryForWindowedSource() {
    // Given:
    givenWindowedSourceStream();

    // When:
    final KStreamHolder<?> stream = windowedStreamSource.build(planBuilder, planInfo);

    // Then:
    reset(buildContext);
    stream.getExecutionKeyFactory().buildKeySerde(keyFormatInfo, PHYSICAL_SCHEMA, ctx);
    verify(buildContext).buildKeySerde(keyFormatInfo, windowInfo, PHYSICAL_SCHEMA, ctx);
  }

  @Test
  public void shouldBuildTableWithCorrectStoreName() {
    // Given:
    givenUnwindowedSourceTableV1(true, LEGACY_PSEUDOCOLUMN_VERSION_NUMBER);

    // When:
    tableSourceV1.build(planBuilder, planInfo);

    // Then:
    verify(materializationFactory).create(keySerde, valueSerde, "base-Reduce");
  }

  @SuppressWarnings("unchecked")
  private <K> FixedKeyProcessor<K, GenericRow, GenericRow> getProcessorFromStreamSource(
      final SourceStep<?> streamSource
  ) {
    streamSource.build(planBuilder, planInfo);
    verify(kStream).processValues(fixedKeyProcessorSupplierArgumentCaptor.capture());
    final FixedKeyProcessor<K, GenericRow, GenericRow> processor =
        (FixedKeyProcessor<K, GenericRow, GenericRow>) fixedKeyProcessorSupplierArgumentCaptor.getValue().get();
    processor.init(fixedKeyProcessorContext);
    return processor;
  }

  @SuppressWarnings("unchecked")
  private <K> ValueTransformerWithKey<K, GenericRow, GenericRow> getTransformerFromTableSource(
      final SourceStep<?> streamSource
  ) {
    streamSource.build(planBuilder, planInfo);
    verify(kTable).transformValues(transformSupplierCaptor.capture());
    final ValueTransformerWithKey<K, GenericRow, GenericRow> transformer =
        (ValueTransformerWithKey<K, GenericRow, GenericRow>) transformSupplierCaptor.getValue().get();
    transformer.init(processorCtx);
    return transformer;
  }

  private void givenWindowedSourceStream(final int pseudoColumnVersion) {
    when(buildContext.buildKeySerde(any(), any(), any(), any())).thenReturn(windowedKeySerde);
    givenConsumed(consumedWindowed, windowedKeySerde);
    windowedStreamSource = new WindowedStreamSource(
        new ExecutionStepPropertiesV1(ctx),
        TOPIC_NAME,
        Formats.of(keyFormatInfo, valueFormatInfo, KEY_FEATURES, VALUE_FEATURES),
        windowInfo,
        TIMESTAMP_COLUMN,
        SOURCE_SCHEMA,
        OptionalInt.of(pseudoColumnVersion)
    );
  }

  private void givenWindowedSourceStream() {
    givenWindowedSourceStream(ROWPARTITION_ROWOFFSET_PSEUDOCOLUMN_VERSION);
  }

  private void givenUnwindowedSourceStream(final int pseudoColumnVersion) {
    when(buildContext.buildKeySerde(any(), any(), any())).thenReturn(keySerde);
    givenConsumed(consumed, keySerde);
    streamSource = new StreamSource(
        new ExecutionStepPropertiesV1(ctx),
        TOPIC_NAME,
        Formats.of(keyFormatInfo, valueFormatInfo, KEY_FEATURES, VALUE_FEATURES),
        TIMESTAMP_COLUMN,
        SOURCE_SCHEMA,
        OptionalInt.of(pseudoColumnVersion)
    );
  }

  private void givenUnwindowedSourceStream() {
    givenUnwindowedSourceStream(ROWPARTITION_ROWOFFSET_PSEUDOCOLUMN_VERSION);
  }

  private void givenMultiColumnSourceStream(final int pseudoColumnVersion) {
    when(buildContext.buildKeySerde(any(), any(), any())).thenReturn(keySerde);
    givenConsumed(consumed, keySerde);
    streamSource = new StreamSource(
        new ExecutionStepPropertiesV1(ctx),
        TOPIC_NAME,
        Formats.of(keyFormatInfo, valueFormatInfo, KEY_FEATURES, VALUE_FEATURES),
        TIMESTAMP_COLUMN,
        MULTI_COL_SOURCE_SCHEMA,
        OptionalInt.of(pseudoColumnVersion)
    );
  }

  private void givenMultiColumnSourceStream() {
    givenMultiColumnSourceStream(ROWPARTITION_ROWOFFSET_PSEUDOCOLUMN_VERSION);
  }

  private void givenWindowedSourceTable(final int pseudoColumnVersion) {
    when(buildContext.buildKeySerde(any(), any(), any(), any())).thenReturn(windowedKeySerde);
    givenConsumed(consumedWindowed, windowedKeySerde);
    givenConsumed(consumedWindowed, windowedKeySerde);
    windowedTableSource = new WindowedTableSource(
        new ExecutionStepPropertiesV1(ctx),
        TOPIC_NAME,
        Formats.of(keyFormatInfo, valueFormatInfo, KEY_FEATURES, VALUE_FEATURES),
        windowInfo,
        TIMESTAMP_COLUMN,
        SOURCE_SCHEMA,
        OptionalInt.of(pseudoColumnVersion)
    );
  }

  private void givenWindowedSourceTable() {
    givenWindowedSourceTable(ROWPARTITION_ROWOFFSET_PSEUDOCOLUMN_VERSION);
  }

  private void givenUnwindowedSourceTableV1(
      final Boolean forceChangelog, final int pseudoColumnVersion) {
    when(buildContext.buildKeySerde(any(), any(), any())).thenReturn(keySerde);
    givenConsumed(consumed, keySerde);
    tableSourceV1 = new TableSourceV1(
        new ExecutionStepPropertiesV1(ctx),
        TOPIC_NAME,
        Formats.of(keyFormatInfo, valueFormatInfo, KEY_FEATURES, VALUE_FEATURES),
        TIMESTAMP_COLUMN,
        SOURCE_SCHEMA,
        Optional.of(forceChangelog),
        OptionalInt.of(pseudoColumnVersion)
    );
  }

  private <K> void givenConsumed(final Consumed<K, GenericRow> consumed, final Serde<K> keySerde) {
    when(consumedFactory.create(keySerde, valueSerde)).thenReturn(consumed);
    when(consumed.withTimestampExtractor(any())).thenReturn(consumed);
    when(consumed.withOffsetResetPolicy(any())).thenReturn(consumed);
  }

  private static PlanInfo givenDownstreamRepartition(final ExecutionStep<?> sourceStep) {
    final PlanInfo mockPlanInfo = mock(PlanInfo.class);
    when(mockPlanInfo.isRepartitionedInPlan(sourceStep)).thenReturn(true);
    return mockPlanInfo;
  }

  private FixedKeyRecord getMockFixedRecord(final Object key,
      final GenericRow value) {
    final FixedKeyRecord record = mock(FixedKeyRecord.class);
    when(record.value())
        .thenReturn(value);
    when(record.key()).thenReturn(key);
    when(record.timestamp()).thenReturn(A_ROWTIME);
    when(record.headers()).thenReturn(HEADERS);
    // mock withValue to return new mock with new value
    when(record.withValue(any())).thenAnswer(inv -> {
      final GenericRow row = inv.getArgument(0);
      final FixedKeyRecord newRecord = mock(FixedKeyRecord.class);
      when(newRecord.value()).thenReturn(row);
      return newRecord;
    });
    return record;
  }

  private void assertFixedKeyProcessorContextForwardsValue(
      final FixedKeyProcessor processor,
      final Object key,
      final GenericRow value,
      final GenericRow expectedRow
  ) {
    when(fixedKeyProcessorContext.recordMetadata()).thenReturn(
        Optional.of(new MockRecordMetadata(TOPIC_NAME, A_ROWPARTITION, A_ROWOFFSET))
    );

    // When:
    processor.process(getMockFixedRecord(key, value));

    // Then:
    verify(fixedKeyProcessorContext).forward(argThat(
        r -> {
          assertThat(r.value(), instanceOf(GenericRow.class));
          final GenericRow genericRow = (GenericRow) r.value();
          assertThat(genericRow, equalTo(expectedRow));
          return true;
        })
    );
  }

  private class MockRecordMetadata implements RecordMetadata {

    private final String topic;
    private final int partition;
    private final long offset;

    MockRecordMetadata(final String topic, final int partition, final long offset) {
      this.topic = topic;
      this.partition = partition;
      this.offset = offset;
    }

    @Override
    public String topic() {
      return topic;
    }

    @Override
    public int partition() {
      return partition;
    }

    @Override
    public long offset() {
      return offset;
    }
  }
}
