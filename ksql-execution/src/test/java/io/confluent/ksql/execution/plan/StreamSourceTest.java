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

package io.confluent.ksql.execution.plan;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.builder.KsqlQueryBuilder;
import io.confluent.ksql.execution.context.QueryContext;
import io.confluent.ksql.execution.plan.StreamSource.Functions;
import io.confluent.ksql.execution.plan.StreamSource.KeySerdeSupplier;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.PhysicalSchema;
import io.confluent.ksql.serde.FormatInfo;
import io.confluent.ksql.serde.KeyFormat;
import io.confluent.ksql.serde.KeySerde;
import io.confluent.ksql.serde.SerdeOption;
import io.confluent.ksql.serde.ValueFormat;
import io.confluent.ksql.serde.WindowInfo;
import io.confluent.ksql.util.timestamp.TimestampExtractionPolicy;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology.AutoOffsetReset;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KsqlConsumed;
import org.apache.kafka.streams.kstream.ValueMapperWithKey;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.kstream.ValueTransformerSupplier;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.TimeWindow;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.TimestampExtractor;
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


public class StreamSourceTest {
  private static final LogicalSchema SOURCE_SCHEMA = LogicalSchema.of(SchemaBuilder.struct()
      .field("field1", Schema.OPTIONAL_STRING_SCHEMA)
      .field("field2", Schema.OPTIONAL_INT64_SCHEMA)
      .build());
  private static final Schema KEY_SCHEMA = SchemaBuilder.struct()
      .field("k1", Schema.OPTIONAL_STRING_SCHEMA)
      .build();
  private static final Struct KEY = new Struct(KEY_SCHEMA).put("k1", "foo");
  private static final LogicalSchema SCHEMA
      = LogicalSchemaWithMetaAndKeyFields.fromOriginal("alias", SOURCE_SCHEMA).getSchema();

  private static final String TOPIC_NAME = "topic";
  private static final int TIMESTAMP_IDX = 1;

  @Mock
  private QueryContext ctx;
  @Mock
  private KsqlQueryBuilder queryBuilder;
  @Mock
  private StreamsBuilder streamsBuilder;
  @Mock
  private KStream kStream;
  @Mock
  private ExecutionStepProperties properties;
  @Mock
  private FormatInfo keyFormatInfo;
  @Mock
  private WindowInfo windowInfo;
  @Mock
  private KeyFormat keyFormat;
  @Mock
  private ValueFormat valueFormat;
  @Mock
  private FormatInfo valueFormatInfo;
  private final Set<SerdeOption> serdeOptions = new HashSet<>();
  @Mock
  private Serde<GenericRow> valueSerde;
  @Mock
  private KeySerde keySerde;
  @Mock
  private Optional<AutoOffsetReset> offsetReset;
  @Mock
  private TimestampExtractionPolicy extractionPolicy;
  @Mock
  private TimestampExtractor extractor;
  @Mock
  private KeySerdeSupplier keySerdeSupplier;
  @Mock
  private ValueMapperWithKey<?, GenericRow, GenericRow> metaFieldMapper;
  @Mock
  private ValueTransformerSupplier<GenericRow, GenericRow> timestampTransformer;
  @Mock
  private Consumed consumed;
  @Mock
  private BiFunction<Serde<?>, Serde<GenericRow>, Consumed> cFactory;
  @Mock
  private PhysicalSchema physicalSchema;
  @Mock
  private BiFunction<LogicalSchema, Set<SerdeOption>, PhysicalSchema> physicalSchemaFactory;
  @Mock
  private ProcessorContext processorCtx;
  @Captor
  private ArgumentCaptor<ValueMapperWithKey> mapperCaptor;
  @Captor
  private ArgumentCaptor<ValueTransformerSupplier> transformSupplierCaptor;
  @Captor
  private ArgumentCaptor<Consumed> consumedCaptor;

  private final GenericRow row = new GenericRow(new LinkedList<>(ImmutableList.of("baz", 123)));

  private StreamSource<Struct> streamSource;

  @Rule
  public final MockitoRule mockitoRule = MockitoJUnit.rule();

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Before
  @SuppressWarnings("unchecked")
  public void setup() {
    when(properties.getQueryContext()).thenReturn(ctx);
    when(cFactory.apply(any(), any())).thenReturn(consumed);
    when(consumed.withOffsetResetPolicy(any())).thenReturn(consumed);
    when(consumed.withTimestampExtractor(any())).thenReturn(consumed);
    when(extractionPolicy.create(anyInt())).thenReturn(extractor);
    when(extractionPolicy.timestampField()).thenReturn("field2");
    when(queryBuilder.getStreamsBuilder()).thenReturn(streamsBuilder);
    when(streamsBuilder.stream(anyString(), any(Consumed.class))).thenReturn(kStream);
    when(kStream.mapValues(any(ValueMapperWithKey.class))).thenReturn(kStream);
    when(kStream.transformValues(any(ValueTransformerSupplier.class))).thenReturn(kStream);
    when(offsetReset.isPresent()).thenReturn(true);
    when(offsetReset.get()).thenReturn(AutoOffsetReset.EARLIEST);
    when(queryBuilder.buildValueSerde(any(), any(), any())).thenReturn(valueSerde);
    when(keySerdeSupplier.buildKeySerde(any(), any(), any(), any())).thenReturn(keySerde);
    when(valueFormat.getFormatInfo()).thenReturn(valueFormatInfo);
    when(physicalSchemaFactory.apply(any(), any())).thenReturn(physicalSchema);
    when(processorCtx.timestamp()).thenReturn(456L);
    when(keyFormat.getFormatInfo()).thenReturn(keyFormatInfo);
    when(keyFormat.getWindowInfo()).thenReturn(Optional.of(windowInfo));

    streamSource = new StreamSource(
        properties,
        TOPIC_NAME,
        Formats.of(keyFormat, valueFormat, serdeOptions),
        extractionPolicy,
        TIMESTAMP_IDX,
        offsetReset,
        SOURCE_SCHEMA,
        new Functions(
            keySerdeSupplier,
            metaFieldMapper,
            timestampTransformer,
            cFactory,
            physicalSchemaFactory
        )
    );
  }

  @Test
  @SuppressWarnings("unchecked")
  public void shouldApplyCorrectTransformationsToSourceStream() {
    // When:
    final KStream<Struct, GenericRow> builtKstream = streamSource.build(queryBuilder);

    // Then:
    assertThat(builtKstream, is(kStream));
    final InOrder validator = inOrder(streamsBuilder, kStream);
    validator.verify(streamsBuilder).stream(TOPIC_NAME, consumed);
    validator.verify(kStream).mapValues(metaFieldMapper);
    validator.verify(kStream).transformValues(timestampTransformer);
  }

  @Test
  public void shouldAddTimestampExtractor() {
    // When:
    streamSource.buildWithSerde(queryBuilder);

    // Then:
    verify(extractionPolicy).create(TIMESTAMP_IDX);
    verify(consumed).withTimestampExtractor(extractor);
  }

  @Test
  public void shouldBuildWithOffsetReset() {
    // When:
    streamSource.buildWithSerde(queryBuilder);

    verify(consumed).withOffsetResetPolicy(AutoOffsetReset.EARLIEST);
  }

  @Test
  public void shouldNotBuildWithOffsetResetIfNotProvided() {
    // Given:
    when(offsetReset.isPresent()).thenReturn(false);

    // When
    streamSource.buildWithSerde(queryBuilder);

    verify(consumed, times(0)).withOffsetResetPolicy(any());
  }

  @Test
  public void shouldBuildPhysicalSchemaFromSourceSchema() {
    // When:
    streamSource.buildWithSerde(queryBuilder);

    // Then:
    verify(physicalSchemaFactory).apply(SOURCE_SCHEMA, serdeOptions);
  }

  @Test
  public void shouldBuildSourceValueSerdeCorrectly() {
    // When:
    streamSource.buildWithSerde(queryBuilder);

    // Then:
    verify(queryBuilder).buildValueSerde(valueFormatInfo, physicalSchema, ctx);
    verify(cFactory).apply(any(), same(valueSerde));
  }

  @Test
  public void shouldBuildSourceKeySerdeCorrectly() {
    // When:
    streamSource.buildWithSerde(queryBuilder);

    // Then:
    verify(keySerdeSupplier).buildKeySerde(keyFormat, physicalSchema, ctx, queryBuilder);
    verify(cFactory).apply(same(keySerde), any());
  }

  @SuppressWarnings("unchecked")
  private ValueMapperWithKey<?, GenericRow, GenericRow> getMapperFromStreamSource(
      final StreamSource<?> streamSource) {
    streamSource.build(queryBuilder);
    verify(kStream).mapValues(mapperCaptor.capture());
    return mapperCaptor.getValue();
  }

  @Test
  @SuppressWarnings("unchecked")
  public void shouldAddWindowedKeyWithCorrectFormat() {
    // Given:
    when(keyFormat.isWindowed()).thenReturn(true);
    final Windowed<Struct> key = new Windowed<>(KEY, new TimeWindow(100, 200));
    final StreamSource<?> streamSource = StreamSource.createWindowed(
        ctx,
        LogicalSchemaWithMetaAndKeyFields.fromTransformed(SCHEMA),
        TOPIC_NAME,
        Formats.of(keyFormat, valueFormat, serdeOptions),
        extractionPolicy,
        -1,
        offsetReset
    );
    final ValueMapperWithKey mapper = getMapperFromStreamSource(streamSource);

    // When:
    final GenericRow result = (GenericRow) mapper.apply(key, row);

    // Then:
    assertThat(
        result,
        equalTo(new GenericRow("foo : Window{start=100 end=-}", "baz", 123)));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void shouldAddNonWindowedKey() {
    // Given:
    final StreamSource streamSource = StreamSource.createNonWindowed(
        ctx,
        LogicalSchemaWithMetaAndKeyFields.fromTransformed(SCHEMA),
        TOPIC_NAME,
        Formats.of(keyFormat, valueFormat, serdeOptions),
        extractionPolicy,
        -1,
        offsetReset
    );
    final ValueMapperWithKey mapper = getMapperFromStreamSource(streamSource);

    // When:
    final GenericRow result = (GenericRow) mapper.apply(KEY, row);

    // Then:
    assertThat(result, equalTo(new GenericRow("foo", "baz", 123)));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void shouldThrowOnMultiFieldKey() {
    // Then:
    expectedException.expect(instanceOf(IllegalStateException.class));

    // When:
    StreamSource.createNonWindowed(
        ctx,
        LogicalSchemaWithMetaAndKeyFields.fromOriginal(
            "alias",
            LogicalSchema.of(
                SchemaBuilder.struct()
                    .field("f1", Schema.INT32_SCHEMA)
                    .field( "f2", Schema.INT64_SCHEMA)
                    .build(),
            SCHEMA.valueSchema())
        ),
        TOPIC_NAME,
        Formats.of(keyFormat, valueFormat, serdeOptions),
        extractionPolicy,
        -1,
        offsetReset
    );
  }

  @SuppressWarnings("unchecked")
  private ValueTransformer getTransformerFromStreamSource(final StreamSource<?> streamSource) {
    streamSource.build(queryBuilder);
    verify(kStream).transformValues(transformSupplierCaptor.capture());
    return transformSupplierCaptor.getValue().get();
  }

  @Test
  @SuppressWarnings("unchecked")
  public void shouldAddTimestampColumn() {
    // Given:
    final StreamSource streamSource = StreamSource.createNonWindowed(
        ctx,
        LogicalSchemaWithMetaAndKeyFields.fromTransformed(SCHEMA),
        TOPIC_NAME,
        Formats.of(keyFormat, valueFormat, serdeOptions),
        extractionPolicy,
        -1,
        offsetReset
    );
    final ValueTransformer transformer = getTransformerFromStreamSource(streamSource);
    transformer.init(processorCtx);

    // When:
    final GenericRow withTimestamp = (GenericRow) transformer.transform(row);

    // Then:
    assertThat(withTimestamp, equalTo(new GenericRow(456L, "baz", 123)));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void shouldUseCorrectSerdeForWindowedKey() {
    // Given:
    when(keyFormat.isWindowed()).thenReturn(true);
    final StreamSource streamSource = StreamSource.createWindowed(
        ctx,
        LogicalSchemaWithMetaAndKeyFields.fromTransformed(SCHEMA),
        TOPIC_NAME,
        Formats.of(keyFormat, valueFormat, serdeOptions),
        extractionPolicy,
        -1,
        offsetReset
    );

    // When:
    streamSource.build(queryBuilder);

    // Then:
    verify(queryBuilder).buildKeySerde(keyFormatInfo, windowInfo, PhysicalSchema.from(SOURCE_SCHEMA, serdeOptions), ctx);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void shouldUseCorrectSerdeForNonWindowedKey() {
    // Given:
    final StreamSource streamSource = StreamSource.createNonWindowed(
        ctx,
        LogicalSchemaWithMetaAndKeyFields.fromTransformed(SCHEMA),
        TOPIC_NAME,
        Formats.of(keyFormat, valueFormat, serdeOptions),
        extractionPolicy,
        -1,
        offsetReset
    );

    // When:
    streamSource.build(queryBuilder);

    // Then:
    verify(queryBuilder).buildKeySerde(keyFormatInfo, PhysicalSchema.from(SOURCE_SCHEMA, serdeOptions), ctx);
  }
}