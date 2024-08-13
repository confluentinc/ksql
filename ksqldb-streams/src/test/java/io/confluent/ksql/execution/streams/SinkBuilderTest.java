/*
 * Copyright 2020 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.execution.streams;

import static org.junit.Assert.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.runtime.RuntimeBuildContext;
import io.confluent.ksql.execution.context.QueryContext;
import io.confluent.ksql.execution.plan.Formats;
import io.confluent.ksql.execution.plan.ExecutionKeyFactory;
import io.confluent.ksql.execution.streams.timestamp.KsqlTimestampExtractor;
import io.confluent.ksql.execution.timestamp.TimestampColumn;
import io.confluent.ksql.logging.processing.ProcessingLogger;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.PhysicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.serde.FormatFactory;
import io.confluent.ksql.serde.FormatInfo;
import io.confluent.ksql.serde.SerdeFeatures;
import io.confluent.ksql.util.KsqlException;
import java.util.Optional;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.To;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class SinkBuilderTest {

  private static final String TOPIC = "TOPIC";
  private static final String QUERY_CONTEXT_NAME = "TIMESTAMP-TRANSFORM";

  private static final LogicalSchema SCHEMA = LogicalSchema.builder()
      .valueColumn(ColumnName.of("BLUE"), SqlTypes.BIGINT)
      .valueColumn(ColumnName.of("GREEN"), SqlTypes.STRING)
      .build();

  private static final FormatInfo KEY_FORMAT = FormatInfo.of(FormatFactory.KAFKA.name());
  private static final FormatInfo VALUE_FORMAT = FormatInfo.of(FormatFactory.JSON.name());
  private static final PhysicalSchema PHYSICAL_SCHEMA = PhysicalSchema
      .from(SCHEMA.withoutPseudoAndKeyColsInValue(), SerdeFeatures.of(), SerdeFeatures.of());

  @Mock
  private RuntimeBuildContext buildContext;
  @Mock
  private ExecutionKeyFactory<Struct> executionKeyFactory;
  @Mock
  private KStream<Struct, GenericRow> kStream;
  @Mock
  private Serde<Struct> keySerde;
  @Mock
  private Serde<GenericRow> valSerde;
  @Mock
  private QueryContext queryContext;
  @Mock
  private Struct key;
  @Mock
  private GenericRow row;
  @Mock
  private ProcessorContext processorContext;
  @Mock
  private ProcessingLogger processingLogger;
  @Captor
  private ArgumentCaptor<To> toCaptor;

  @Before
  public void setup() {
    when(executionKeyFactory.buildKeySerde(any(), any(), any())).thenReturn(keySerde);

    when(buildContext.buildValueSerde(any(), any(), any())).thenReturn(valSerde);
    when(buildContext.getProcessingLogger(any())).thenReturn(processingLogger);
    when(queryContext.getContext()).thenReturn(ImmutableList.of(QUERY_CONTEXT_NAME));
  }

  @Test
  public void shouldBuildKeySerdeCorrectly() {
    // When
    buildDefaultSinkBuilder();

    // Then:
    verify(executionKeyFactory).buildKeySerde(KEY_FORMAT, PHYSICAL_SCHEMA, queryContext);
  }

  @Test
  public void shouldBuildValueSerdeCorrectly() {
    // When
    buildDefaultSinkBuilder();

    // Then:
    verify(buildContext).buildValueSerde(
        VALUE_FORMAT,
        PHYSICAL_SCHEMA,
        queryContext
    );
  }

  @Test
  public void shouldWriteOutStreamWithCorrectSerdes() {
    // When
    buildDefaultSinkBuilder();

    // Then
    verify(kStream).to(anyString(), eq(Produced.with(keySerde, valSerde)));
  }

  @Test
  public void shouldWriteOutStreamToCorrectTopic() {
    // When
    buildDefaultSinkBuilder();

    // Then
    verify(kStream).to(eq(TOPIC), any());
  }

  @Test
  public void shouldBuildStreamUsingTransformTimestampWhenTimestampIsSpecified() {
    // When
    SinkBuilder.build(
        SCHEMA,
        Formats.of(KEY_FORMAT, VALUE_FORMAT, SerdeFeatures.of(), SerdeFeatures.of()),
        Optional.of(new TimestampColumn(ColumnName.of("BLUE"), Optional.empty())),
        TOPIC,
        kStream,
        executionKeyFactory,
        queryContext,
        buildContext
    );

    // Then
    final InOrder inOrder = Mockito.inOrder(kStream);
    inOrder.verify(kStream).transform(any(), any(Named.class));
    inOrder.verify(kStream).to(eq(TOPIC), any());
    inOrder.verifyNoMoreInteractions();
  }

  @Test
  public void shouldBuildStreamWithoutTransformTimestampWhenNoTimestampIsSpecified() {
    // When
    buildDefaultSinkBuilder();

    // Then
    final InOrder inOrder = Mockito.inOrder(kStream);
    inOrder.verify(kStream).to(anyString(), any());
    inOrder.verifyNoMoreInteractions();
  }

  @Test
  public void shouldTransformTimestampRow() {
    // Given
    final long timestampColumnValue = 10001;
    final KsqlTimestampExtractor timestampExtractor = mock(KsqlTimestampExtractor.class);
    when(timestampExtractor.extract(any(), any())).thenReturn(timestampColumnValue);

    // When
    final Transformer<Struct, GenericRow, KeyValue<Struct, GenericRow>> transformer =
        getTransformer(timestampExtractor, processingLogger);
    transformer.init(processorContext);
    final KeyValue<Struct, GenericRow> kv = transformer.transform(key, row);

    // Then
    assertNull(kv);
    verify(timestampExtractor).extract(key, row);
    verify(processorContext, Mockito.times(1))
        .forward(eq(key), eq(row), eq(To.all().withTimestamp(timestampColumnValue)));
    verifyNoMoreInteractions(processingLogger);
  }

  @Test
  public void shouldTransformTombstone() {
    // Given
    final long streamTime = 111111;
    final KsqlTimestampExtractor timestampExtractor = mock(KsqlTimestampExtractor.class);
    when(processorContext.currentStreamTimeMs()).thenReturn(streamTime);

    // When
    final Transformer<Struct, GenericRow, KeyValue<Struct, GenericRow>> transformer =
        getTransformer(timestampExtractor, processingLogger);
    transformer.init(processorContext);
    final KeyValue<Struct, GenericRow> kv = transformer.transform(key, null);

    // Then
    assertNull(kv);
    verify(timestampExtractor, never()).extract(key, null);
    verify(processorContext).forward(eq(key), isNull(), eq(To.all().withTimestamp(streamTime)));
    verifyNoMoreInteractions(processingLogger);
  }

  @Test
  public void shouldCallProcessingLoggerOnForwardError() {
    // Given
    final long timestampColumnValue = 10001;
    final KsqlTimestampExtractor timestampExtractor
        = mock(KsqlTimestampExtractor.class);
    when(timestampExtractor.extract(any(), any())).thenReturn(timestampColumnValue);
    doThrow(KsqlException.class)
        .when(processorContext)
        .forward(any(), any(), any(To.class));

    // When
    final Transformer<Struct, GenericRow, KeyValue<Struct, GenericRow>> transformer =
        getTransformer(timestampExtractor, processingLogger);
    transformer.init(processorContext);
    final KeyValue<Struct, GenericRow> kv = transformer.transform(key, row);

    // Then
    assertNull(kv);
    verify(timestampExtractor).extract(key, row);
    verify(processorContext, Mockito.times(1))
        .forward(eq(key), eq(row), toCaptor.capture());
    verify(processingLogger, Mockito.times(1)).error(any());
  }

  @Test
  public void shouldCallProcessingLoggerOnTimestampExtractorError() {
    // Given
    final KsqlTimestampExtractor timestampExtractor
        = mock(KsqlTimestampExtractor.class);
    doThrow(KsqlException.class).when(timestampExtractor).extract(key, row);

    // When
    final Transformer<Struct, GenericRow, KeyValue<Struct, GenericRow>> transformer =
        getTransformer(timestampExtractor, processingLogger);
    transformer.init(processorContext);
    final KeyValue<Struct, GenericRow> kv = transformer.transform(key, row);

    // Then
    assertNull(kv);
    verify(timestampExtractor).extract(key, row);
    verify(processingLogger, Mockito.times(1)).error(any());
    verifyNoMoreInteractions(processorContext);
  }

  @Test(expected = NullPointerException.class)
  public void shouldThrowOnNullProcessorContext() {
    // Given
    final KsqlTimestampExtractor timestampExtractor
        = mock(KsqlTimestampExtractor.class);

    // When/Then
    getTransformer(timestampExtractor, processingLogger).init(null);
  }

  @Test(expected = NullPointerException.class)
  public void shouldThrowOnNullTimestampExtractor() {
    // When/Then
    getTransformer(null, null);
  }

  @Test(expected = NullPointerException.class)
  public void shouldThrowOnNullProcessingLogger() {
    // Given
    final KsqlTimestampExtractor timestampExtractor
        = mock(KsqlTimestampExtractor.class);

    // When/Then
    getTransformer(timestampExtractor, null);
  }

  private void buildDefaultSinkBuilder() {
    SinkBuilder.build(
        SCHEMA,
        Formats.of(KEY_FORMAT, VALUE_FORMAT, SerdeFeatures.of(), SerdeFeatures.of()),
        Optional.empty(),
        TOPIC,
        kStream,
        executionKeyFactory,
        queryContext,
        buildContext
    );
  }

  private static Transformer<Struct, GenericRow, KeyValue<Struct, GenericRow>> getTransformer(
      final KsqlTimestampExtractor timestampExtractor,
      final ProcessingLogger processingLogger
  ) {
    return new SinkBuilder.TransformTimestamp<Struct>(timestampExtractor, processingLogger).get();
  }
}
