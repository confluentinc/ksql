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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.context.QueryContext;
import io.confluent.ksql.execution.plan.ExecutionKeyFactory;
import io.confluent.ksql.execution.plan.Formats;
import io.confluent.ksql.execution.runtime.RuntimeBuildContext;
import io.confluent.ksql.execution.streams.SinkBuilder.TimestampProcessorSupplier;
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
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.To;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;
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
  private static final String QUERY_CONTEXT_NAME = "PROCESS-TIMESTAMP";

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
  private KStream<Struct, GenericRow> kstream;
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
  private ProcessorContext<Struct, GenericRow> processorContext;
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
    verify(kstream).to(anyString(), eq(Produced.with(keySerde, valSerde)));
  }

  @Test
  public void shouldWriteOutStreamToCorrectTopic() {
    // When
    buildDefaultSinkBuilder();

    // Then
    verify(kstream).to(eq(TOPIC), any());
  }

  @Test
  public void shouldBuildStreamUsingProcessTimestampWhenTimestampIsSpecified() {
    // When
    SinkBuilder.build(
        SCHEMA,
        Formats.of(KEY_FORMAT, VALUE_FORMAT, SerdeFeatures.of(), SerdeFeatures.of()),
        Optional.of(new TimestampColumn(ColumnName.of("BLUE"), Optional.empty())),
        TOPIC,
        kstream,
        executionKeyFactory,
        queryContext,
        buildContext
    );

    // Then
    final InOrder inOrder = Mockito.inOrder(kstream);
    inOrder.verify(kstream).process(
        Mockito.<ProcessorSupplier<Struct, GenericRow, Object, Object>>any(), any(Named.class));
    inOrder.verify(kstream).to(eq(TOPIC), any());
    inOrder.verifyNoMoreInteractions();
  }

  @Test
  public void shouldBuildStreamWithoutProcessTimestampWhenNoTimestampIsSpecified() {
    // When
    buildDefaultSinkBuilder();

    // Then
    final InOrder inOrder = Mockito.inOrder(kstream);
    inOrder.verify(kstream).to(anyString(), any());
    inOrder.verifyNoMoreInteractions();
  }

  @Test
  public void shouldProcessTimestampRow() {
    // Given
    final long timestampColumnValue = 10001;
    final KsqlTimestampExtractor timestampExtractor = mock(KsqlTimestampExtractor.class);
    when(timestampExtractor.extract(any(), any())).thenReturn(timestampColumnValue);

    // When
    final Processor<Struct, GenericRow, Struct, GenericRow> processor =
        getProcessor(timestampExtractor, processingLogger);
    processor.init(processorContext);
    final Record<Struct, GenericRow> inputRecord
        = new Record<>(key, row, 0);
    processor.process(inputRecord);

    // Then
    final Record<Struct, GenericRow> outputRecord = new Record<>(key, row, timestampColumnValue);
    verify(timestampExtractor).extract(key, row);
    verify(processorContext, Mockito.times(1))
        .forward(eq(outputRecord));
    verifyNoMoreInteractions(processingLogger);
  }

  @Test
  public void shouldProcessTombstone() {
    // Given
    final long streamTime = 111111;
    final KsqlTimestampExtractor timestampExtractor = mock(KsqlTimestampExtractor.class);
    when(processorContext.currentStreamTimeMs()).thenReturn(streamTime);

    // When
    final Processor<Struct, GenericRow, Struct, GenericRow> processor =
        getProcessor(timestampExtractor, processingLogger);
    processor.init(processorContext);
    final Record<Struct, GenericRow> record = new Record<>(
        key, null, 0);
    processor.process(record);

    // Then
    final Record<Struct, GenericRow> outputRecord = new Record<>(key, null, streamTime);
    verify(timestampExtractor, never()).extract(key, null);
    verify(processorContext).forward(eq(outputRecord));
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
        .forward(argThat(record -> record instanceof Record));

    // When
    final Processor<Struct, GenericRow, Struct, GenericRow> processor =
        getProcessor(timestampExtractor, processingLogger);
    processor.init(processorContext);
    final Record<Struct, GenericRow> inputRecord = new Record<>(key, row, 0);
    processor.process(inputRecord);

    // Then
    final Record<Struct, GenericRow> outputRecord = new Record<>(key, row, timestampColumnValue);
    verify(timestampExtractor).extract(key, row);
    verify(processorContext, Mockito.times(1))
        .forward(eq(outputRecord));
    verify(processingLogger, Mockito.times(1)).error(any());
  }

  @Test
  public void shouldCallProcessingLoggerOnTimestampExtractorError() {
    // Given
    final KsqlTimestampExtractor timestampExtractor
        = mock(KsqlTimestampExtractor.class);
    doThrow(KsqlException.class).when(timestampExtractor).extract(key, row);

    // When
    final Processor<Struct, GenericRow, Struct, GenericRow> processor =
        getProcessor(timestampExtractor, processingLogger);
    processor.init(processorContext);
    processor.process(new Record<>(key, row, 0));

    // Then
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
    getProcessor(timestampExtractor, processingLogger).init(null);
  }

  @Test(expected = NullPointerException.class)
  public void shouldThrowOnNullTimestampExtractor() {
    // When/Then
    getProcessor(null, null);
  }

  @Test(expected = NullPointerException.class)
  public void shouldThrowOnNullProcessingLogger() {
    // Given
    final KsqlTimestampExtractor timestampExtractor
        = mock(KsqlTimestampExtractor.class);

    // When/Then
    getProcessor(timestampExtractor, null);
  }

  private void buildDefaultSinkBuilder() {
    SinkBuilder.build(
        SCHEMA,
        Formats.of(KEY_FORMAT, VALUE_FORMAT, SerdeFeatures.of(), SerdeFeatures.of()),
        Optional.empty(),
        TOPIC,
        kstream,
        executionKeyFactory,
        queryContext,
        buildContext
    );
  }

  private static Processor<Struct, GenericRow, Struct, GenericRow> getProcessor(
      final KsqlTimestampExtractor timestampExtractor,
      final ProcessingLogger processingLogger
  ) {
    return new TimestampProcessorSupplier<Struct>(timestampExtractor, processingLogger).get();
  }
}
