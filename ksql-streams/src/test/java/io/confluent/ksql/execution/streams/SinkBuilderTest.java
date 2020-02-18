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

import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.builder.KsqlQueryBuilder;
import io.confluent.ksql.execution.context.QueryContext;
import io.confluent.ksql.execution.plan.Formats;
import io.confluent.ksql.execution.plan.KeySerdeFactory;
import io.confluent.ksql.execution.timestamp.TimestampColumn;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.serde.FormatFactory;
import io.confluent.ksql.serde.FormatInfo;
import io.confluent.ksql.serde.SerdeOption;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
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

import java.util.Optional;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class SinkBuilderTest {
  private static final String TOPIC = "TOPIC";

  private static final LogicalSchema SCHEMA = LogicalSchema.builder()
      .valueColumn(ColumnName.of("BLUE"), SqlTypes.BIGINT)
      .valueColumn(ColumnName.of("GREEN"), SqlTypes.STRING)
      .build();

  private static final FormatInfo KEY_FORMAT = FormatInfo.of(FormatFactory.KAFKA.name());
  private static final FormatInfo VALUE_FORMAT = FormatInfo.of(FormatFactory.JSON.name());

  @Mock
  private KsqlQueryBuilder queryBuilder;
  @Mock
  private KeySerdeFactory<Struct> keySerdeFactory;
  @Mock
  private KStream<Struct, GenericRow> kStream;
  @Mock
  private Serde<Struct> keySerde;
  @Mock
  private Serde<GenericRow> valSerde;
  @Mock
  private QueryContext queryContext;
  @Mock
  private GenericRow row;
  @Captor
  private ArgumentCaptor<To> toCaptor;

  @Before
  public void setup() {
    when(keySerdeFactory.buildKeySerde(any(), any(), any())).thenReturn(keySerde);
    when(queryBuilder.buildValueSerde(any(), any(), any())).thenReturn(valSerde);
    doReturn(kStream).when(kStream).transform(any());
  }

  @Test
  public void shouldBuildStreamUsingTransformTimestampWhenTimestampIsSpecified() {
    // Given/When
    SinkBuilder.build(
        SCHEMA,
        Formats.of(KEY_FORMAT, VALUE_FORMAT, SerdeOption.none()),
        Optional.of(new TimestampColumn(ColumnName.of("GREEN"), Optional.empty())),
        TOPIC,
        kStream,
        keySerdeFactory,
        queryContext,
        queryBuilder
    );

    // Then
    final InOrder inOrder = Mockito.inOrder(kStream);
    inOrder.verify(kStream).transform(new SinkBuilder.TransformTimestamp<>(1));
    inOrder.verify(kStream).to(anyString(), any());
    inOrder.verifyNoMoreInteractions();
  }

  @Test
  public void shouldBuildStreamWithoutTransformTimestampWhenNoTimestampIsSpecified() {
    // Given/When
    SinkBuilder.build(
        SCHEMA,
        Formats.of(KEY_FORMAT, VALUE_FORMAT, SerdeOption.none()),
        Optional.empty(),
        TOPIC,
        kStream,
        keySerdeFactory,
        queryContext,
        queryBuilder
    );

    // Then
    final InOrder inOrder = Mockito.inOrder(kStream);
    inOrder.verify(kStream).to(anyString(), any());
    inOrder.verifyNoMoreInteractions();
  }

  @Test
  public void shouldTransformTimestampRow() {
    // Given
    final long timestampColumnValue = 10001;
    final int timestampColumnIndex = 2;
    final ProcessorContext context = mock(ProcessorContext.class);
    when(row.get(timestampColumnIndex)).thenReturn(timestampColumnValue);

    // When
    final Transformer<String, GenericRow, KeyValue<String, GenericRow>> transformer =
        new SinkBuilder.TransformTimestamp<String>(timestampColumnIndex).get();
    transformer.init(context);
    final KeyValue<String, GenericRow> kv = transformer.transform("key", row);

    // Then
    assertNull(kv);
    verify(context, Mockito.times(1))
        .forward(eq("key"), eq(row), toCaptor.capture());
    assertTrue(toCaptor.getValue().equals(To.all().withTimestamp(timestampColumnValue)));
  }

  @Test
  public void shouldNotTransformTimestampIfTimestampIndexNotProvided() {
    // Given
    final int timestampColumnIndex = -1;
    final ProcessorContext context = mock(ProcessorContext.class);
    final GenericRow row = mock(GenericRow.class);

    // When
    final Transformer<String, GenericRow, KeyValue<String, GenericRow>> transformer =
        new SinkBuilder.TransformTimestamp<String>(timestampColumnIndex).get();
    transformer.init(context);
    final KeyValue<String, GenericRow> kv = transformer.transform("key", row);

    // Then
    assertThat(kv, is(KeyValue.pair("key", row)));
    verifyZeroInteractions(context);
  }
}
