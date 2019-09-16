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
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.builder.KsqlQueryBuilder;
import io.confluent.ksql.execution.context.QueryContext;
import io.confluent.ksql.execution.plan.DefaultExecutionStepProperties;
import io.confluent.ksql.execution.plan.ExecutionStep;
import io.confluent.ksql.execution.plan.Formats;
import io.confluent.ksql.execution.plan.StreamSink;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.PhysicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.serde.Format;
import io.confluent.ksql.serde.FormatInfo;
import io.confluent.ksql.serde.KeyFormat;
import io.confluent.ksql.serde.KeySerde;
import io.confluent.ksql.serde.SerdeOption;
import io.confluent.ksql.serde.ValueFormat;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

public class StreamSinkBuilderTest {
  private static final String TOPIC = "TOPIC";
  private static final LogicalSchema SCHEMA = LogicalSchema.builder()
      .valueColumn("BLUE", SqlTypes.BIGINT)
      .valueColumn("GREEN", SqlTypes.STRING)
      .build()
      .withMetaAndKeyColsInValue();
  private static final PhysicalSchema PHYSICAL_SCHEMA =
      PhysicalSchema.from(SCHEMA.withoutMetaAndKeyColsInValue(), SerdeOption.none());
  private static final KeyFormat KEY_FORMAT = KeyFormat.nonWindowed(FormatInfo.of(Format.KAFKA));
  private static final ValueFormat VALUE_FORMAT = ValueFormat.of(FormatInfo.of(Format.JSON));

  @Mock
  private KsqlQueryBuilder queryBuilder;
  @Mock
  private KeySerdeFactory<Struct> keySerdeFactory;
  @Mock
  private KStream<Struct, GenericRow> stream;
  @Mock
  private ExecutionStep<KStream<Struct, GenericRow>> source;
  @Mock
  private KeySerde<Struct> keySerde;
  @Mock
  private Serde<GenericRow> valSerde;
  @Captor
  private ArgumentCaptor<ValueMapper<GenericRow, GenericRow>> mapperCaptor;

  private final QueryContext queryContext =
      new QueryContext.Stacker(new QueryId("qid")).push("sink").getQueryContext();

  private StreamSink<Struct> sink;

  @Rule
  public final MockitoRule mockitoRule = MockitoJUnit.rule();

  @Before
  @SuppressWarnings("unchecked")
  public void setup() {
    when(keySerdeFactory.buildKeySerde(any(), any(), any())).thenReturn(keySerde);
    when(queryBuilder.buildValueSerde(any(), any(), any())).thenReturn(valSerde);
    when(stream.mapValues(any(ValueMapper.class))).thenReturn(stream);
    when(source.getProperties()).thenReturn(
        new DefaultExecutionStepProperties(SCHEMA, mock(QueryContext.class))
    );
    sink = new StreamSink<>(
        new DefaultExecutionStepProperties(SCHEMA, queryContext),
        source,
        Formats.of(KEY_FORMAT, VALUE_FORMAT, SerdeOption.none()),
        TOPIC
    );
  }

  @Test
  @SuppressWarnings("unchecked")
  public void shouldWriteOutStream() {
    // When:
    StreamSinkBuilder.build(stream, sink, keySerdeFactory, queryBuilder);

    // Then:
    final InOrder inOrder = Mockito.inOrder(stream);
    inOrder.verify(stream).mapValues(any(ValueMapper.class));
    inOrder.verify(stream).to(anyString(), any());
    verifyNoMoreInteractions(stream);
  }

  @Test
  public void shouldWriteOutStreamToCorrectTopic() {
    // When:
    StreamSinkBuilder.build(stream, sink, keySerdeFactory, queryBuilder);

    // Then:
    verify(stream).to(eq(TOPIC), any());
  }

  @Test
  public void shouldBuildKeySerdeCorrectly() {
    // When:
    StreamSinkBuilder.build(stream, sink, keySerdeFactory, queryBuilder);

    // Then:
    verify(keySerdeFactory).buildKeySerde(KEY_FORMAT, PHYSICAL_SCHEMA, queryContext);
  }

  @Test
  public void shouldBuildValueSerdeCorrectly() {
    // When:
    StreamSinkBuilder.build(stream, sink, keySerdeFactory, queryBuilder);

    // Then:
    verify(queryBuilder).buildValueSerde(
        VALUE_FORMAT.getFormatInfo(),
        PHYSICAL_SCHEMA,
        queryContext
    );
  }

  @Test
  public void shouldWriteOutStreamWithCorrectSerdes() {
    // When:
    StreamSinkBuilder.build(stream, sink, keySerdeFactory, queryBuilder);

    // Then:
    verify(stream).to(anyString(), eq(Produced.with(keySerde, valSerde)));
  }

  @Test
  public void shouldRemoveKeyAndTimeFieldsFromValue() {
    // When:
    StreamSinkBuilder.build(stream, sink, keySerdeFactory, queryBuilder);

    // Then:
    verify(stream).mapValues(mapperCaptor.capture());
    final ValueMapper<GenericRow, GenericRow> mapper = mapperCaptor.getValue();
    assertThat(
        mapper.apply(new GenericRow(123, "456", 789, "101112")),
        equalTo(new GenericRow(789, "101112"))
    );
  }

  @Test
  public void shouldIgnoreNullRowsWhenRemovingKeyAndTimeFieldsFromValue() {
    // When:
    StreamSinkBuilder.build(stream, sink, keySerdeFactory, queryBuilder);

    // Then:
    verify(stream).mapValues(mapperCaptor.capture());
    final ValueMapper<GenericRow, GenericRow> mapper = mapperCaptor.getValue();
    assertThat(mapper.apply(null), is(nullValue()));
  }
}