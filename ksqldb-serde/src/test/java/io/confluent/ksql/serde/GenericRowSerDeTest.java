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

package io.confluent.ksql.serde;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.logging.processing.ProcessingLogContext;
import io.confluent.ksql.schema.ksql.PersistenceSchema;
import io.confluent.ksql.serde.GenericRowSerDe.GenericRowDeserializer;
import io.confluent.ksql.serde.GenericRowSerDe.GenericRowSerializer;
import io.confluent.ksql.serde.tracked.TrackedCallback;
import io.confluent.ksql.util.KsqlConfig;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class GenericRowSerDeTest {

  private static final String LOGGER_PREFIX = "bob";
  private static final Map<String, ?> SOME_CONFIG = ImmutableMap.of("some", "thing");
  private static final byte[] SERIALIZED = "serialized".getBytes(StandardCharsets.UTF_8);

  @Mock
  private GenericSerdeFactory innerFactory;
  @Mock
  private FormatInfo format;
  @Mock
  private PersistenceSchema schema;
  @Mock
  private KsqlConfig config;
  @Mock
  private Supplier<SchemaRegistryClient> srClientFactory;
  @Mock
  private ProcessingLogContext processingLogCxt;
  @Mock
  private Serde<List<?>> innerSerde;
  @Mock
  private Serializer<List<?>> innerSerializer;
  @Mock
  private Deserializer<List<?>> innerDeserializer;
  @Mock
  private Serde<Object> loggingSerde;
  @Mock
  private Serde<Object> trackingSerde;
  @Mock
  private TrackedCallback callback;
  @Captor
  private ArgumentCaptor<Serde<GenericRow>> rowSerdeCaptor;

  private ValueSerdeFactory factory;
  private GenericRowSerializer serializer;
  private GenericRowDeserializer deserializer;

  @Before
  public void setUp() {
    factory = new GenericRowSerDe(innerFactory);

    serializer = new GenericRowSerializer(innerSerializer, 2);
    deserializer = new GenericRowDeserializer(innerDeserializer, 2);

    when(innerFactory.createFormatSerde(any(), any(), any(), any(), any(), anyBoolean())).thenReturn(innerSerde);
    when(innerFactory.wrapInLoggingSerde(any(), any(), any())).thenReturn(loggingSerde);
    when(innerFactory.wrapInTrackingSerde(any(), any())).thenReturn(trackingSerde);
    when(innerSerde.serializer()).thenReturn(innerSerializer);
    when(innerSerde.deserializer()).thenReturn(innerDeserializer);
    when(innerSerializer.serialize(any(), any())).thenReturn(SERIALIZED);
  }

  @Test
  public void shouldCreateInnerSerde() {
    // When:
    factory.create(format, schema, config, srClientFactory, LOGGER_PREFIX, processingLogCxt,
        Optional.empty());

    // Then:
    verify(innerFactory).createFormatSerde("Value", format, schema, config, srClientFactory, false);
  }

  @Test
  public void shouldWrapInLoggingSerde() {
    // When:
    factory.create(format, schema, config, srClientFactory, LOGGER_PREFIX, processingLogCxt,
        Optional.empty());

    // Then:
    verify(innerFactory).wrapInLoggingSerde(any(), eq(LOGGER_PREFIX), eq(processingLogCxt));
  }

  @Test
  public void shouldConfigureLoggingSerde() {
    // When:
    factory.create(format, schema, config, srClientFactory, LOGGER_PREFIX, processingLogCxt,
        Optional.empty());

    // Then:
    verify(loggingSerde).configure(ImmutableMap.of(), false);
  }

  @Test
  public void shouldReturnTrackingSerde() {
    // When:
    factory.create(format, schema, config, srClientFactory, LOGGER_PREFIX, processingLogCxt,
        Optional.of(callback));

    // Then:
    verify(innerFactory).wrapInTrackingSerde(loggingSerde, callback);
  }

  @Test
  public void shouldNotWrapInTrackingSerdeIfNoCallbackProvided() {
    // When:
    factory.create(format, schema, config, srClientFactory, LOGGER_PREFIX, processingLogCxt,
        Optional.empty());

    // Then:
    verify(innerFactory, never()).wrapInTrackingSerde(any(), any());
  }

  @Test
  public void shouldWrapInGenericSerde() {
    // When:
    factory.create(format, schema, config, srClientFactory, LOGGER_PREFIX, processingLogCxt,
        Optional.empty());

    // Then:
    verify(innerFactory).wrapInLoggingSerde(rowSerdeCaptor.capture(), any(), any());

    assertThat(rowSerdeCaptor.getValue().serializer(), is(instanceOf(GenericRowSerializer.class)));
    assertThat(rowSerdeCaptor.getValue().deserializer(),
        is(instanceOf(GenericRowDeserializer.class)));
  }

  @Test
  public void shouldReturnLoggingSerde() {
    // When:
    final Serde<GenericRow> result = factory
        .create(format, schema, config, srClientFactory, LOGGER_PREFIX, processingLogCxt,
            Optional.empty());

    // Then:
    assertThat(result, is(sameInstance(loggingSerde)));
  }

  @Test
  public void shouldConfigureInnerSerializerOnConfigure() {
    // When:
    serializer.configure(SOME_CONFIG, true);

    // Then:
    verify(innerSerializer).configure(SOME_CONFIG, true);
  }

  @Test
  public void shouldConfigureInnerDeserializerOnConfigure() {
    // When:
    deserializer.configure(SOME_CONFIG, true);

    // Then:
    verify(innerDeserializer).configure(SOME_CONFIG, true);
  }

  @Test
  public void shouldCloseInnerSerializerOnClose() {
    // When:
    serializer.close();

    // Then:
    verify(innerSerializer).close();
  }

  @Test
  public void shouldCloseInnerDeserializerOnClose() {
    // When:
    deserializer.close();

    // Then:
    verify(innerDeserializer).close();
  }

  @Test
  public void shouldSerializeNulls() {
    // When:
    final byte[] result = serializer.serialize("topic", null);

    // Then:
    verify(innerSerializer).serialize("topic", null);
    assertThat(result, is(SERIALIZED));
  }

  @Test
  public void shouldDeserializeNulls() {
    // Given:
    when(innerDeserializer.deserialize(any(), any())).thenReturn(null);

    // When:
    final GenericRow result = deserializer.deserialize("topic", SERIALIZED);

    // Then:
    verify(innerDeserializer).deserialize("topic", SERIALIZED);
    assertThat(result, is(nullValue()));
  }

  @Test
  public void shouldThrowOnSerializeOnColumnCountMismatch() {
    // Given:
    final GenericRow row = GenericRow.genericRow("too", "many", "columns");

    // When:
    final Exception e = assertThrows(
        SerializationException.class,
        () -> serializer.serialize("topicName", row)
    );

    // Then:
    assertThat(e.getMessage(), is("Column count mismatch on serialization."
        + " topic: topicName"
        + ", expected: 2"
        + ", got: 3"
    ));
  }

  @Test
  public void shouldThrowOnDeserializeOnColumnCountMismatch() {
    // Given:
    givenInnerDeserializerReturns(ImmutableList.of("too", "many", "columns"));

    // When:
    final Exception e = assertThrows(
        SerializationException.class,
        () -> deserializer.deserialize("topicName", SERIALIZED)
    );

    // Then:
    assertThat(e.getMessage(), is("Column count mismatch on deserialization."
        + " topic: topicName"
        + ", expected: 2"
        + ", got: 3"
    ));
  }

  @Test
  public void shouldConvertRowToListWhenSerializing() {
    // Given:
    final GenericRow row = GenericRow.genericRow("hello", 10);

    // When:
    serializer.serialize("topicName", row);

    // Then:
    verify(innerSerializer).serialize("topicName", row.values());
  }

  @Test
  public void shouldConvertListToRowWhenDeserializing() {
    // Given:
    givenInnerDeserializerReturns(ImmutableList.of("world", -10));

    // When:
    final GenericRow row = deserializer.deserialize("topicName", SERIALIZED);

    // Then:
    assertThat(row, is(GenericRow.genericRow("world", -10)));
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private void givenInnerDeserializerReturns(final List<?> values) {
    when(innerDeserializer.deserialize(any(), any())).thenReturn((List)values);
  }
}