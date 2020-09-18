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
import static org.mockito.ArgumentMatchers.eq;
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
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
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

  private static final ConnectSchema CONNECT_SCHEMA = (ConnectSchema) SchemaBuilder.struct()
      .field("vic", Schema.OPTIONAL_STRING_SCHEMA)
      .field("bob", Schema.OPTIONAL_INT32_SCHEMA)
      .build();

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
  private Serde<Struct> innerSerde;
  @Mock
  private Serializer<Struct> innerSerializer;
  @Mock
  private Deserializer<Struct> innerDeserializer;
  @Mock
  private Struct deserialized;
  @Mock
  private Schema deserializedSchema;
  @Mock
  private Serde<Object> loggingSerde;
  @Captor
  private ArgumentCaptor<Serde<GenericRow>> rowSerdeCaptor;

  private ValueSerdeFactory factory;
  private GenericRowSerializer serializer;
  private GenericRowDeserializer deserializer;
  private final Struct struct = new Struct(CONNECT_SCHEMA);

  @Before
  public void setUp() {
    factory = new GenericRowSerDe(innerFactory);

    serializer = new GenericRowSerializer(innerSerializer, CONNECT_SCHEMA);
    deserializer = new GenericRowDeserializer(innerDeserializer, CONNECT_SCHEMA);

    when(innerFactory.createFormatSerde(any(), any(), any(), any(), any())).thenReturn(innerSerde);
    when(innerFactory.wrapInLoggingSerde(any(), any(), any())).thenReturn(loggingSerde);
    when(innerSerde.serializer()).thenReturn(innerSerializer);
    when(innerSerde.deserializer()).thenReturn(innerDeserializer);
    when(innerSerializer.serialize(any(), any())).thenReturn(SERIALIZED);
    when(innerDeserializer.deserialize(any(), any())).thenReturn(deserialized);
    when(deserialized.schema()).thenReturn(deserializedSchema);
  }

  @Test
  public void shouldCreateInnerSerde() {
    // When:
    factory.create(format, schema, config, srClientFactory, LOGGER_PREFIX, processingLogCxt);

    // Then:
    verify(innerFactory).createFormatSerde("Value", format, schema, config, srClientFactory);
  }

  @Test
  public void shouldWrapInLoggingSerde() {
    // When:
    factory.create(format, schema, config, srClientFactory, LOGGER_PREFIX, processingLogCxt);

    // Then:
    verify(innerFactory).wrapInLoggingSerde(any(), eq(LOGGER_PREFIX), eq(processingLogCxt));
  }

  @Test
  public void shouldConfigureLoggingSerde() {
    // When:
    factory.create(format, schema, config, srClientFactory, LOGGER_PREFIX, processingLogCxt);

    // Then:
    verify(loggingSerde).configure(ImmutableMap.of(), false);
  }

  @Test
  public void shouldWrapInGenericSerde() {
    // When:
    factory.create(format, schema, config, srClientFactory, LOGGER_PREFIX, processingLogCxt);

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
        .create(format, schema, config, srClientFactory, LOGGER_PREFIX, processingLogCxt);

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
    assertThat(e.getMessage(), is("Field count mismatch on serialization."
        + " topic: topicName"
        + ", expected: 2"
        + ", got: 3"
    ));
  }

  @Test
  public void shouldThrowOnDeserializeOnColumnCountMismatch() {
    // Given:
    when(deserializedSchema.fields()).thenReturn(ImmutableList.of(
        new Field("too", 0, Schema.OPTIONAL_INT64_SCHEMA),
        new Field("many", 0, Schema.OPTIONAL_INT64_SCHEMA),
        new Field("columns", 0, Schema.OPTIONAL_INT64_SCHEMA)
    ));

    // When:
    final Exception e = assertThrows(
        SerializationException.class,
        () -> deserializer.deserialize("topicName", SERIALIZED)
    );

    // Then:
    assertThat(e.getMessage(), is("Field count mismatch on deserialization."
        + " topic: topicName"
        + ", expected: 2"
        + ", got: 3"
    ));
  }

  @Test
  public void shouldConvertRowToStructWhenSerializing() {
    // Given:
    final GenericRow row = GenericRow.genericRow("hello", 10);

    // When:
    serializer.serialize("topicName", row);

    // Then:
    verify(innerSerializer).serialize("topicName", struct
        .put("vic", "hello")
        .put("bob", 10)
    );
  }

  @Test
  public void shouldConvertStructToRowWhenDeserializing() {
    // Given:
    struct
        .put("vic", "world")
        .put("bob", -10);

    when(innerDeserializer.deserialize(any(), any())).thenReturn(struct);

    // When:
    final GenericRow row = deserializer.deserialize("topicName", SERIALIZED);

    // Then:
    assertThat(row, is(GenericRow.genericRow("world", -10)));
  }

  @Test
  public void shouldThrowOnSerializationIfColumnValueDoesNotMatchSchema() {
    // Given:
    final GenericRow row = GenericRow.genericRow("hello", "Not a number");

    // When:
    assertThrows(
        DataException.class,
        () -> serializer.serialize("topicName", row)
    );
  }

  @Test
  public void shouldThrowOnSerializationIfStructColumnValueDoesNotMatchSchema() {
    // Given:
    final GenericRow row = GenericRow.genericRow(struct, 10);

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> serializer.serialize("topicName", row)
    );

    // Then:
    assertThat(e.getMessage(), is(
        "Failed to prepare Struct value field 'vic' for serialization. "
            + "This could happen if the value was produced by a user-defined function "
            + "where the schema has non-optional return types. ksqlDB requires all "
            + "schemas to be optional at all levels of the Struct: the Struct itself, "
            + "schemas for all fields within the Struct, and so on."
    ));
  }
}