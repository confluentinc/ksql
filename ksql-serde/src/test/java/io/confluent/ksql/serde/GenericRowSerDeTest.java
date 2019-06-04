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
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.logging.processing.ProcessingLogContext;
import io.confluent.ksql.schema.persistence.PersistenceSchema;
import io.confluent.ksql.util.KsqlConfig;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class GenericRowSerDeTest {

  private static final String LOGGER_PREFIX = "bob";

  private static final ConnectSchema ROW_SCHEMA = (ConnectSchema) SchemaBuilder.struct()
      .field("f0", Schema.OPTIONAL_STRING_SCHEMA)
      .field("f1", Schema.OPTIONAL_INT32_SCHEMA)
      .build();

  private static final String SOME_TOPIC = "fred";
  private static final byte[] SOME_BYTES = "Vic".getBytes(StandardCharsets.UTF_8);
  private static final Map<String, ?> SOME_CONFIG = ImmutableMap.of("some", "thing");

  @Mock
  private KsqlSerdeFactory valueSerdeFactory;
  @Mock
  private KsqlConfig ksqlConfig;
  @Mock
  private Supplier<SchemaRegistryClient> srClientFactory;
  @Mock
  private ProcessingLogContext processingContext;
  @Mock
  private Serde<Object> deletageSerde;
  @Mock
  private Serializer<Object> delegateSerializer;
  @Mock
  private Deserializer<Object> delegateDeserializer;
  private Serde<GenericRow> rowSerde;

  @Before
  public void setUp() {
    when(valueSerdeFactory.createSerde(any(), any(), any(), any(), any())).thenReturn(deletageSerde);
    when(deletageSerde.serializer()).thenReturn(delegateSerializer);
    when(deletageSerde.deserializer()).thenReturn(delegateDeserializer);

    rowSerde = GenericRowSerDe.from(
        valueSerdeFactory,
        ROW_SCHEMA,
        ksqlConfig,
        srClientFactory,
        LOGGER_PREFIX,
        processingContext
    );
  }

  @Test
  public void shouldGetStructSerdeOnConstruction() {
    // Given:
    clearInvocations(valueSerdeFactory);

    // When:
    GenericRowSerDe.from(
        valueSerdeFactory,
        ROW_SCHEMA,
        ksqlConfig,
        srClientFactory,
        LOGGER_PREFIX,
        processingContext
    );

    // Then:
    verify(valueSerdeFactory).createSerde(
        PersistenceSchema.of(ROW_SCHEMA),
        ksqlConfig,
        srClientFactory,
        LOGGER_PREFIX,
        processingContext
    );
  }

  @Test(expected = NullPointerException.class)
  public void shouldThrowOnNullStructSerde() {
    // Given:
    when(valueSerdeFactory.createSerde(any(), any(), any(), any(), any())).thenReturn(null);

    // When:
    GenericRowSerDe.from(
        valueSerdeFactory,
        ROW_SCHEMA,
        ksqlConfig,
        srClientFactory,
        LOGGER_PREFIX,
        processingContext
    );
  }

  @Test(expected = NullPointerException.class)
  public void shouldThrowOnNullSchema() {
     GenericRowSerDe.from(
        valueSerdeFactory,
        null,
        ksqlConfig,
        srClientFactory,
        LOGGER_PREFIX,
        processingContext
    );
  }

  @Test
  public void shouldConfigureInnerSerializer() {
    // Given:
    final Serializer<GenericRow> serializer = rowSerde.serializer();

    // When:
    serializer.configure(SOME_CONFIG, true);

    // Then:
    verify(delegateSerializer).configure(SOME_CONFIG, true);
  }

  @Test
  public void shouldConfigureInnerDeserializer() {
    // Given:
    final Deserializer<GenericRow> deserializer = rowSerde.deserializer();

    // When:
    deserializer.configure(SOME_CONFIG, true);

    // Then:
    verify(delegateDeserializer).configure(SOME_CONFIG, true);
  }

  @Test
  public void shouldRequestNewSerializerEachTime() {
    // Given:
    rowSerde.serializer();

    // When:
    rowSerde.serializer();

    // Then:
    verify(deletageSerde, times(2)).serializer();
  }

  @Test
  public void shouldRequestNewDeserializerEachTime() {
    // Given:
    rowSerde.deserializer();

    // When:
    rowSerde.deserializer();

    // Then:
    verify(deletageSerde, times(2)).deserializer();
  }

  @Test
  public void shouldSerializeGenericRow() {
    // Given:
    final GenericRow row = new GenericRow("str", 10);

    when(delegateSerializer.serialize(any(), any())).thenReturn(SOME_BYTES);

    final Serializer<GenericRow> serializer = rowSerde.serializer();

    // When:
    final byte[] bytes = serializer.serialize(SOME_TOPIC, row);

    // Then:
    verify(delegateSerializer).serialize(
        SOME_TOPIC,
        new Struct(ROW_SCHEMA)
            .put("f0", "str")
            .put("f1", 10)
    );

    assertThat(bytes, is(SOME_BYTES));
  }

  @Test
  public void shouldSerializeNullGenericRow() {
    // Given:
    when(delegateSerializer.serialize(any(), any())).thenReturn(null);

    final Serializer<GenericRow> serializer = rowSerde.serializer();

    // When:
    final byte[] bytes = serializer.serialize(SOME_TOPIC, null);

    // Then:
    verify(delegateSerializer).serialize(SOME_TOPIC, null);

    assertThat(bytes, is(nullValue()));
  }

  @Test
  public void shouldDeserializeGenericRow() {
    // Given:
    when(delegateDeserializer.deserialize(any(), any()))
        .thenReturn(new Struct(ROW_SCHEMA)
            .put("f0", "str")
            .put("f1", 10));

    final Deserializer<GenericRow> deserializer = rowSerde.deserializer();

    // When:
    final GenericRow row = deserializer.deserialize(SOME_TOPIC, SOME_BYTES);

    // Then:
    verify(delegateDeserializer).deserialize(SOME_TOPIC, SOME_BYTES);

    assertThat(row, is(new GenericRow("str", 10)));
  }

  @Test
  public void shouldDeserializeNullGenericRow() {
    // Given:
    when(delegateDeserializer.deserialize(any(), any())).thenReturn(null);

    final Deserializer<GenericRow> deserializer = rowSerde.deserializer();

    // When:
    final GenericRow row = deserializer.deserialize(SOME_TOPIC, null);

    // Then:
    verify(delegateDeserializer).deserialize(SOME_TOPIC, null);

    assertThat(row, is(nullValue()));
  }
}