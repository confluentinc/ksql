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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.logging.processing.ProcessingLogContext;
import io.confluent.ksql.logging.processing.ProcessingLogger;
import io.confluent.ksql.logging.processing.ProcessingLoggerFactory;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.PhysicalSchema;
import io.confluent.ksql.util.KsqlConfig;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class GenericRowSerDeTest {

  private static final String LOGGER_PREFIX = "bob";

  private static final PhysicalSchema MUTLI_FIELD_SCHEMA =
      PhysicalSchema.from(
          LogicalSchema.of(SchemaBuilder.struct()
              .field("f0", Schema.OPTIONAL_STRING_SCHEMA)
              .field("f1", Schema.OPTIONAL_INT32_SCHEMA)
              .build()),
          SerdeOption.none());

  private static final PhysicalSchema WRAPPED_SINGLE_FIELD_SCHEMA =
      PhysicalSchema.from(
          LogicalSchema.of(SchemaBuilder.struct()
              .field("f0", Schema.OPTIONAL_STRING_SCHEMA)
              .build()),
          SerdeOption.none());

  private static final PhysicalSchema UNWRAPPED_SINGLE_FIELD_SCHEMA =
      PhysicalSchema.from(
          LogicalSchema.of(SchemaBuilder.struct()
              .field("f0", Schema.OPTIONAL_STRING_SCHEMA)
              .build()),
          SerdeOption.of(SerdeOption.UNWRAP_SINGLE_VALUES));

  private static final String SOME_TOPIC = "fred";
  private static final byte[] SOME_BYTES = "Vic".getBytes(StandardCharsets.UTF_8);
  private static final Map<String, ?> SOME_CONFIG = ImmutableMap.of("some", "thing");

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Mock
  private KsqlSerdeFactory valueSerdeFactory;
  @Mock
  private KsqlConfig ksqlConfig;
  @Mock
  private Supplier<SchemaRegistryClient> srClientFactory;
  @Mock
  private ProcessingLogContext processingContext;
  @Mock
  private ProcessingLoggerFactory loggerFactory;
  @Mock
  private Serde<Object> deletageSerde;
  @Mock
  private Serializer<Object> delegateSerializer;
  @Mock
  private Deserializer<Object> delegateDeserializer;

  private Serde<GenericRow> rowSerde;

  @Before
  public void setUp() {
    when(valueSerdeFactory.createSerde(any(), any(), any())).thenReturn(deletageSerde);
    when(deletageSerde.serializer()).thenReturn(delegateSerializer);
    when(deletageSerde.deserializer()).thenReturn(delegateDeserializer);

    when(delegateSerializer.serialize(any(), any())).thenReturn(SOME_BYTES);

    final ProcessingLogger logger = mock(ProcessingLogger.class);
    when(loggerFactory.getLogger(any())).thenReturn(logger);
    when(processingContext.getLoggerFactory()).thenReturn(loggerFactory);
  }

  @Test
  public void shouldGetStructSerdeOnConstruction() {
    // When:
    GenericRowSerDe.from(
        valueSerdeFactory,
        MUTLI_FIELD_SCHEMA,
        ksqlConfig,
        srClientFactory,
        LOGGER_PREFIX,
        processingContext
    );

    // Then:
    verify(valueSerdeFactory).createSerde(
        MUTLI_FIELD_SCHEMA.valueSchema(),
        ksqlConfig,
        srClientFactory
    );
  }

  @Test(expected = NullPointerException.class)
  public void shouldThrowOnNullStructSerde() {
    // Given:
    when(valueSerdeFactory.createSerde(any(), any(), any())).thenReturn(null);

    // When:
    GenericRowSerDe.from(
        valueSerdeFactory,
        MUTLI_FIELD_SCHEMA,
        ksqlConfig,
        srClientFactory,
        LOGGER_PREFIX,
        processingContext
    );
  }

  @Test(expected = NullPointerException.class)
  public void shouldThrowOnNullSchema() {
    // When:
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
  public void shouldCreateProcessingLoggerWithCorrectName() {
    // When:
    GenericRowSerDe.from(
        valueSerdeFactory,
        MUTLI_FIELD_SCHEMA,
        ksqlConfig,
        srClientFactory,
        LOGGER_PREFIX,
        processingContext
    );

    // Then:
    verify(loggerFactory).getLogger("bob.deserializer");
  }

  @Test
  public void shouldConfigureInnerSerializerForWrapped() {
    // Given:
    final Serializer<GenericRow> serializer = givenSerdeForSchema(MUTLI_FIELD_SCHEMA)
        .serializer();

    // When:
    serializer.configure(SOME_CONFIG, true);

    // Then:
    verify(delegateSerializer).configure(SOME_CONFIG, true);
  }

  @Test
  public void shouldConfigureInnerSerializerForUnwrapped() {
    // Given:
    final Serializer<GenericRow> serializer = givenSerdeForSchema(UNWRAPPED_SINGLE_FIELD_SCHEMA)
        .serializer();

    // When:
    serializer.configure(SOME_CONFIG, true);

    // Then:
    verify(delegateSerializer).configure(SOME_CONFIG, true);
  }

  @Test
  public void shouldConfigureInnerDeserializerForWrapped() {
    // Given:
    final Deserializer<GenericRow> deserializer = givenSerdeForSchema(MUTLI_FIELD_SCHEMA)
        .deserializer();

    // When:
    deserializer.configure(SOME_CONFIG, true);

    // Then:
    verify(delegateDeserializer).configure(SOME_CONFIG, true);
  }

  @Test
  public void shouldConfigureInnerDeserializerForUnwrapped() {
    // Given:
    final Deserializer<GenericRow> deserializer = givenSerdeForSchema(UNWRAPPED_SINGLE_FIELD_SCHEMA)
        .deserializer();

    // When:
    deserializer.configure(SOME_CONFIG, true);

    // Then:
    verify(delegateDeserializer).configure(SOME_CONFIG, true);
  }

  @Test
  public void shouldSerializeMultiFieldGenericRow() {
    // Given:
    final Serializer<GenericRow> serializer = givenSerdeForSchema(MUTLI_FIELD_SCHEMA)
        .serializer();

    final GenericRow row = new GenericRow("str", 10);

    // When:
    final byte[] bytes = serializer.serialize(SOME_TOPIC, row);

    // Then:
    verify(delegateSerializer).serialize(
        SOME_TOPIC,
        new Struct(MUTLI_FIELD_SCHEMA.logicalSchema().valueSchema())
            .put("f0", "str")
            .put("f1", 10)
    );

    assertThat(bytes, is(SOME_BYTES));
  }

  @Test
  public void shouldSerializeNullMultiFieldGenericRow() {
    // Given:
    final Serializer<GenericRow> serializer = givenSerdeForSchema(MUTLI_FIELD_SCHEMA)
        .serializer();

    when(delegateSerializer.serialize(any(), any())).thenReturn(null);

    // When:
    final byte[] bytes = serializer.serialize(SOME_TOPIC, null);

    // Then:
    verify(delegateSerializer).serialize(SOME_TOPIC, null);

    assertThat(bytes, is(nullValue()));
  }

  @Test
  public void shouldThrowOnSerializationOnTooFewFields() {
    // Given:
    final Serializer<GenericRow> serializer = givenSerdeForSchema(MUTLI_FIELD_SCHEMA)
        .serializer();

    final GenericRow tooFew = new GenericRow("str");

    // Then:
    expectedException.expect(SerializationException.class);
    expectedException.expectMessage("Field count mismatch. expected: 2, got: 1");

    // When:
    serializer.serialize(SOME_TOPIC, tooFew);
  }

  @Test
  public void shouldThrowOnSerializationOnTooManyFields() {
    // Given:
    final Serializer<GenericRow> serializer = givenSerdeForSchema(MUTLI_FIELD_SCHEMA)
        .serializer();

    final GenericRow tooFew = new GenericRow("str", 10, "extra");

    // Then:
    expectedException.expect(SerializationException.class);
    expectedException.expectMessage("Field count mismatch. expected: 2, got: 3");

    // When:
    serializer.serialize(SOME_TOPIC, tooFew);
  }

  @Test
  public void shouldSerializeWrappedSingleFieldGenericRow() {
    // Given:
    final Serializer<GenericRow> serializer = givenSerdeForSchema(WRAPPED_SINGLE_FIELD_SCHEMA)
        .serializer();

    final GenericRow row = new GenericRow("str");

    // When:
    final byte[] bytes = serializer.serialize(SOME_TOPIC, row);

    // Then:
    verify(delegateSerializer).serialize(
        SOME_TOPIC,
        new Struct(WRAPPED_SINGLE_FIELD_SCHEMA.logicalSchema().valueSchema())
            .put("f0", "str")
    );

    assertThat(bytes, is(SOME_BYTES));
  }

  @Test
  public void shouldSerializeUnwrappedSingleFieldGenericRow() {
    // Given:
    final Serializer<GenericRow> serializer = givenSerdeForSchema(UNWRAPPED_SINGLE_FIELD_SCHEMA)
        .serializer();

    final GenericRow row = new GenericRow("str");

    // When:
    final byte[] bytes = serializer.serialize(SOME_TOPIC, row);

    // Then:
    verify(delegateSerializer).serialize(SOME_TOPIC, "str");

    assertThat(bytes, is(SOME_BYTES));
  }

  @Test
  public void shouldSerializeNullUnwrappedSingleFieldGenericRow() {
    // Given:
    final Serializer<GenericRow> serializer = givenSerdeForSchema(UNWRAPPED_SINGLE_FIELD_SCHEMA)
        .serializer();

    when(delegateSerializer.serialize(any(), any())).thenReturn(null);

    // When:
    final byte[] bytes = serializer.serialize(SOME_TOPIC, null);

    // Then:
    verify(delegateSerializer).serialize(SOME_TOPIC, null);

    assertThat(bytes, is(nullValue()));
  }

  @Test
  public void shouldThrowOnMultiFieldRowIfUsingUnwrappedSerializer() {
    // Given:
    final Serializer<GenericRow> serializer = givenSerdeForSchema(UNWRAPPED_SINGLE_FIELD_SCHEMA)
        .serializer();

    final GenericRow row = new GenericRow("str", "too many fields");

    // Then:
    expectedException.expect(SerializationException.class);
    expectedException.expectMessage("Expected single-field value. got: 2");

    // When:
    serializer.serialize(SOME_TOPIC, row);
  }

  @Test
  public void shouldDeserializeMultiFieldGenericRow() {
    // Given:
    final Deserializer<GenericRow> deserializer = givenSerdeForSchema(MUTLI_FIELD_SCHEMA)
        .deserializer();

    when(delegateDeserializer.deserialize(any(), any()))
        .thenReturn(new Struct(MUTLI_FIELD_SCHEMA.logicalSchema().valueSchema())
            .put("f0", "str")
            .put("f1", 10));

    // When:
    final GenericRow row = deserializer.deserialize(SOME_TOPIC, SOME_BYTES);

    // Then:
    verify(delegateDeserializer).deserialize(SOME_TOPIC, SOME_BYTES);

    assertThat(row, is(new GenericRow("str", 10)));
  }

  @Test
  public void shouldDeserializeNullMultiFieldGenericRow() {
    // Given:
    final Deserializer<GenericRow> deserializer = givenSerdeForSchema(MUTLI_FIELD_SCHEMA)
        .deserializer();

    when(delegateDeserializer.deserialize(any(), any())).thenReturn(null);

    // When:
    final GenericRow row = deserializer.deserialize(SOME_TOPIC, null);

    // Then:
    verify(delegateDeserializer).deserialize(SOME_TOPIC, null);

    assertThat(row, is(nullValue()));
  }

  @Test
  public void shouldDeserializeWrappedSingleFieldGenericRow() {
    // Given:
    final Deserializer<GenericRow> deserializer = givenSerdeForSchema(WRAPPED_SINGLE_FIELD_SCHEMA)
        .deserializer();

    when(delegateDeserializer.deserialize(any(), any()))
        .thenReturn(new Struct(WRAPPED_SINGLE_FIELD_SCHEMA.logicalSchema().valueSchema())
            .put("f0", "str"));

    // When:
    final GenericRow row = deserializer.deserialize(SOME_TOPIC, SOME_BYTES);

    // Then:
    verify(delegateDeserializer).deserialize(SOME_TOPIC, SOME_BYTES);

    assertThat(row, is(new GenericRow("str")));
  }

  @Test
  public void shouldDeserializeUnwrappedSingleFieldGenericRow() {
    // Given:
    final Deserializer<GenericRow> deserializer = givenSerdeForSchema(UNWRAPPED_SINGLE_FIELD_SCHEMA)
        .deserializer();

    when(delegateDeserializer.deserialize(any(), any())).thenReturn("str");

    // When:
    final GenericRow row = deserializer.deserialize(SOME_TOPIC, SOME_BYTES);

    // Then:
    verify(delegateDeserializer).deserialize(SOME_TOPIC, SOME_BYTES);

    assertThat(row, is(new GenericRow("str")));
  }

  @Test
  public void shouldDeserializeNullUnwrappedSingleFieldGenericRow() {
    // Given:
    final Deserializer<GenericRow> deserializer = givenSerdeForSchema(UNWRAPPED_SINGLE_FIELD_SCHEMA)
        .deserializer();

    when(delegateDeserializer.deserialize(any(), any())).thenReturn(null);

    // When:
    final GenericRow row = deserializer.deserialize(SOME_TOPIC, SOME_BYTES);

    // Then:
    verify(delegateDeserializer).deserialize(SOME_TOPIC, SOME_BYTES);

    assertThat(row, is(nullValue()));
  }

  private Serde<GenericRow> givenSerdeForSchema(final PhysicalSchema schema) {
    rowSerde = GenericRowSerDe.from(
        valueSerdeFactory,
        schema,
        ksqlConfig,
        srClientFactory,
        LOGGER_PREFIX,
        processingContext
    );
    return rowSerde;
  }
}