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

package io.confluent.ksql.serde.avro;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.fail;
import static org.junit.internal.matchers.ThrowableMessageMatcher.hasMessage;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.confluent.connect.avro.AvroConverter;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.ksql.logging.processing.ProcessingLogger;
import io.confluent.ksql.schema.persistence.PersistenceSchema;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlConstants;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import org.apache.avro.LogicalTypes;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.connect.data.ConnectSchema;
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
public class KsqlAvroDeserializerTest {

  private static final org.apache.avro.Schema BOOLEAN_AVRO_SCHEMA =
      parseAvroSchema("{\"type\": \"boolean\"}");

  private static final org.apache.avro.Schema INT_AVRO_SCHEMA =
      parseAvroSchema("{\"type\": \"int\"}");

  private static final org.apache.avro.Schema LONG_AVRO_SCHEMA =
      parseAvroSchema("{\"type\": \"long\"}");

  private static final org.apache.avro.Schema DOUBLE_AVRO_SCHEMA =
      parseAvroSchema("{\"type\": \"double\"}");

  private static final org.apache.avro.Schema STRING_AVRO_SCHEMA =
      parseAvroSchema("{\"type\": \"string\"}");

  private static final org.apache.avro.Schema STRING_ARRAY_AVRO_SCHEMA =
      parseAvroSchema("{\"type\": \"array\", \"items\": \"string\"}");

  private static final org.apache.avro.Schema INT_MAP_AVRO_SCHEMA =
      parseAvroSchema("{\"type\": \"map\", \"values\": \"int\"}");

  private static final org.apache.avro.Schema LONG_MAP_AVRO_SCHEMA =
      parseAvroSchema("{\"type\": \"map\", \"values\": \"long\"}");

  private static final org.apache.avro.Schema ARRAY_OF_MAP_AVRO_SCHEMA =
      parseAvroSchema("{\"type\": \"array\", \"items\": ["
          + "\"null\", "
          + "{\"type\": \"map\", \"values\": \"long\"}"
          + "]}");

  private static final org.apache.avro.Schema ORDER_AVRO_SCHEMA = parseAvroSchema("{"
      + "\"namespace\": \"kql\","
      + "\"name\": \"orders\","
      + "\"type\": \"record\","
      + "\"fields\": ["
      + " {\"name\": \"orderTime\", \"type\": [\"null\",\"long\"]},"
      + " {\"name\": \"orderId\",  \"type\": [\"null\",\"long\"]},"
      + " {\"name\": \"itemId\", \"type\": [\"null\",\"string\"]},"
      + " {\"name\": \"orderUnits\", \"type\": [\"null\",\"double\"]},"
      + " {\"name\": \"arrayCol\", \"type\": {\"type\": \"array\", \"items\": [\"null\",\"double\"]}},"
      + " {\"name\": \"mapCol\", \"type\": {\"type\": \"map\", \"values\": [\"null\",\"double\"]}}"
      + " ]"
      + "}");

  private static final String SOME_TOPIC = "bob";

  private static final String ORDERTIME = "ORDERTIME";
  private static final String ORDERID = "ORDERID";
  private static final String ITEMID = "ITEMID";
  private static final String ORDERUNITS = "ORDERUNITS";
  private static final String ARRAYCOL = "ARRAYCOL";
  private static final String MAPCOL = "MAPCOL";

  private static final Schema ORDER_SCHEMA = SchemaBuilder.struct()
      .field(ORDERTIME, Schema.OPTIONAL_INT64_SCHEMA)
      .field(ORDERID, Schema.OPTIONAL_INT64_SCHEMA)
      .field(ITEMID, Schema.OPTIONAL_STRING_SCHEMA)
      .field(ORDERUNITS, Schema.OPTIONAL_FLOAT64_SCHEMA)
      .field(ARRAYCOL, SchemaBuilder
          .array(Schema.OPTIONAL_FLOAT64_SCHEMA)
          .optional()
          .build())
      .field(MAPCOL, SchemaBuilder
          .map(Schema.STRING_SCHEMA, Schema.OPTIONAL_FLOAT64_SCHEMA)
          .optional()
          .build())
      .optional()
      .build();

  private static final Map<String, Object> AN_ORDER = ImmutableMap.<String, Object>builder()
      .put("orderTime", 1511897796092L)
      .put("orderId", 1L)
      .put("itemId", "Item_1")
      .put("orderUnits", 10.0)
      .put("arrayCol", ImmutableList.of(10.0, 20.0))
      .put("mapCol", Collections.singletonMap("key1", 10.0))
      .build();

  private static final KsqlConfig KSQL_CONFIG = new KsqlConfig(Collections.singletonMap(
      KsqlConfig.SCHEMA_REGISTRY_URL_PROPERTY, "fake-schema-registry-url"));

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Mock
  private ProcessingLogger recordLogger;

  private SchemaRegistryClient schemaRegistryClient;
  private AvroConverter converter;
  private Deserializer<Object> deserializer;
  private KafkaAvroSerializer serializer;

  @Before
  public void setUp() {
    final ImmutableMap<String, Object> configs = ImmutableMap.of(
        AbstractKafkaAvroSerDeConfig.AUTO_REGISTER_SCHEMAS, true,
        AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, ""
    );

    schemaRegistryClient = new MockSchemaRegistryClient();

    converter = new AvroConverter(schemaRegistryClient);
    converter.configure(configs, false);

    serializer = new KafkaAvroSerializer(schemaRegistryClient, configs);
  }

  @Test
  public void shouldDeserializeAvroRecordCorrectly() {
    // Given:
    final byte[] bytes = givenAvroSerialized(AN_ORDER, ORDER_AVRO_SCHEMA);

    givenDeserializerForSchema(ORDER_SCHEMA);

    // When:
    final Object result = deserializer.deserialize(SOME_TOPIC, bytes);

    // Then:
    final Struct expectedResult = buildExpectedStruct(ORDER_SCHEMA, AN_ORDER);
    assertThat(result, is(expectedResult));
  }

  @Test
  public void shouldCoerceFieldValues() {
    // Given:
    final Map<String, Object> anOrder = new HashMap<>(AN_ORDER);
    anOrder.put("orderId", 1); // <-- int, rather than required long in ORDER_SCHEMA.

    final byte[] bytes = givenAvroSerialized(anOrder,
        "{"
            + "\"namespace\": \"kql\","
            + " \"name\": \"orders\","
            + " \"type\": \"record\","
            + " \"fields\": ["
            + "     {\"name\": \"orderTime\", \"type\": \"long\"},"
            + "     {\"name\": \"orderId\",  \"type\": \"int\"}," // <-- int, not long
            + "     {\"name\": \"itemId\", \"type\": \"string\"},"
            + "     {\"name\": \"orderUnits\", \"type\": \"double\"},"
            + "     {\"name\": \"arrayCol\", \"type\": {\"type\": \"array\", \"items\": "
            + "\"double\"}},"
            + "     {\"name\": \"mapCol\", \"type\": {\"type\": \"map\", \"values\": "
            + "\"double\"}}"
            + " ]"
            + "}");

    givenDeserializerForSchema(ORDER_SCHEMA);

    // When:
    final Object result = deserializer.deserialize(SOME_TOPIC, bytes);

    // Then:
    final Struct expectedResult = buildExpectedStruct(ORDER_SCHEMA, AN_ORDER);
    assertThat(result, is(expectedResult));
  }

  @Test
  public void shouldThrowIfNotARecord() {
    // Given:
    final byte[] bytes = givenAvroSerialized(10L, LONG_AVRO_SCHEMA);

    givenDeserializerForSchema(ORDER_SCHEMA);

    // Then:
    expectedException.expect(SerializationException.class);
    expectedException.expectCause(hasMessage(containsString(
        "Cannot deserialize type int64 as type struct")));

    // When:
    deserializer.deserialize(SOME_TOPIC, bytes);
  }

  @Test
  public void shouldThrowIfFieldValueCanNotBeCoercedToConnectSchemaType() {
    // Given:
    final Map<String, Object> anOrder = new HashMap<>(AN_ORDER);
    anOrder.put("orderId", true);

    final byte[] bytes = givenAvroSerialized(anOrder, "{"
        + "\"namespace\": \"kql\","
        + " \"name\": \"orders\","
        + " \"type\": \"record\","
        + " \"fields\": ["
        + "     {\"name\": \"orderTime\", \"type\": \"long\"},"
        + "     {\"name\": \"orderId\",  \"type\": \"boolean\"}," // <-- boolean is incompatible
        + "     {\"name\": \"itemId\", \"type\": \"string\"},"
        + "     {\"name\": \"orderUnits\", \"type\": \"double\"},"
        + "     {\"name\": \"arrayCol\", \"type\": {\"type\": \"array\", \"items\": "
        + "\"double\"}},"
        + "     {\"name\": \"mapCol\", \"type\": {\"type\": \"map\", \"values\": "
        + "\"double\"}}"
        + " ]"
        + "}");

    givenDeserializerForSchema(ORDER_SCHEMA);

    // Then:
    expectedException.expect(SerializationException.class);
    expectedException.expectCause(hasMessage(is(
        "Cannot deserialize type boolean as type int64 for path: ->ORDERID")));

    // When:
    deserializer.deserialize(SOME_TOPIC, bytes);
  }

  @Test
  public void shouldDeserializeIfThereAreRedundantFields() {
    // Given:
    final byte[] bytes = givenAvroSerialized(AN_ORDER, ORDER_AVRO_SCHEMA);

    final Schema reducedSchema = SchemaBuilder.struct()
        .field(ORDERTIME, Schema.OPTIONAL_INT64_SCHEMA)
        .field(ORDERID, Schema.OPTIONAL_INT64_SCHEMA)
        .field(ITEMID, Schema.OPTIONAL_STRING_SCHEMA)
        .field(ORDERUNITS, Schema.OPTIONAL_FLOAT64_SCHEMA)
        // <-- ARRAYCOL missing
        // <-- MAPCOL missing
        .build();

    givenDeserializerForSchema(reducedSchema);

    // When:
    final Object result = deserializer.deserialize(SOME_TOPIC, bytes);

    // Then:
    final Struct expectedResult = buildExpectedStruct(reducedSchema, AN_ORDER);
    assertThat(result, is(expectedResult));
  }

  @Test
  public void shouldDeserializeWithMissingFields() {
    // Given:
    final byte[] bytes = givenAvroSerialized(AN_ORDER, ORDER_AVRO_SCHEMA);

    final SchemaBuilder builder = SchemaBuilder.struct();
    ORDER_SCHEMA.fields().forEach(f -> builder.field(f.name(), f.schema()));

    final Schema expandedSchema = builder
        .field("EXTRA", Schema.OPTIONAL_INT64_SCHEMA)
        .build();

    givenDeserializerForSchema(expandedSchema);

    // When:
    final Object result = deserializer.deserialize(SOME_TOPIC, bytes);

    // Then:
    final Struct expectedResult = buildExpectedStruct(expandedSchema, AN_ORDER);
    assertThat(result, is(expectedResult));
  }

  @Test
  public void shouldDeserializeNullAsNull() {
    // Given:
    givenDeserializerForSchema(ORDER_SCHEMA);

    // When:
    final Object row = deserializer.deserialize("topic", null);

    // Then:
    assertThat(row, is(nullValue()));
  }

  @Test
  public void shouldTreatNullAsNull() {
    // Given:
    final ImmutableMap<String, Object> withNulls = ImmutableMap.<String, Object>builder()
        .put("arrayCol", Arrays.asList(10.0, null))
        .put("mapCol", Collections.singletonMap("key1", null))
        .build();

    final byte[] bytes = givenAvroSerialized(withNulls, ORDER_AVRO_SCHEMA);

    givenDeserializerForSchema(ORDER_SCHEMA);

    // When:
    final Object result = deserializer.deserialize(SOME_TOPIC, bytes);

    // Then:
    final Struct expectedResult = buildExpectedStruct(ORDER_SCHEMA, withNulls);
    assertThat(result, is(expectedResult));
  }

  @Test
  public void shouldDeserializedAvroBoolean() {
    // Given:
    givenDeserializerForSchema(Schema.OPTIONAL_BOOLEAN_SCHEMA);

    final byte[] bytes = givenAvroSerialized(true, BOOLEAN_AVRO_SCHEMA);

    // When:
    final Object result = deserializer.deserialize(SOME_TOPIC, bytes);

    // Then:
    assertThat(result, is(true));
  }

  @Test
  public void shouldThrowIfCanNotCoerceToBoolean() {
    // Given:
    givenDeserializerForSchema(Schema.OPTIONAL_BOOLEAN_SCHEMA);

    final byte[] bytes = givenAvroSerialized(10, LONG_AVRO_SCHEMA);

    // Then:
    expectedException.expect(SerializationException.class);
    expectedException.expectCause(hasMessage(containsString(
        "Cannot deserialize type int32 as type boolean")));

    // When:
    deserializer.deserialize(SOME_TOPIC, bytes);
  }

  @Test
  public void shouldDeserializedAvroInt() {
    // Given:
    givenDeserializerForSchema(Schema.OPTIONAL_INT32_SCHEMA);

    final byte[] bytes = givenAvroSerialized(42, INT_AVRO_SCHEMA);

    // When:
    final Object result = deserializer.deserialize(SOME_TOPIC, bytes);

    // Then:
    assertThat(result, is(42));
  }

  @Test
  public void shouldThrowIfCanNotCoerceToInt() {
    // Given:
    givenDeserializerForSchema(Schema.OPTIONAL_INT32_SCHEMA);

    final byte[] bytes = givenAvroSerialized(true, BOOLEAN_AVRO_SCHEMA);

    // Then:
    expectedException.expect(SerializationException.class);
    expectedException.expectCause(hasMessage(containsString(
        "Cannot deserialize type boolean as type int32")));

    // When:
    deserializer.deserialize(SOME_TOPIC, bytes);
  }

  @Test
  public void shouldDeserializedAvroBigInt() {
    // Given:
    givenDeserializerForSchema(Schema.OPTIONAL_INT64_SCHEMA);

    final Map<org.apache.avro.Schema, Object> validCoercions = ImmutableMap.of(
        INT_AVRO_SCHEMA, 42,
        LONG_AVRO_SCHEMA, 42L
    );

    validCoercions.forEach((schema, value) -> {

      final byte[] bytes = givenAvroSerialized(value, schema);

      // When:
      final Object result = deserializer.deserialize(SOME_TOPIC, bytes);

      // Then:
      assertThat(result, is(42L));
    });
  }

  @Test
  public void shouldThrowIfCanNotCoerceToBigInt() {
    // Given:
    givenDeserializerForSchema(Schema.OPTIONAL_INT64_SCHEMA);

    final byte[] bytes = givenAvroSerialized(true, BOOLEAN_AVRO_SCHEMA);

    // Then:
    expectedException.expect(SerializationException.class);
    expectedException.expectCause(hasMessage(containsString(
        "Cannot deserialize type boolean as type int64")));

    // When:
    deserializer.deserialize(SOME_TOPIC, bytes);
  }

  @Test
  public void shouldDeserializedAvroDouble() {
    // Given:
    givenDeserializerForSchema(Schema.OPTIONAL_FLOAT64_SCHEMA);

    final byte[] bytes = givenAvroSerialized(23.1, DOUBLE_AVRO_SCHEMA);

    // When:
    final Object result = deserializer.deserialize(SOME_TOPIC, bytes);

    // Then:
    assertThat(result, is(23.1));
  }

  @Test
  public void shouldThrowIfCanNotCoerceToDouble() {
    // Given:
    givenDeserializerForSchema(Schema.OPTIONAL_FLOAT64_SCHEMA);

    final byte[] bytes = givenAvroSerialized(true, BOOLEAN_AVRO_SCHEMA);

    // Then:
    expectedException.expect(SerializationException.class);
    expectedException.expectCause(hasMessage(containsString(
        "Cannot deserialize type boolean as type float64")));

    // When:
    deserializer.deserialize(SOME_TOPIC, bytes);
  }

  @Test
  public void shouldDeserializedAvroString() {
    // Given:
    givenDeserializerForSchema(Schema.OPTIONAL_STRING_SCHEMA);

    final Map<org.apache.avro.Schema, Object> validCoercions = ImmutableMap.of(
        BOOLEAN_AVRO_SCHEMA, false,
        INT_AVRO_SCHEMA, 41,
        LONG_AVRO_SCHEMA, 42L,
        DOUBLE_AVRO_SCHEMA, 43.5,
        STRING_AVRO_SCHEMA, "just a string"
    );

    validCoercions.forEach((schema, value) -> {

      final byte[] bytes = givenAvroSerialized(value, schema);

      // When:
      final Object result = deserializer.deserialize(SOME_TOPIC, bytes);

      // Then:
      assertThat(result, is(value.toString()));
    });
  }

  @Test
  public void shouldThrowIfCanNotCoerceToString() {
    // Given:
    givenDeserializerForSchema(Schema.OPTIONAL_STRING_SCHEMA);

    final byte[] bytes = givenAvroSerialized(AN_ORDER, ORDER_AVRO_SCHEMA);

    // Then:
    expectedException.expect(SerializationException.class);
    expectedException.expectCause(hasMessage(containsString(
        "Cannot deserialize type struct as type string")));

    // When:
    deserializer.deserialize(SOME_TOPIC, bytes);
  }

  @Test
  public void shouldDeserializedAvroArray() {
    // Given:
    givenDeserializerForSchema(SchemaBuilder
        .array(Schema.OPTIONAL_STRING_SCHEMA)
        .build()
    );

    final List<?> value = ImmutableList.of("look", "ma,", "an", "array!");

    final byte[] bytes = givenAvroSerialized(value, STRING_ARRAY_AVRO_SCHEMA);

    // When:
    final Object result = deserializer.deserialize(SOME_TOPIC, bytes);

    // Then:
    assertThat(result, is(value));
  }

  @Test
  public void shouldCoerceArrayElements() {
    // Given:
    givenDeserializerForSchema(SchemaBuilder
        .array(Schema.OPTIONAL_STRING_SCHEMA)
        .build()
    );

    final Map<String, List<?>> validCoercions = ImmutableMap.of(
        "{\"type\": \"array\", \"items\": \"boolean\"}", ImmutableList.of(true, false),
        "{\"type\": \"array\", \"items\": \"int\"}", ImmutableList.of(1, 2),
        "{\"type\": \"array\", \"items\": \"long\"}", ImmutableList.of(1L, 2L),
        "{\"type\": \"array\", \"items\": \"double\"}", ImmutableList.of(1.1, 2.2),
        "{\"type\": \"array\", \"items\": \"string\"}", ImmutableList.of("string", "elements!")
    );

    validCoercions.forEach((avroSchema, value) -> {

      final byte[] bytes = givenAvroSerialized(value, avroSchema);

      final List<String> expected = value.stream()
          .map(Object::toString)
          .collect(Collectors.toList());

      // When:
      final Object result = deserializer.deserialize(SOME_TOPIC, bytes);

      // Then:
      assertThat(result, is(expected));
    });
  }

  @Test
  public void shouldThrowIfNotAnArray() {
    // Given:
    givenDeserializerForSchema(SchemaBuilder
        .array(Schema.OPTIONAL_STRING_SCHEMA)
        .build()
    );

    final byte[] bytes = givenAvroSerialized(true, BOOLEAN_AVRO_SCHEMA);

    // Then:
    expectedException.expect(SerializationException.class);
    expectedException.expectCause(hasMessage(containsString(
        "Cannot deserialize type boolean as type array")));

    // When:
    deserializer.deserialize(SOME_TOPIC, bytes);
  }

  @Test
  public void shouldThrowIfCanNotCoerceArrayElement() {
    // Given:
    givenDeserializerForSchema(SchemaBuilder
        .array(Schema.OPTIONAL_INT32_SCHEMA)
        .build()
    );

    final List<?> value = ImmutableList.of("look", "ma,", "an", "array!");

    final byte[] bytes = givenAvroSerialized(value, STRING_ARRAY_AVRO_SCHEMA);

    // Then:
    expectedException.expect(SerializationException.class);
    expectedException.expectCause(hasMessage(containsString(
        "Cannot deserialize type string as type int32")));

    // When:
    deserializer.deserialize(SOME_TOPIC, bytes);
  }

  @Test
  public void shouldDeserializedAvroArrayOfMap() {
    // Given:
    givenDeserializerForSchema(SchemaBuilder
        .array(SchemaBuilder
            .map(SchemaBuilder.STRING_SCHEMA, Schema.OPTIONAL_INT64_SCHEMA)
            .optional()
            .build())
        .build()
    );

    final List<?> value = ImmutableList.of(ImmutableMap.of("a", 1L, "b", 2L));

    final byte[] bytes = givenAvroSerialized(value, ARRAY_OF_MAP_AVRO_SCHEMA);

    // When:
    final Object result = deserializer.deserialize(SOME_TOPIC, bytes);

    // Then:
    assertThat(result, is(value));
  }

  @Test
  public void shouldDeserializedAvroMap() {
    // Given:
    givenDeserializerForSchema(SchemaBuilder
        .map(Schema.OPTIONAL_STRING_SCHEMA, Schema.OPTIONAL_INT64_SCHEMA)
        .build()
    );

    final Map<org.apache.avro.Schema, Map<String, Number>> validCoercions = ImmutableMap.of(
        INT_MAP_AVRO_SCHEMA, ImmutableMap.of("a", 1, "b", 2),
        LONG_MAP_AVRO_SCHEMA, ImmutableMap.of("a", 1L, "b", 2L)
    );

    validCoercions.forEach((avroSchema, value) -> {

      final byte[] bytes = givenAvroSerialized(value, INT_MAP_AVRO_SCHEMA);

      final Map<String, Long> expected = value.entrySet().stream()
          .collect(Collectors.toMap(Entry::getKey, e -> e.getValue().longValue()));

      // When:
      final Object result = deserializer.deserialize(SOME_TOPIC, bytes);

      // Then:
      assertThat(result, is(expected));
    });
  }

  @Test
  public void shouldThrowIfNotAMap() {
    // Given:
    givenDeserializerForSchema(SchemaBuilder
        .map(Schema.OPTIONAL_STRING_SCHEMA, Schema.OPTIONAL_INT32_SCHEMA)
        .build()
    );

    final byte[] bytes = givenAvroSerialized(true, BOOLEAN_AVRO_SCHEMA);

    // Then:
    expectedException.expect(SerializationException.class);
    expectedException.expectCause(hasMessage(containsString(
        "Cannot deserialize type boolean as type map")));

    // When:
    deserializer.deserialize(SOME_TOPIC, bytes);
  }

  @Test
  public void shouldThrowIfCanNotCoerceMapValue() {
    // Given:
    givenDeserializerForSchema(SchemaBuilder
        .map(Schema.OPTIONAL_STRING_SCHEMA, Schema.OPTIONAL_BOOLEAN_SCHEMA)
        .build()
    );

    final Map<?, ?> value = ImmutableMap.of("a", 1, "b", 2);

    final byte[] bytes = givenAvroSerialized(value, INT_MAP_AVRO_SCHEMA);

    // Then:
    expectedException.expect(SerializationException.class);
    expectedException.expectCause(hasMessage(containsString(
        "Cannot deserialize type int32 as type boolean")));

    // When:
    deserializer.deserialize(SOME_TOPIC, bytes);
  }

  @Test
  public void shouldThrowOnMapSchemaWithNonStringKeys() {
    // Given:
    final Schema schema = SchemaBuilder
        .map(Schema.OPTIONAL_INT32_SCHEMA, Schema.INT32_SCHEMA)
        .build();

    // Then:
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("Avro only supports MAPs with STRING keys");

    // When:
    givenDeserializerForSchema(schema);
  }

  @Test
  public void shouldThrowOnNestedMapSchemaWithNonStringKeys() {
    // Given:
    final Schema schema = SchemaBuilder
        .struct()
        .field("f0", SchemaBuilder
            .map(Schema.OPTIONAL_INT32_SCHEMA, Schema.INT32_SCHEMA)
            .optional()
            .build())
        .build();

    // Then:
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("Avro only supports MAPs with STRING keys");

    // When:
    givenDeserializerForSchema(schema);
  }

  @Test
  public void shouldIncludeTopicNameInException() {
    // Given:
    givenDeserializerForSchema(Schema.OPTIONAL_INT64_SCHEMA);

    final byte[] bytes = givenAvroSerialized(true, BOOLEAN_AVRO_SCHEMA);

    // Then:
    expectedException.expectMessage(SOME_TOPIC);

    // When:
    deserializer.deserialize(SOME_TOPIC, bytes);
  }

  @Test
  public void shouldNotIncludeBadValueInExceptionAsThatWouldBeASecurityIssue() {
    // Given:
    givenDeserializerForSchema(Schema.OPTIONAL_INT64_SCHEMA);

    final byte[] bytes = givenAvroSerialized("personal info: do not log me", STRING_AVRO_SCHEMA);

    try {

      // When:
      deserializer.deserialize(SOME_TOPIC, bytes);

      fail("Invalid test: should throw");

    } catch (final Exception e) {
      assertThat(ExceptionUtils.getStackTrace(e), not(containsString("personal info")));
    }
  }

  @Test
  public void shouldDecodeSourceNameIntoFieldName() {
    // Given:
    final Schema schema = SchemaBuilder.struct()
        .field("source.field0", Schema.OPTIONAL_INT32_SCHEMA)
        .build();

    givenDeserializerForSchema(schema);

    final byte[] bytes = givenAvroSerialized(ImmutableMap.of("source_field0", 123),
        "{"
            + "\"namespace\": \"kql\","
            + "\"name\": \"aliased\","
            + "\"type\": \"record\","
            + "\"fields\": [{\"name\": \"source_field0\", \"type\": \"int\"}]"
            + "}");

    // When:
    final Object result = deserializer.deserialize(SOME_TOPIC, bytes);

    // Then:
    assertThat(result, is(new Struct(schema).put("source.field0", 123)));
  }

  @Test
  public void shouldDeserializeBooleanToBoolean() {
    shouldDeserializeTypeCorrectly(
        org.apache.avro.Schema.create(org.apache.avro.Schema.Type.BOOLEAN),
        false,
        Schema.OPTIONAL_BOOLEAN_SCHEMA
    );
    shouldDeserializeTypeCorrectly(
        org.apache.avro.Schema.create(org.apache.avro.Schema.Type.BOOLEAN),
        true,
        Schema.OPTIONAL_BOOLEAN_SCHEMA
    );
  }

  @Test
  public void shouldDeserializeIntToInt() {
    shouldDeserializeTypeCorrectly(
        org.apache.avro.Schema.create(org.apache.avro.Schema.Type.INT),
        123,
        Schema.OPTIONAL_INT32_SCHEMA);
  }

  @Test
  public void shouldDeserializeIntToBigint() {
    shouldDeserializeTypeCorrectly(
        org.apache.avro.Schema.create(org.apache.avro.Schema.Type.INT),
        123L,
        Schema.OPTIONAL_INT64_SCHEMA);
  }

  @Test
  public void shouldDeserializeLongToBigint() {
    shouldDeserializeTypeCorrectly(
        org.apache.avro.Schema.create(org.apache.avro.Schema.Type.LONG),
        ((long) Integer.MAX_VALUE) * 32,
        Schema.OPTIONAL_INT64_SCHEMA
    );
  }

  @Test
  public void shouldDeserializeFloatToDouble() {
    shouldDeserializeTypeCorrectly(
        org.apache.avro.Schema.create(org.apache.avro.Schema.Type.FLOAT),
        (float) 1.25,
        Schema.OPTIONAL_FLOAT64_SCHEMA,
        1.25
    );
  }

  @Test
  public void shouldDeserializeDoubleToDouble() {
    shouldDeserializeTypeCorrectly(
        org.apache.avro.Schema.create(org.apache.avro.Schema.Type.DOUBLE),
        1.234567890123456789,
        Schema.OPTIONAL_FLOAT64_SCHEMA
    );
  }

  @Test
  public void shouldDeserializeStringToString() {
    shouldDeserializeTypeCorrectly(
        org.apache.avro.Schema.create(org.apache.avro.Schema.Type.STRING),
        "foobarbizbazboz",
        Schema.OPTIONAL_STRING_SCHEMA
    );
  }

  @Test
  public void shouldDeserializeEnumToString() {
    final org.apache.avro.Schema enumSchema = org.apache.avro.Schema.createEnum(
        "enum",
        "doc",
        "namespace",
        ImmutableList.of("V0", "V1", "V2"));
    shouldDeserializeTypeCorrectly(
        enumSchema,
        new GenericData.EnumSymbol(enumSchema, "V1"),
        Schema.OPTIONAL_STRING_SCHEMA,
        "V1");

  }

  @Test
  public void shouldDeserializeRecordToStruct() {
    final org.apache.avro.Schema recordSchema = org.apache.avro.SchemaBuilder.record("record")
        .fields()
        .name("inner1")
        .type(org.apache.avro.Schema.create(org.apache.avro.Schema.Type.STRING))
        .noDefault()
        .name("inner2")
        .type(org.apache.avro.Schema.create(org.apache.avro.Schema.Type.INT))
        .noDefault()
        .endRecord();
    final GenericRecord record = new GenericData.Record(recordSchema);
    record.put("inner1", "foobar");
    record.put("inner2", 123456);

    final Schema structSchema = SchemaBuilder.struct()
        .field("inner1", Schema.OPTIONAL_STRING_SCHEMA)
        .field("inner2", Schema.OPTIONAL_INT32_SCHEMA)
        .optional()
        .build();
    final Struct struct = new Struct(structSchema);
    struct.put("inner1", "foobar");
    struct.put("inner2", 123456);

    shouldDeserializeTypeCorrectly(recordSchema, record, structSchema, struct);
  }

  @Test
  public void shouldDeserializeNullValue() {
    shouldDeserializeTypeCorrectly(
        org.apache.avro.SchemaBuilder.unionOf().nullType().and().intType().endUnion(),
        null,
        Schema.OPTIONAL_INT32_SCHEMA);
  }

  @Test
  public void shouldDeserializeArrayToArray() {
    shouldDeserializeTypeCorrectly(
        org.apache.avro.SchemaBuilder.array().items().intType(),
        ImmutableList.of(1, 2, 3, 4, 5, 6),
        SchemaBuilder.array(Schema.OPTIONAL_INT32_SCHEMA).optional().build()
    );
  }

  @Test
  public void shouldDeserializeMapToMap() {
    shouldDeserializeTypeCorrectly(
        org.apache.avro.SchemaBuilder.map().values().intType(),
        ImmutableMap.of("one", 1, "two", 2, "three", 3),
        SchemaBuilder.map(
            Schema.OPTIONAL_STRING_SCHEMA, Schema.OPTIONAL_INT32_SCHEMA
        ).optional().build()
    );
  }

  @Test
  public void shouldDeserializeDateToInteger() {
    shouldDeserializeTypeCorrectly(
        LogicalTypes.date().addToSchema(
            org.apache.avro.SchemaBuilder.builder().intType()),
        (int) ChronoUnit.DAYS.between(LocalDate.ofEpochDay(0), LocalDate.now()),
        Schema.OPTIONAL_INT32_SCHEMA
    );
  }

  @Test
  public void shouldDeserializeDateToBigint() {
    shouldDeserializeTypeCorrectly(
        LogicalTypes.date().addToSchema(
            org.apache.avro.SchemaBuilder.builder().intType()),
        ChronoUnit.DAYS.between(LocalDate.ofEpochDay(0), LocalDate.now()),
        Schema.OPTIONAL_INT64_SCHEMA
    );
  }

  @Test
  public void shouldDeserializeTimeMicrosToBigint() {
    shouldDeserializeTypeCorrectly(
        LogicalTypes.timeMicros().addToSchema(
            org.apache.avro.SchemaBuilder.builder().longType()),
        ChronoUnit.MICROS.between(
            LocalDateTime.of(LocalDate.now(), LocalTime.MIDNIGHT),
            LocalDateTime.now()),
        Schema.OPTIONAL_INT64_SCHEMA
    );
  }

  @Test
  public void shouldDeserializeTimeMillisToBigint() {
    shouldDeserializeTypeCorrectly(
        LogicalTypes.timeMillis().addToSchema(
            org.apache.avro.SchemaBuilder.builder().intType()),
        ChronoUnit.MILLIS.between(
            LocalDateTime.of(LocalDate.now(), LocalTime.MIDNIGHT),
            LocalDateTime.now()),
        Schema.OPTIONAL_INT64_SCHEMA
    );
  }

  @Test
  public void shouldDeserializeTimestampToInteger() {
    shouldDeserializeTypeCorrectly(
        LogicalTypes.timestampMicros().addToSchema(
            org.apache.avro.SchemaBuilder.builder().longType()),
        ChronoUnit.MICROS.between(
            LocalDateTime.of(LocalDate.ofEpochDay(0), LocalTime.MIDNIGHT),
            LocalDateTime.now()),
        Schema.OPTIONAL_INT64_SCHEMA
    );
  }

  @Test
  public void shouldDeserializeTimestampToBigint() {
    shouldDeserializeTypeCorrectly(
        LogicalTypes.timestampMillis().addToSchema(
            org.apache.avro.SchemaBuilder.builder().longType()),
        ChronoUnit.MILLIS.between(
            LocalDateTime.of(LocalDate.ofEpochDay(0), LocalTime.MIDNIGHT),
            LocalDateTime.now()),
        Schema.OPTIONAL_INT64_SCHEMA
    );
  }

  @Test
  public void shouldDeserializeUnionToStruct() {
    final org.apache.avro.Schema avroSchema = org.apache.avro.SchemaBuilder.unionOf()
        .intType().and()
        .stringType()
        .endUnion();
    final Schema ksqlSchema = SchemaBuilder.struct()
        .field("int", Schema.OPTIONAL_INT32_SCHEMA)
        .field("string", Schema.OPTIONAL_STRING_SCHEMA)
        .optional()
        .build();
    final Struct ksqlValue = new Struct(ksqlSchema).put("string", "foobar");
    shouldDeserializeTypeCorrectly(avroSchema, "foobar", ksqlSchema, ksqlValue);
  }

  private void shouldDeserializeConnectTypeCorrectly(
      final Schema connectSchema,
      final Object connectValue,
      final Schema ksqlSchema,
      final Object ksqlValue
  ) {
    final Schema connectRecordSchema = SchemaBuilder.struct()
        .field("field0", connectSchema)
        .build();
    final Struct connectRecord = new Struct(connectRecordSchema);
    connectRecord.put("field0", connectValue);

    final byte[] bytes = serializeAsBinaryAvro("topic", connectRecordSchema, connectRecord);

    final Schema ksqlRecordSchema = SchemaBuilder.struct()
        .field("field0", ksqlSchema)
        .optional()
        .build();

    givenDeserializerForSchema(ksqlRecordSchema);

    final Struct row = (Struct) deserializer.deserialize("topic", bytes);

    assertThat(row.schema().field("field0").schema(), is(ksqlSchema));
    assertThat(row.get("field0"), is(ksqlValue));
  }

  @Test
  public void shouldDeserializeConnectInt8ToInteger() {
    shouldDeserializeConnectTypeCorrectly(
        Schema.INT8_SCHEMA,
        (byte) 32,
        Schema.OPTIONAL_INT32_SCHEMA,
        32
    );
  }

  @Test
  public void shouldDeserializeConnectInt16ToInteger() {
    shouldDeserializeConnectTypeCorrectly(
        Schema.INT16_SCHEMA,
        (short) 16384,
        Schema.OPTIONAL_INT32_SCHEMA,
        16384
    );
  }

  @Test
  public void shouldDeserializeConnectInt8ToBigint() {
    shouldDeserializeConnectTypeCorrectly(
        Schema.INT8_SCHEMA,
        (byte) 32,
        Schema.OPTIONAL_INT64_SCHEMA,
        32L
    );
  }

  @Test
  public void shouldDeserializeConnectInt16ToBigint() {
    shouldDeserializeConnectTypeCorrectly(
        Schema.INT16_SCHEMA,
        (short) 16384,
        Schema.OPTIONAL_INT64_SCHEMA,
        16384L
    );
  }

  @Test
  public void shouldDeserializeConnectMapWithInt8Key() {
    shouldDeserializeConnectTypeCorrectly(
        SchemaBuilder.map(Schema.INT8_SCHEMA, Schema.INT32_SCHEMA).optional().build(),
        ImmutableMap.of((byte) 1, 10, (byte) 2, 20, (byte) 3, 30),
        SchemaBuilder.map(
            Schema.OPTIONAL_STRING_SCHEMA, Schema.OPTIONAL_INT32_SCHEMA
        ).optional().build(),
        ImmutableMap.of("1", 10, "2", 20, "3", 30)
    );
  }

  @Test
  public void shouldDeserializeConnectMapWithInt16Key() {
    shouldDeserializeConnectTypeCorrectly(
        SchemaBuilder.map(Schema.INT16_SCHEMA, Schema.INT32_SCHEMA).optional().build(),
        ImmutableMap.of((short) 1, 10, (short) 2, 20, (short) 3, 30),
        SchemaBuilder.map(
            Schema.OPTIONAL_STRING_SCHEMA, Schema.OPTIONAL_INT32_SCHEMA
        ).optional().build(),
        ImmutableMap.of("1", 10, "2", 20, "3", 30)
    );
  }

  @Test
  public void shouldDeserializeConnectMapWithInt32Key() {
    shouldDeserializeConnectTypeCorrectly(
        SchemaBuilder.map(Schema.INT32_SCHEMA, Schema.INT32_SCHEMA).optional().build(),
        ImmutableMap.of(1, 10, 2, 20, 3, 30),
        SchemaBuilder.map(
            Schema.OPTIONAL_STRING_SCHEMA, Schema.OPTIONAL_INT32_SCHEMA
        ).optional().build(),
        ImmutableMap.of("1", 10, "2", 20, "3", 30)
    );
  }

  @Test
  public void shouldDeserializeConnectMapWithInt64Key() {
    shouldDeserializeConnectTypeCorrectly(
        SchemaBuilder.map(Schema.INT64_SCHEMA, Schema.INT32_SCHEMA).optional().build(),
        ImmutableMap.of( 1L, 10, 2L, 20, 3L, 30),
        SchemaBuilder.map(
            Schema.OPTIONAL_STRING_SCHEMA, Schema.OPTIONAL_INT32_SCHEMA
        ).optional().build(),
        ImmutableMap.of("1", 10, "2", 20, "3", 30)
    );
  }

  @Test
  public void shouldDeserializeConnectMapWithBooleanKey() {
    shouldDeserializeConnectTypeCorrectly(
        SchemaBuilder.map(Schema.BOOLEAN_SCHEMA, Schema.INT32_SCHEMA).optional().build(),
        ImmutableMap.of( true, 10, false, 20),
        SchemaBuilder.map(
            Schema.OPTIONAL_STRING_SCHEMA, Schema.OPTIONAL_INT32_SCHEMA
        ).optional().build(),
        ImmutableMap.of("true", 10, "false", 20)
    );
  }

  private byte[] serializeAsBinaryAvro(
      final String topicName,
      final Schema schema,
      final Object value
  ) {
    return converter.fromConnectData(topicName, schema, value);
  }

  private Struct serializeDeserializeAvroRecord(
      final Schema schema,
      final String topicName,
      final GenericRecord avroRecord
  ) {
    final byte[] bytes = serializer.serialize(topicName, avroRecord);

    givenDeserializerForSchema(schema);

    return (Struct) deserializer.deserialize(topicName, bytes);
  }

  private void shouldDeserializeTypeCorrectly(
      final org.apache.avro.Schema avroSchema,
      final Object avroValue,
      final Schema ksqlSchema
  ) {
    shouldDeserializeTypeCorrectly(avroSchema, avroValue, ksqlSchema, avroValue);
  }

  private void shouldDeserializeTypeCorrectly(
      final org.apache.avro.Schema avroSchema,
      final Object avroValue,
      final Schema ksqlSchema,
      final Object ksqlValue
  ) {
    final org.apache.avro.Schema avroRecordSchema = org.apache.avro.SchemaBuilder.record("test_row")
        .fields()
        .name("field0")
        .type(avroSchema)
        .noDefault()
        .endRecord();
    final Schema ksqlRecordSchema = SchemaBuilder.struct().field("field0", ksqlSchema).build();

    final GenericRecord avroRecord = new GenericData.Record(avroRecordSchema);
    avroRecord.put("field0", avroValue);

    final Struct row = serializeDeserializeAvroRecord(
        ksqlRecordSchema, "test-topic", avroRecord);

    assertThat(row.schema(), is(ksqlRecordSchema));
    assertThat(row.get("field0"), equalTo(ksqlValue));
  }

  private void givenDeserializerForSchema(final Schema schema) {
    final KsqlAvroSerdeFactory serdeFactory = new KsqlAvroSerdeFactory(
        KsqlConstants.DEFAULT_AVRO_SCHEMA_FULL_NAME);

    deserializer = serdeFactory.createDeserializer(
        PersistenceSchema.of((ConnectSchema) schema),
        KSQL_CONFIG,
        () -> schemaRegistryClient,
        recordLogger
    );

    deserializer.configure(Collections.emptyMap(), false);
  }

  private static Struct buildExpectedStruct(
      final Schema connectSchema,
      final Map<String, Object> fieldVaues
  ) {
    final Struct expectedResult = new Struct(connectSchema);

    fieldVaues.entrySet().stream()
        .filter(e -> expectedResult.schema().field(e.getKey().toUpperCase()) != null)
        .forEach(e -> expectedResult.put(e.getKey().toUpperCase(), e.getValue()));

    return expectedResult;
  }

  private byte[] givenAvroSerialized(
      final Object value,
      final String avroSchema
  ) {
    return givenAvroSerialized(value, parseAvroSchema(avroSchema));
  }

  private byte[] givenAvroSerialized(
      final Object value,
      final org.apache.avro.Schema avroSchema
  ) {
    final Object avroValue = givenAvroValue(avroSchema, value);
    return serializer.serialize(SOME_TOPIC, avroValue);
  }

  private static org.apache.avro.Schema parseAvroSchema(final String avroSchema) {
    return new org.apache.avro.Schema.Parser().parse(avroSchema);
  }

  private static GenericData.Record givenAvroRecord(
      final org.apache.avro.Schema avroSchema,
      final Map<String, ?> fieldValues
  ) {
    final GenericData.Record record = new GenericData.Record(avroSchema);
    fieldValues.forEach(record::put);
    return record;
  }

  @SuppressWarnings("unchecked")
  private static Object givenAvroValue(
      final org.apache.avro.Schema avroSchema,
      final Object value
  ) {
    switch (avroSchema.getType()) {
      case RECORD:
        return givenAvroRecord(avroSchema, (Map<String, ?>) value);
      case ARRAY:
        return new GenericData.Array<>(avroSchema, (Collection<?>) value);
      case MAP:
        return new AvroTestUtil.GenericMap<>(avroSchema, (Map<?, ?>) value);
      default:
        return value;
    }
  }
}
