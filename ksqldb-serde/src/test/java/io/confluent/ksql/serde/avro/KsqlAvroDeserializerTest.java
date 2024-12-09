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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.kafka.connect.data.Schema.OPTIONAL_INT64_SCHEMA;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.*;
import static org.junit.internal.matchers.ThrowableMessageMatcher.hasMessage;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.confluent.connect.avro.AvroConverter;
import io.confluent.connect.avro.AvroDataConfig;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.ksql.serde.connect.ConnectProperties;
import io.confluent.ksql.util.DecimalUtil;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
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
import org.apache.avro.Conversions.DecimalConversion;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@SuppressWarnings("rawtypes")
@RunWith(MockitoJUnitRunner.class)
public class KsqlAvroDeserializerTest {

  private static final org.apache.avro.Schema BOOLEAN_AVRO_SCHEMA =
      parseAvroSchema("{\"type\": \"boolean\"}");

  private static final org.apache.avro.Schema OPTIONAL_BOOLEAN_AVRO_SCHEMA =
      parseAvroSchema("[\"null\", \"boolean\"]");

  private static final org.apache.avro.Schema BYTES_AVRO_SCHEMA =
      parseAvroSchema("{\"type\": \"bytes\"}");

  private static final org.apache.avro.Schema OPTIONAL_BYTES_AVRO_SCHEMA =
      parseAvroSchema("[\"null\", \"bytes\"]");

  private static final org.apache.avro.Schema INT_AVRO_SCHEMA =
      parseAvroSchema("{\"type\": \"int\"}");

  private static final org.apache.avro.Schema OPTIONAL_INT_AVRO_SCHEMA =
      parseAvroSchema("[\"null\", \"int\"]");

  private static final org.apache.avro.Schema LONG_AVRO_SCHEMA =
      parseAvroSchema("{\"type\": \"long\"}");

  private static final org.apache.avro.Schema OPTIONAL_LONG_AVRO_SCHEMA =
      parseAvroSchema("[\"null\", \"long\"]");

  private static final org.apache.avro.Schema DOUBLE_AVRO_SCHEMA =
      parseAvroSchema("{\"type\": \"double\"}");

  private static final org.apache.avro.Schema OPTIONAL_DOUBLE_AVRO_SCHEMA =
      parseAvroSchema("[\"null\", \"double\"]");

  private static final org.apache.avro.Schema DECIMAL_AVRO_SCHEMA =
      parseAvroSchema("{"
          + "\"type\": \"bytes\","
          + "\"logicalType\": \"decimal\","
          + "\"precision\": 4,"
          + "\"scale\": 2"
          + "}");

  private static final org.apache.avro.Schema STRING_AVRO_SCHEMA =
      parseAvroSchema("{\"type\": \"string\"}");

  private static final org.apache.avro.Schema OPTIONAL_STRING_AVRO_SCHEMA =
      parseAvroSchema("[\"null\", \"string\"]");

  private static final org.apache.avro.Schema STRING_ARRAY_AVRO_SCHEMA =
      parseAvroSchema("{\"type\": \"array\", \"items\": \"string\"}");

  private static final org.apache.avro.Schema OPTIONAL_ARRAY_AVRO_SCHEMA =
      parseAvroSchema("[\"null\", {\"type\": \"array\", \"items\": \"string\"}]");

  private static final org.apache.avro.Schema INT_MAP_AVRO_SCHEMA =
      parseAvroSchema("{\"type\": \"map\", \"values\": \"int\"}");

  private static final org.apache.avro.Schema LONG_MAP_AVRO_SCHEMA =
      parseAvroSchema("{\"type\": \"map\", \"values\": \"long\"}");

  private static final org.apache.avro.Schema OPTIONAL_INT_MAP_AVRO_SCHEMA =
      parseAvroSchema("[\"null\", {\"type\": \"map\", \"values\": \"int\"}]");

  private static final org.apache.avro.Schema OPTIONAL_KEY_MAP_AVRO_SCHEMA =
      parseAvroSchema("{"
          + "\"type\":\"array\","
          + "\"items\":{"
          + "\"type\":\"record\","
          + "\"name\":\"KsqlDataSourceSchema\","
          + "\"namespace\":\"io.confluent.ksql.avro_schemas\","
          + "\"fields\":["
          + "{\"name\":\"key\",\"type\":[\"null\",\"string\"],\"default\":null},"
          + "{\"name\":\"value\",\"type\":[\"null\",\"int\"],\"default\":null}],"
          + "\"connect.internal.type\":\"MapEntry\"}}");

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

  private SchemaRegistryClient schemaRegistryClient;
  private AvroConverter converter;
  private KafkaAvroSerializer serializer;

  @Before
  public void setUp() {
    final ImmutableMap<String, Object> configs = ImmutableMap.of(
        AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS, true,
        AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, ""
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

    final Deserializer<Struct> deserializer =
        givenDeserializerForSchema((ConnectSchema) ORDER_SCHEMA, Struct.class);

    // When:
    final Object result = deserializer.deserialize(SOME_TOPIC, bytes);

    // Then:
    final Struct expectedResult = buildExpectedStruct(ORDER_SCHEMA, AN_ORDER);
    assertThat(result, is(expectedResult));
  }

  @Test
  public void shouldDeserializeAvroRecordWithNoConnectMeta() {
    // Given:
    final byte[] bytes = givenAvroSerializedWithNoConnectMeta(AN_ORDER, ORDER_AVRO_SCHEMA);

    final Deserializer<Struct> deserializer =
        givenDeserializerForSchema((ConnectSchema) ORDER_SCHEMA, Struct.class);

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

    final Deserializer<Struct> deserializer =
        givenDeserializerForSchema((ConnectSchema) ORDER_SCHEMA, Struct.class);

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

    final Deserializer<Struct> deserializer =
        givenDeserializerForSchema((ConnectSchema) ORDER_SCHEMA, Struct.class);

    // When:
    final Exception e = assertThrows(
        SerializationException.class,
        () -> deserializer.deserialize(SOME_TOPIC, bytes)
    );

    // Then:
    assertThat(e.getCause(), (hasMessage(containsString(
        "Cannot deserialize type int64 as type struct"))));
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

    final Deserializer<Struct> deserializer =
        givenDeserializerForSchema((ConnectSchema) ORDER_SCHEMA, Struct.class);

    // When:
    final Exception e = assertThrows(
        SerializationException.class,
        () -> deserializer.deserialize(SOME_TOPIC, bytes)
    );

    // Then:
    assertThat(e.getCause(), (hasMessage(is(
        "Cannot deserialize type boolean as type int64 for path: ->ORDERID"))));
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

    final Deserializer<Struct> deserializer =
        givenDeserializerForSchema((ConnectSchema) reducedSchema, Struct.class);

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

    final Deserializer<Struct> deserializer =
        givenDeserializerForSchema((ConnectSchema) expandedSchema, Struct.class);

    // When:
    final Object result = deserializer.deserialize(SOME_TOPIC, bytes);

    // Then:
    final Struct expectedResult = buildExpectedStruct(expandedSchema, AN_ORDER);
    assertThat(result, is(expectedResult));
  }

  @Test
  public void shouldDeserializeNullAsNull() {
    // Given:
    final Deserializer<Struct> deserializer =
        givenDeserializerForSchema((ConnectSchema) ORDER_SCHEMA, Struct.class);

    // When:
    final Object row = deserializer.deserialize("topic", null);

    // Then:
    assertThat(row, is(nullValue()));
  }

  @Test
  public void shouldTreatNullFieldAsNull() {
    // Given:
    final ImmutableMap<String, Object> withNulls = ImmutableMap.<String, Object>builder()
        .put("arrayCol", Arrays.asList(10.0, null))
        .put("mapCol", Collections.singletonMap("key1", null))
        .build();

    final byte[] bytes = givenAvroSerialized(withNulls, ORDER_AVRO_SCHEMA);

    final Deserializer<Struct> deserializer =
        givenDeserializerForSchema((ConnectSchema) ORDER_SCHEMA, Struct.class);

    // When:
    final Object result = deserializer.deserialize(SOME_TOPIC, bytes);

    // Then:
    final Struct expectedResult = buildExpectedStruct(ORDER_SCHEMA, withNulls);
    assertThat(result, is(expectedResult));
  }

  @Test
  public void shouldDeserializeAvroBoolean() {
    // Given:
    final Deserializer<Boolean> deserializer =
        givenDeserializerForSchema((ConnectSchema) Schema.OPTIONAL_BOOLEAN_SCHEMA, Boolean.class);

    final byte[] bytes = givenAvroSerialized(true, BOOLEAN_AVRO_SCHEMA);

    // When:
    final Object result = deserializer.deserialize(SOME_TOPIC, bytes);

    // Then:
    assertThat(result, is(true));
  }

  @Test
  public void shouldDeserializeAvroOptionalBoolean() {
    // Given:
    final Deserializer<Boolean> deserializer =
        givenDeserializerForSchema((ConnectSchema) Schema.OPTIONAL_BOOLEAN_SCHEMA, Boolean.class);

    final byte[] bytes = givenAvroSerialized(true, OPTIONAL_BOOLEAN_AVRO_SCHEMA);

    // When:
    final Object result = deserializer.deserialize(SOME_TOPIC, bytes);

    // Then:
    assertThat(result, is(true));
  }

  @Test
  public void shouldThrowIfCanNotCoerceToBoolean() {
    // Given:
    final Deserializer<Boolean> deserializer =
        givenDeserializerForSchema((ConnectSchema) Schema.OPTIONAL_BOOLEAN_SCHEMA, Boolean.class);

    final byte[] bytes = givenAvroSerialized(10, LONG_AVRO_SCHEMA);

    // When:
    final Exception e = assertThrows(
        SerializationException.class,
        () -> deserializer.deserialize(SOME_TOPIC, bytes)
    );

    // Then:
    assertThat(e.getCause(), (hasMessage(containsString(
        "Cannot deserialize type int32 as type boolean"))));
  }

  @Test
  public void shouldDeserializeAvroInt() {
    // Given:
    final Deserializer<Integer> deserializer =
        givenDeserializerForSchema((ConnectSchema) Schema.OPTIONAL_INT32_SCHEMA, Integer.class);

    final byte[] bytes = givenAvroSerialized(42, INT_AVRO_SCHEMA);

    // When:
    final Object result = deserializer.deserialize(SOME_TOPIC, bytes);

    // Then:
    assertThat(result, is(42));
  }

  @Test
  public void shouldDeserializeAvroOptionalInt() {
    // Given:
    final Deserializer<Integer> deserializer =
        givenDeserializerForSchema((ConnectSchema) Schema.OPTIONAL_INT32_SCHEMA, Integer.class);

    final byte[] bytes = givenAvroSerialized(42, OPTIONAL_INT_AVRO_SCHEMA);

    // When:
    final Object result = deserializer.deserialize(SOME_TOPIC, bytes);

    // Then:
    assertThat(result, is(42));
  }

  @Test
  public void shouldDeserializeConnectToInt() {
    /*
    Note: Connect stores additional metadata in the schema when serializing other types,
    e.g. Int8, to Avro, which only supports int.
     */

    final Map<byte[], Integer> validCoercions = ImmutableMap
        .<byte[], Integer>builder()
        .put(givenConnectSerialized((byte) 40, Schema.INT8_SCHEMA), 40)
        .put(givenConnectSerialized((byte) 41, Schema.OPTIONAL_INT8_SCHEMA), 41)
        .put(givenConnectSerialized((short) 42, Schema.INT16_SCHEMA), 42)
        .put(givenConnectSerialized((short) 43, Schema.OPTIONAL_INT16_SCHEMA), 43)
        .put(givenConnectSerialized(44, Schema.INT32_SCHEMA), 44)
        .put(givenConnectSerialized(45, Schema.OPTIONAL_INT32_SCHEMA), 45)
        .build();

    validCoercions.forEach((bytes, expcted) -> {

      // Given:
      final Deserializer<Integer> deserializer =
          givenDeserializerForSchema((ConnectSchema) Schema.OPTIONAL_INT32_SCHEMA, Integer.class);

      // When:
      final Object result = deserializer.deserialize(SOME_TOPIC, bytes);

      // Then:
      assertThat(result, is(expcted));
    });
  }

  @Test
  public void shouldThrowIfCanNotCoerceToInt() {
    // Given:
    final Deserializer<Integer> deserializer =
        givenDeserializerForSchema((ConnectSchema) Schema.OPTIONAL_INT32_SCHEMA, Integer.class);

    final byte[] bytes = givenAvroSerialized(true, BOOLEAN_AVRO_SCHEMA);

    // When:
    final Exception e = assertThrows(
        SerializationException.class,
        () -> deserializer.deserialize(SOME_TOPIC, bytes)
    );

    // Then:
    assertThat(e.getCause(), (hasMessage(containsString(
        "Cannot deserialize type boolean as type int32"))));
  }

  @Test
  public void shouldDeserializeAvroBigInt() {
    // Given:
    final Deserializer<Long> deserializer =
        givenDeserializerForSchema((ConnectSchema) Schema.OPTIONAL_INT64_SCHEMA, Long.class);

    final Map<org.apache.avro.Schema, Object> validCoercions = ImmutableMap.of(
        INT_AVRO_SCHEMA, 42,
        OPTIONAL_INT_AVRO_SCHEMA, 42,
        LONG_AVRO_SCHEMA, 42L,
        OPTIONAL_LONG_AVRO_SCHEMA, 42L
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
  public void shouldDeserializeConnectToBigInt() {
    /*
    Note: Connect stores additional metadata in the schema when serializing other types,
    e.g. Int8, to Avro which only supports int and long.
     */

    // Given:
    final Deserializer<Long> deserializer =
        givenDeserializerForSchema((ConnectSchema) Schema.OPTIONAL_INT64_SCHEMA, Long.class);

    final Map<byte[], Long> validCoercions = ImmutableMap
        .<byte[], Long>builder()
        .put(givenConnectSerialized((byte) 40, Schema.INT8_SCHEMA), 40L)
        .put(givenConnectSerialized((byte) 41, Schema.OPTIONAL_INT8_SCHEMA), 41L)
        .put(givenConnectSerialized((short) 42, Schema.INT16_SCHEMA), 42L)
        .put(givenConnectSerialized((short) 43, Schema.OPTIONAL_INT16_SCHEMA), 43L)
        .put(givenConnectSerialized(44, Schema.INT32_SCHEMA), 44L)
        .put(givenConnectSerialized(45, Schema.OPTIONAL_INT32_SCHEMA), 45L)
        .put(givenConnectSerialized(46L, Schema.INT64_SCHEMA), 46L)
        .put(givenConnectSerialized(47L, Schema.OPTIONAL_INT64_SCHEMA), 47L)
        .build();

    validCoercions.forEach((bytes, expcted) -> {

      // When:
      final Object result = deserializer.deserialize(SOME_TOPIC, bytes);

      // Then:
      assertThat(result, is(expcted));
    });
  }

  @Test
  public void shouldThrowIfCanNotCoerceToBigInt() {
    // Given:
    final Deserializer<Long> deserializer =
        givenDeserializerForSchema((ConnectSchema) Schema.OPTIONAL_INT64_SCHEMA, Long.class);

    final byte[] bytes = givenAvroSerialized(true, BOOLEAN_AVRO_SCHEMA);

    // When:
    final Exception e = assertThrows(
        SerializationException.class,
        () -> deserializer.deserialize(SOME_TOPIC, bytes)
    );

    // Then:
    assertThat(e.getCause(), (hasMessage(containsString(
        "Cannot deserialize type boolean as type int64"))));
  }

  @Test
  public void shouldDeserializeAvroDouble() {
    // Given:
    final Deserializer<Double> deserializer =
        givenDeserializerForSchema((ConnectSchema) Schema.OPTIONAL_FLOAT64_SCHEMA, Double.class);

    final Map<org.apache.avro.Schema, Object> validCoercions = ImmutableMap
        .<org.apache.avro.Schema, Object>builder()
        .put(DOUBLE_AVRO_SCHEMA, 23.1)
        .put(OPTIONAL_DOUBLE_AVRO_SCHEMA, 25.4)
        .build();

    validCoercions.forEach((schema, value) -> {

      final byte[] bytes = givenAvroSerialized(value, schema);

      // When:
      final Object result = deserializer.deserialize(SOME_TOPIC, bytes);

      // Then:
      assertThat(result, is(value));
    });
  }

  @Test
  public void shouldDeserializeAvroDecimal() {
    // Given:
    final Deserializer<BigDecimal> deserializer =
        givenDeserializerForSchema((ConnectSchema) DecimalUtil.builder(4, 2).build(),
            BigDecimal.class);
    final BigDecimal value = new BigDecimal("12.34");
    final byte[] bytes = givenConnectSerialized(value, DecimalUtil.builder(4, 2).build());

    // When:
    final Object result = deserializer.deserialize(SOME_TOPIC, bytes);

    // Then:
    assertThat(result, is(value));
  }

  @Test
  public void shouldDeserializeAvroDecimalWithNoConnectMeta() {
    // Given:
    final Deserializer<BigDecimal> deserializer =
        givenDeserializerForSchema((ConnectSchema) DecimalUtil.builder(4, 2).build(),
            BigDecimal.class);
    final BigDecimal value = new BigDecimal("12.34");
    final byte[] bytes = givenConnectSerializedWithNoConnectMeta(value, DecimalUtil.builder(4, 2).build());

    // When:
    final Object result = deserializer.deserialize(SOME_TOPIC, bytes);

    // Then:
    assertThat(result, is(value));
  }

  @Test
  public void shouldDeserializeConnectToDouble() {
    /*
    Note: Connect stores additional metadata in the schema when serializing other types,
    e.g. Float32, to Avro, which only supports double.
     */

    // Given:
    final Deserializer<Double> deserializer =
        givenDeserializerForSchema((ConnectSchema) Schema.OPTIONAL_FLOAT64_SCHEMA, Double.class);

    final Map<byte[], Double> validCoercions = ImmutableMap
        .<byte[], Double>builder()
        .put(givenConnectSerialized(10.1f, Schema.FLOAT32_SCHEMA), (double) 10.1f)
        .put(givenConnectSerialized(20.3f, Schema.OPTIONAL_FLOAT32_SCHEMA), (double) 20.3f)
        .put(givenConnectSerialized(30.4, Schema.FLOAT64_SCHEMA), 30.4)
        .put(givenConnectSerialized(40.5, Schema.OPTIONAL_FLOAT64_SCHEMA), 40.5)
        .build();

    validCoercions.forEach((bytes, expcted) -> {

      // When:
      final Object result = deserializer.deserialize(SOME_TOPIC, bytes);

      // Then:
      assertThat(result, is(expcted));
    });
  }

  @Test
  public void shouldThrowIfCanNotCoerceToDouble() {
    // Given:
    final Deserializer<Double> deserializer =
        givenDeserializerForSchema((ConnectSchema) Schema.OPTIONAL_FLOAT64_SCHEMA, Double.class);

    final byte[] bytes = givenAvroSerialized(true, BOOLEAN_AVRO_SCHEMA);

    // When:
    final Exception e = assertThrows(
        SerializationException.class,
        () -> deserializer.deserialize(SOME_TOPIC, bytes)
    );

    // Then:
    assertThat(e.getCause(), (hasMessage(containsString(
        "Cannot deserialize type boolean as type float64"))));
  }

  @Test
  public void shouldDeserializeAvroString() {
    // Given:
    final Deserializer<String> deserializer =
        givenDeserializerForSchema((ConnectSchema) Schema.OPTIONAL_STRING_SCHEMA, String.class);

    final Map<org.apache.avro.Schema, Object> validCoercions = ImmutableMap
        .<org.apache.avro.Schema, Object>builder()
        .put(BOOLEAN_AVRO_SCHEMA, false)
        .put(OPTIONAL_BOOLEAN_AVRO_SCHEMA, true)
        .put(INT_AVRO_SCHEMA, 40)
        .put(OPTIONAL_INT_AVRO_SCHEMA, 41)
        .put(LONG_AVRO_SCHEMA, 42L)
        .put(OPTIONAL_LONG_AVRO_SCHEMA, 43L)
        .put(DOUBLE_AVRO_SCHEMA, 44.5)
        .put(OPTIONAL_DOUBLE_AVRO_SCHEMA, 45.5)
        .put(STRING_AVRO_SCHEMA, "just a string")
        .put(OPTIONAL_STRING_AVRO_SCHEMA, "just another string")
        .build();

    validCoercions.forEach((schema, value) -> {

      final byte[] bytes = givenAvroSerialized(value, schema);

      // When:
      final Object result = deserializer.deserialize(SOME_TOPIC, bytes);

      // Then:
      assertThat(result, is(value.toString()));
    });
  }

  @Test
  public void shouldDeserializeConnectToString() {
    // Given:
    final Deserializer<String> deserializer =
        givenDeserializerForSchema((ConnectSchema) Schema.OPTIONAL_STRING_SCHEMA, String.class);

    final Map<byte[], Object> validCoercions = ImmutableMap
        .<byte[], Object>builder()
        .put(givenConnectSerialized(true, Schema.BOOLEAN_SCHEMA), true)
        .put(givenConnectSerialized(false, Schema.OPTIONAL_BOOLEAN_SCHEMA), false)
        .put(givenConnectSerialized((byte) 40, Schema.INT8_SCHEMA), 40)
        .put(givenConnectSerialized((byte) 41, Schema.OPTIONAL_INT8_SCHEMA), 41)
        .put(givenConnectSerialized((short) 42, Schema.INT16_SCHEMA), 42)
        .put(givenConnectSerialized((short) 43, Schema.OPTIONAL_INT16_SCHEMA), 43)
        .put(givenConnectSerialized(44, Schema.INT32_SCHEMA), 44)
        .put(givenConnectSerialized(45, Schema.OPTIONAL_INT32_SCHEMA), 45L)
        .put(givenConnectSerialized(46L, Schema.INT64_SCHEMA), 46L)
        .put(givenConnectSerialized(47L, Schema.OPTIONAL_INT64_SCHEMA), 47L)
        .put(givenConnectSerialized(10.1f, Schema.FLOAT32_SCHEMA), 10.1)
        .put(givenConnectSerialized(20.3f, Schema.OPTIONAL_FLOAT32_SCHEMA), 20.3)
        .put(givenConnectSerialized(30.4, Schema.FLOAT64_SCHEMA), 30.4)
        .put(givenConnectSerialized(40.5, Schema.OPTIONAL_FLOAT64_SCHEMA), 40.5)
        .build();

    validCoercions.forEach((bytes, expcted) -> {

      // When:
      final Object result = deserializer.deserialize(SOME_TOPIC, bytes);

      // Then:
      assertThat(result, is(expcted.toString()));
    });
  }

  @Test
  public void shouldThrowIfCanNotCoerceToString() {
    // Given:
    final Deserializer<String> deserializer =
        givenDeserializerForSchema((ConnectSchema) Schema.OPTIONAL_STRING_SCHEMA, String.class);

    final byte[] bytes = givenAvroSerialized(AN_ORDER, ORDER_AVRO_SCHEMA);

    // When:
    final Exception e = assertThrows(
        SerializationException.class,
        () -> deserializer.deserialize(SOME_TOPIC, bytes)
    );

    // Then:
    assertThat(e.getCause(), (hasMessage(containsString(
        "Cannot deserialize type struct as type string"))));
  }

  @Test
  public void shouldDeserializeAvroArray() {
    // Given:
    final Deserializer<List> deserializer =
        givenDeserializerForSchema((ConnectSchema) SchemaBuilder
            .array(Schema.OPTIONAL_STRING_SCHEMA)
            .build(), List.class);

    final List<?> value = ImmutableList.of("look", "ma,", "an", "array!");

    final byte[] bytes = givenAvroSerialized(value, STRING_ARRAY_AVRO_SCHEMA);

    // When:
    final Object result = deserializer.deserialize(SOME_TOPIC, bytes);

    // Then:
    assertThat(result, is(value));
  }

  @Test
  public void shouldDeserializeAvroOptionalArray() {
    // Given:
    final Deserializer<List> deserializer =
        givenDeserializerForSchema((ConnectSchema) SchemaBuilder
            .array(Schema.OPTIONAL_STRING_SCHEMA)
            .build(), List.class);

    final List<?> value = ImmutableList.of("look", "ma,", "an", "array!");

    final byte[] bytes = givenAvroSerialized(value, OPTIONAL_ARRAY_AVRO_SCHEMA);

    // When:
    final Object result = deserializer.deserialize(SOME_TOPIC, bytes);

    // Then:
    assertThat(result, is(value));
  }

  @Test
  public void shouldCoerceArrayElements() {
    // Given:
    final Deserializer<List> deserializer =
        givenDeserializerForSchema((ConnectSchema) SchemaBuilder
            .array(Schema.OPTIONAL_STRING_SCHEMA)
            .build(), List.class);

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
    final Deserializer<List> deserializer =
        givenDeserializerForSchema((ConnectSchema) SchemaBuilder
            .array(Schema.OPTIONAL_STRING_SCHEMA)
            .build(), List.class);

    final byte[] bytes = givenAvroSerialized(true, BOOLEAN_AVRO_SCHEMA);

    // When:
    final Exception e = assertThrows(
        SerializationException.class,
        () -> deserializer.deserialize(SOME_TOPIC, bytes)
    );

    // Then:
    assertThat(e.getCause(), (hasMessage(containsString(
        "Cannot deserialize type boolean as type array"))));
  }

  @Test
  public void shouldThrowIfCanNotCoerceArrayElement() {
    // Given:
    final Deserializer<List> deserializer =
        givenDeserializerForSchema((ConnectSchema) SchemaBuilder
            .array(Schema.OPTIONAL_INT32_SCHEMA)
            .build(), List.class);

    final List<?> value = ImmutableList.of("look", "ma,", "an", "array!");

    final byte[] bytes = givenAvroSerialized(value, STRING_ARRAY_AVRO_SCHEMA);

    // When:
    final Exception e = assertThrows(
        SerializationException.class,
        () -> deserializer.deserialize(SOME_TOPIC, bytes)
    );

    // Then:
    assertThat(e.getCause(), (hasMessage(containsString(
        "Cannot deserialize type string as type int32"))));
  }

  @Test
  public void shouldDeserializeAvroArrayOfMap() {
    // Given:
    final Deserializer<List> deserializer =
        givenDeserializerForSchema((ConnectSchema) SchemaBuilder
            .array(SchemaBuilder
                .map(SchemaBuilder.STRING_SCHEMA, Schema.OPTIONAL_INT64_SCHEMA)
                .optional()
                .build())
            .build(), List.class);

    final List<?> value = ImmutableList.of(ImmutableMap.of("a", 1L, "b", 2L));

    final byte[] bytes = givenAvroSerialized(value, ARRAY_OF_MAP_AVRO_SCHEMA);

    // When:
    final Object result = deserializer.deserialize(SOME_TOPIC, bytes);

    // Then:
    assertThat(result, is(value));
  }

  @Test
  public void shouldDeserializeAvroMap() {
    // Given:
    final Deserializer<Map> deserializer =
        givenDeserializerForSchema((ConnectSchema) SchemaBuilder
            .map(Schema.OPTIONAL_STRING_SCHEMA, Schema.OPTIONAL_INT64_SCHEMA)
            .build(), Map.class);

    final Map<org.apache.avro.Schema, Map<String, Number>> validCoercions = ImmutableMap.of(
        INT_MAP_AVRO_SCHEMA, ImmutableMap.of("a", 1, "b", 2),
        OPTIONAL_INT_MAP_AVRO_SCHEMA, ImmutableMap.of("a", 1, "b", 2),
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
  public void shouldDeserializeAvroMapWithNoConnectMeta() {
    // Given:
    final Deserializer<Map> deserializer =
        givenDeserializerForSchema((ConnectSchema) SchemaBuilder
            .map(Schema.OPTIONAL_STRING_SCHEMA, Schema.OPTIONAL_INT64_SCHEMA)
            .build(), Map.class);

    final Map<org.apache.avro.Schema, Map<String, Number>> validCoercions = ImmutableMap.of(
        INT_MAP_AVRO_SCHEMA, ImmutableMap.of("a", 1, "b", 2),
        OPTIONAL_INT_MAP_AVRO_SCHEMA, ImmutableMap.of("a", 1, "b", 2),
        LONG_MAP_AVRO_SCHEMA, ImmutableMap.of("a", 1L, "b", 2L)
    );

    validCoercions.forEach((avroSchema, value) -> {

      final byte[] bytes = givenAvroSerializedWithNoConnectMeta(value, avroSchema);

      final Map<String, Long> expected = value.entrySet().stream()
          .collect(Collectors.toMap(Entry::getKey, e -> e.getValue().longValue()));

      // When:
      final Object result = deserializer.deserialize(SOME_TOPIC, bytes);

      // Then:
      assertThat(result, is(expected));
    });
  }

  @Test
  public void shouldDeserializeConnectMapWithOptionalKeys() {
    /*
    Note: Connect serializers maps with optional keys are an array of key-value pairs.
     */

    // Given:
    final Deserializer<Map> deserializer =
        givenDeserializerForSchema((ConnectSchema) SchemaBuilder
            .map(Schema.STRING_SCHEMA, Schema.OPTIONAL_INT64_SCHEMA)
            .build(), Map.class);

    final List<?> mapEntries = buildEntriesForMapWithOptionalKey(null, null, "a", 1);

    final byte[] bytes = givenAvroSerialized(mapEntries, OPTIONAL_KEY_MAP_AVRO_SCHEMA);

    // When:
    final Object result = deserializer.deserialize(SOME_TOPIC, bytes);

    // Then:
    final Map<String, Long> expected = new HashMap<>();
    expected.put(null, null);
    expected.put("a", 1L);
    assertThat(result, is(expected));
  }

  @Test
  public void shouldThrowIfNotAMap() {
    // Given:
    final Deserializer<Map> deserializer =
        givenDeserializerForSchema((ConnectSchema) SchemaBuilder
            .map(Schema.OPTIONAL_STRING_SCHEMA, Schema.OPTIONAL_INT32_SCHEMA)
            .build(), Map.class);

    final byte[] bytes = givenAvroSerialized(true, BOOLEAN_AVRO_SCHEMA);

    // When:
    final Exception e = assertThrows(
        SerializationException.class,
        () -> deserializer.deserialize(SOME_TOPIC, bytes)
    );

    // Then:
    assertThat(e.getCause(), (hasMessage(containsString(
        "Cannot deserialize type boolean as type map"))));
  }

  @Test
  public void shouldThrowIfCanNotCoerceMapValue() {
    // Given:
    final Deserializer<Map> deserializer =
        givenDeserializerForSchema((ConnectSchema) SchemaBuilder
            .map(Schema.OPTIONAL_STRING_SCHEMA, Schema.OPTIONAL_BOOLEAN_SCHEMA)
            .build(), Map.class);

    final Map<?, ?> value = ImmutableMap.of("a", 1, "b", 2);

    final byte[] bytes = givenAvroSerialized(value, INT_MAP_AVRO_SCHEMA);

    // When:
    final Exception e = assertThrows(
        SerializationException.class,
        () -> deserializer.deserialize(SOME_TOPIC, bytes)
    );

    // Then:
    assertThat(e.getCause(), (hasMessage(containsString(
        "Cannot deserialize type int32 as type boolean"))));
  }

  @Test
  public void shouldThrowOnMapSchemaWithNonStringKeys() {
    // Given:
    final Schema schema = SchemaBuilder
        .map(Schema.OPTIONAL_INT32_SCHEMA, Schema.INT32_SCHEMA)
        .build();

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> givenDeserializerForSchema((ConnectSchema) schema, Struct.class)
    );

    // Then:
    assertThat(e.getMessage(), containsString("Avro only supports MAPs with STRING keys"));
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

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> givenDeserializerForSchema((ConnectSchema) schema, Struct.class)
    );

    // Then:
    assertThat(e.getMessage(), containsString("Avro only supports MAPs with STRING keys"));
  }

  @Test
  public void shouldIncludeTopicNameInException() {
    // Given:
    final Deserializer<Long> deserializer =
        givenDeserializerForSchema((ConnectSchema) OPTIONAL_INT64_SCHEMA, Long.class);

    final byte[] bytes = givenAvroSerialized(true, BOOLEAN_AVRO_SCHEMA);

    // When:
    final Exception e = assertThrows(
        Exception.class,
        () -> deserializer.deserialize(SOME_TOPIC, bytes)
    );

    // Then:
    assertThat(e.getMessage(), containsString(SOME_TOPIC));
  }

  @Test
  public void shouldNotIncludeBadValueInExceptionAsThatWouldBeASecurityIssue() {
    // Given:
    final Deserializer<Long> deserializer =
        givenDeserializerForSchema((ConnectSchema) Schema.OPTIONAL_INT64_SCHEMA, Long.class);

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

    final Deserializer<Struct> deserializer =
        givenDeserializerForSchema((ConnectSchema) schema, Struct.class);

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
  public void shouldDeserializeBooleanFieldToBoolean() {
    shouldDeserializeFieldTypeCorrectly(
        org.apache.avro.Schema.create(org.apache.avro.Schema.Type.BOOLEAN),
        false,
        Schema.OPTIONAL_BOOLEAN_SCHEMA
    );
    shouldDeserializeFieldTypeCorrectly(
        org.apache.avro.Schema.create(org.apache.avro.Schema.Type.BOOLEAN),
        true,
        Schema.OPTIONAL_BOOLEAN_SCHEMA
    );
  }

  @Test
  public void shouldDeserializeIntFieldToInt() {
    shouldDeserializeFieldTypeCorrectly(
        org.apache.avro.Schema.create(org.apache.avro.Schema.Type.INT),
        123,
        Schema.OPTIONAL_INT32_SCHEMA);
  }

  @Test
  public void shouldDeserializeIntFieldToBigint() {
    shouldDeserializeFieldTypeCorrectly(
        org.apache.avro.Schema.create(org.apache.avro.Schema.Type.INT),
        123L,
        Schema.OPTIONAL_INT64_SCHEMA);
  }

  @Test
  public void shouldDeserializeLongFieldToBigint() {
    shouldDeserializeFieldTypeCorrectly(
        org.apache.avro.Schema.create(org.apache.avro.Schema.Type.LONG),
        ((long) Integer.MAX_VALUE) * 32,
        Schema.OPTIONAL_INT64_SCHEMA
    );
  }

  @Test
  public void shouldDeserializeFloatFieldToDouble() {
    shouldDeserializeFieldTypeCorrectly(
        org.apache.avro.Schema.create(org.apache.avro.Schema.Type.FLOAT),
        (float) 1.25,
        Schema.OPTIONAL_FLOAT64_SCHEMA,
        1.25
    );
  }

  @Test
  public void shouldDeserializeDoubleFieldToDouble() {
    shouldDeserializeFieldTypeCorrectly(
        org.apache.avro.Schema.create(org.apache.avro.Schema.Type.DOUBLE),
        1.234567890123456789,
        Schema.OPTIONAL_FLOAT64_SCHEMA
    );
  }

  @Test
  public void shouldDeserializeStringFieldToString() {
    shouldDeserializeFieldTypeCorrectly(
        org.apache.avro.Schema.create(org.apache.avro.Schema.Type.STRING),
        "foobarbizbazboz",
        Schema.OPTIONAL_STRING_SCHEMA
    );
  }

  @Test
  public void shouldDeserializeEnumFieldToString() {
    final org.apache.avro.Schema enumSchema = org.apache.avro.Schema.createEnum(
        "enum",
        "doc",
        "namespace",
        ImmutableList.of("V0", "V1", "V2"));
    shouldDeserializeFieldTypeCorrectly(
        enumSchema,
        new GenericData.EnumSymbol(enumSchema, "V1"),
        Schema.OPTIONAL_STRING_SCHEMA,
        "V1");
  }

  @Test
  public void shouldDeserializeRecordFieldToStruct() {
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

    shouldDeserializeFieldTypeCorrectly(recordSchema, record, structSchema, struct);
  }

  @Test
  public void shouldDeserializeNullFieldValue() {
    shouldDeserializeFieldTypeCorrectly(
        org.apache.avro.SchemaBuilder.unionOf().nullType().and().intType().endUnion(),
        null,
        Schema.OPTIONAL_INT32_SCHEMA);
  }

  @Test
  public void shouldDeserializeArrayFieldToArray() {
    shouldDeserializeFieldTypeCorrectly(
        org.apache.avro.SchemaBuilder.array().items().intType(),
        ImmutableList.of(1, 2, 3, 4, 5, 6),
        SchemaBuilder.array(Schema.OPTIONAL_INT32_SCHEMA).optional().build()
    );
  }

  @Test
  public void shouldDeserializeMapFieldToMap() {
    shouldDeserializeFieldTypeCorrectly(
        org.apache.avro.SchemaBuilder.map().values().intType(),
        ImmutableMap.of("one", 1, "two", 2, "three", 3),
        SchemaBuilder.map(
            Schema.OPTIONAL_STRING_SCHEMA, Schema.OPTIONAL_INT32_SCHEMA
        ).optional().build()
    );
  }

  @Test
  public void shouldDeserializeDateFieldToInteger() {
    shouldDeserializeFieldTypeCorrectly(
        LogicalTypes.date().addToSchema(
            org.apache.avro.SchemaBuilder.builder().intType()),
        (int) ChronoUnit.DAYS.between(LocalDate.ofEpochDay(0), LocalDate.now()),
        Schema.OPTIONAL_INT32_SCHEMA
    );
  }

  @Test
  public void shouldDeserializeDateFieldToBigint() {
    shouldDeserializeFieldTypeCorrectly(
        LogicalTypes.date().addToSchema(
            org.apache.avro.SchemaBuilder.builder().intType()),
        ChronoUnit.DAYS.between(LocalDate.ofEpochDay(0), LocalDate.now()),
        Schema.OPTIONAL_INT64_SCHEMA
    );
  }

  @Test
  public void shouldDeserializeTimeMicrosFieldToBigint() {
    shouldDeserializeFieldTypeCorrectly(
        LogicalTypes.timeMicros().addToSchema(
            org.apache.avro.SchemaBuilder.builder().longType()),
        ChronoUnit.MICROS.between(
            LocalDateTime.of(LocalDate.now(), LocalTime.MIDNIGHT),
            LocalDateTime.now()),
        Schema.OPTIONAL_INT64_SCHEMA
    );
  }

  @Test
  public void shouldDeserializeTimeMillisFieldToBigint() {
    shouldDeserializeFieldTypeCorrectly(
        LogicalTypes.timeMillis().addToSchema(
            org.apache.avro.SchemaBuilder.builder().intType()),
        ChronoUnit.MILLIS.between(
            LocalDateTime.of(LocalDate.now(), LocalTime.MIDNIGHT),
            LocalDateTime.now()),
        Schema.OPTIONAL_INT64_SCHEMA
    );
  }

  @Test
  public void shouldDeserializeTimeFieldToTime() {
    shouldDeserializeFieldTypeCorrectly(
        LogicalTypes.timeMillis().addToSchema(
            org.apache.avro.SchemaBuilder.builder().intType()),
        500,
        Time.SCHEMA,
        new java.sql.Time(500)
    );
  }

  @Test
  public void shouldDeserializeDateFieldToDate() {
    shouldDeserializeFieldTypeCorrectly(
        LogicalTypes.date().addToSchema(
            org.apache.avro.SchemaBuilder.builder().intType()),
        10,
        Date.SCHEMA,
        new java.sql.Date(864000000L)
    );
  }

  @Test
  public void shouldDeserializeTimestampFieldToInteger() {
    shouldDeserializeFieldTypeCorrectly(
        LogicalTypes.timestampMicros().addToSchema(
            org.apache.avro.SchemaBuilder.builder().longType()),
        ChronoUnit.MICROS.between(
            LocalDateTime.of(LocalDate.ofEpochDay(0), LocalTime.MIDNIGHT),
            LocalDateTime.now()),
        Schema.OPTIONAL_INT64_SCHEMA
    );
  }

  @Test
  public void shouldDeserializeTimestampFieldToTimestamp() {
    shouldDeserializeFieldTypeCorrectly(
        LogicalTypes.timestampMillis().addToSchema(
            org.apache.avro.SchemaBuilder.builder().longType()),
        500L,
        Timestamp.SCHEMA,
        new java.sql.Timestamp(500)
    );
  }

  @Test
  public void shouldDeserializeTimestampFieldToLong() {
    shouldDeserializeFieldTypeCorrectly(
        LogicalTypes.timestampMillis().addToSchema(
            org.apache.avro.SchemaBuilder.builder().longType()),
        500L,
        OPTIONAL_INT64_SCHEMA,
        500L
    );
  }

  @Test
  public void shouldDeserializeAvroBytes() {
    // Given:
    final Deserializer<ByteBuffer> deserializer =
        givenDeserializerForSchema((ConnectSchema) Schema.OPTIONAL_BYTES_SCHEMA, ByteBuffer.class);

    final Map<org.apache.avro.Schema, Object> validCoercions = ImmutableMap
        .<org.apache.avro.Schema, Object>builder()
        .put(BYTES_AVRO_SCHEMA, "abc".getBytes(UTF_8))
        .put(OPTIONAL_BYTES_AVRO_SCHEMA, "def".getBytes(UTF_8))
        .build();

    validCoercions.forEach((schema, value) -> {

      final byte[] bytes = givenAvroSerialized(value, schema);

      // When:
      final Object result = deserializer.deserialize(SOME_TOPIC, bytes);

      // Then:
      assertThat(result, is(ByteBuffer.wrap((byte[]) value)));
    });
  }

  @Test
  public void shouldDeserializeUnionFieldToStruct() {
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
    shouldDeserializeFieldTypeCorrectly(avroSchema, "foobar", ksqlSchema, ksqlValue);
  }

  private void shouldDeserializeConnectFieldTypeCorrectly(
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

    final Deserializer<Struct> deserializer =
        givenDeserializerForSchema((ConnectSchema) ksqlRecordSchema, Struct.class);

    final Struct row = deserializer.deserialize("topic", bytes);

    assertThat(row.schema().field("field0").schema(), is(ksqlSchema));
    assertThat(row.get("field0"), is(ksqlValue));
  }

  @Test
  public void shouldDeserializeConnectInt8ToInteger() {
    shouldDeserializeConnectFieldTypeCorrectly(
        Schema.INT8_SCHEMA,
        (byte) 32,
        Schema.OPTIONAL_INT32_SCHEMA,
        32
    );
  }

  @Test
  public void shouldDeserializeConnectInt16ToInteger() {
    shouldDeserializeConnectFieldTypeCorrectly(
        Schema.INT16_SCHEMA,
        (short) 16384,
        Schema.OPTIONAL_INT32_SCHEMA,
        16384
    );
  }

  @Test
  public void shouldDeserializeConnectInt8ToBigint() {
    shouldDeserializeConnectFieldTypeCorrectly(
        Schema.INT8_SCHEMA,
        (byte) 32,
        Schema.OPTIONAL_INT64_SCHEMA,
        32L
    );
  }

  @Test
  public void shouldDeserializeConnectInt16ToBigint() {
    shouldDeserializeConnectFieldTypeCorrectly(
        Schema.INT16_SCHEMA,
        (short) 16384,
        Schema.OPTIONAL_INT64_SCHEMA,
        16384L
    );
  }

  @Test
  public void shouldDeserializeConnectMapWithInt8Key() {
    shouldDeserializeConnectFieldTypeCorrectly(
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
    shouldDeserializeConnectFieldTypeCorrectly(
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
    shouldDeserializeConnectFieldTypeCorrectly(
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
    shouldDeserializeConnectFieldTypeCorrectly(
        SchemaBuilder.map(Schema.INT64_SCHEMA, Schema.INT32_SCHEMA).optional().build(),
        ImmutableMap.of(1L, 10, 2L, 20, 3L, 30),
        SchemaBuilder.map(
            Schema.OPTIONAL_STRING_SCHEMA, Schema.OPTIONAL_INT32_SCHEMA
        ).optional().build(),
        ImmutableMap.of("1", 10, "2", 20, "3", 30)
    );
  }

  @Test
  public void shouldDeserializeConnectMapWithBooleanKey() {
    shouldDeserializeConnectFieldTypeCorrectly(
        SchemaBuilder.map(Schema.BOOLEAN_SCHEMA, Schema.INT32_SCHEMA).optional().build(),
        ImmutableMap.of(true, 10, false, 20),
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

    final Deserializer<Struct> deserializer =
        givenDeserializerForSchema((ConnectSchema) schema, Struct.class);

    return deserializer.deserialize(topicName, bytes);
  }

  private void shouldDeserializeFieldTypeCorrectly(
      final org.apache.avro.Schema avroSchema,
      final Object avroValue,
      final Schema ksqlSchema
  ) {
    shouldDeserializeFieldTypeCorrectly(avroSchema, avroValue, ksqlSchema, avroValue);
  }

  private void shouldDeserializeFieldTypeCorrectly(
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

  private <T> Deserializer<T> givenDeserializerForSchema(
      final ConnectSchema schema,
      final Class<T> targetType
  ) {
    final ImmutableMap<String, String> formatProperties = ImmutableMap.of(
        ConnectProperties.FULL_SCHEMA_NAME, AvroProperties.DEFAULT_AVRO_SCHEMA_FULL_NAME);
    final KsqlAvroSerdeFactory serdeFactory = new KsqlAvroSerdeFactory(
        new AvroProperties(formatProperties));

    final Deserializer<T> deserializer = serdeFactory.createSerde(
        schema,
        KSQL_CONFIG,
        () -> schemaRegistryClient,
        targetType,
        false).deserializer();

    deserializer.configure(Collections.emptyMap(), false);

    return deserializer;
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
  private byte[] givenAvroSerializedWithNoConnectMeta(
      final Object value,
      final org.apache.avro.Schema avroSchema
  ) {
    final ImmutableMap<String, Object> configs = ImmutableMap.of(
        AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS, true,
        AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "",
        AvroDataConfig.CONNECT_META_DATA_CONFIG, false
    );
    final Object avroValue = givenAvroValue(avroSchema, value);
    KafkaAvroSerializer serializer = new KafkaAvroSerializer(schemaRegistryClient, configs);
    return serializer.serialize(SOME_TOPIC, avroValue);
  }

  private byte[] givenConnectSerialized(
      final Object value,
      final Schema connectSchema
  ) {
    return serializeAsBinaryAvro(SOME_TOPIC, connectSchema, value);
  }
  private byte[] givenConnectSerializedWithNoConnectMeta(
      final Object value,
      final Schema connectSchema
  ) {
    final ImmutableMap<String, Object> configs = ImmutableMap.of(
        AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS, true,
        AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "",
        AvroDataConfig.CONNECT_META_DATA_CONFIG, false
    );

    AvroConverter connectConverter = new AvroConverter(schemaRegistryClient);
    connectConverter.configure(configs, false);
    return connectConverter.fromConnectData(SOME_TOPIC, connectSchema, value);
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
    final org.apache.avro.Schema schema = avroSchema.getType() == Type.UNION
        ? avroSchema.getTypes().get(1)
        : avroSchema;

    switch (schema.getType()) {
      case BYTES:
        if (value instanceof BigDecimal) {
          final BigDecimal decimal = (BigDecimal) value;
          return new DecimalConversion().toBytes(
              decimal,
              avroSchema,
              LogicalTypes.decimal(decimal.precision(), decimal.scale())).array();
        } else {
          return value;
        }
      case RECORD:
        return givenAvroRecord(schema, (Map<String, ?>) value);
      case ARRAY:
        return new GenericData.Array<>(schema, (Collection<?>) value);
      case MAP:
        return new AvroTestUtil.GenericMap<>(schema, (Map<?, ?>) value);
      default:
        return value;
    }
  }

  private static List<?> buildEntriesForMapWithOptionalKey(
      final String k1, final Integer v1,
      final String k2, final Integer v2
  ) {

    final org.apache.avro.Schema entrySchema = AvroTestUtil
        .connectOptionalKeyMapEntrySchema("bob", INT_AVRO_SCHEMA);

    final Record e1 = new Record(entrySchema);
    e1.put("key", k1);
    e1.put("value", v1);

    final Record e2 = new Record(entrySchema);
    e2.put("key", k2);
    e2.put("value", v2);

    return ImmutableList.of(e1, e2);
  }
}
