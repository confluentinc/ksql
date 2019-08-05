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

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.junit.internal.matchers.ThrowableMessageMatcher.hasMessage;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.confluent.connect.avro.AvroData;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.ksql.schema.ksql.PersistenceSchema;
import io.confluent.ksql.util.DecimalUtil;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlConstants;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.apache.avro.Conversions.DecimalConversion;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema.Type;
import org.apache.avro.SchemaBuilder.FieldAssembler;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.Array;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.util.Utf8;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.hamcrest.CoreMatchers;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@SuppressWarnings("SameParameterValue")
@RunWith(MockitoJUnitRunner.class)
public class KsqlAvroSerializerTest {

  private static final org.apache.avro.Schema ORDER_AVRO_SCHEMA = parseAvroSchema("{"
      + "\"namespace\": \"io.confluent.ksql.avro_schemas\","
      + "\"name\": \"KsqlDataSourceSchema\","
      + "\"type\": \"record\","
      + "\"fields\": ["
      + " {\"name\": \"ORDERTIME\", \"type\": [\"null\",\"long\"], \"default\": null},"
      + " {\"name\": \"ORDERID\",  \"type\": [\"null\",\"long\"], \"default\": null},"
      + " {\"name\": \"ITEMID\", \"type\": [\"null\",\"string\"], \"default\": null},"
      + " {\"name\": \"ORDERUNITS\", \"type\": [\"null\",\"double\"], \"default\": null},"
      + " {\"name\": \"ARRAYCOL\", \"type\": [\"null\",{\"type\": \"array\", \"items\": [\"null\",\"double\"]}], \"default\": null},"
      + " {\"name\": \"MAPCOL\", \"type\": [\"null\",{\"type\": \"map\", \"values\": [\"null\",\"double\"]}], \"default\": null}"
      + " ]"
      + "}");

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

  private static final org.apache.avro.Schema BOOLEAN_ARRAY_AVRO_SCHEMA =
      parseAvroSchema("{\"type\": \"array\", \"items\": [\"null\", \"boolean\"]}]");

  private static final org.apache.avro.Schema REQUIRED_KEY_MAP_AVRO_SCHEMA =
      parseAvroSchema("{\"type\": \"map\", \"values\": [\"null\", \"int\"]}");

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

  private static final org.apache.avro.Schema DECIMAL_SCHEMA =
      parseAvroSchema(
          "{"
          + "\"type\": \"bytes\","
          + "\"logicalType\": \"decimal\","
          + "\"precision\": 4,"
          + "\"scale\": 2"
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
          .array(Schema.OPTIONAL_FLOAT64_SCHEMA).optional().build())
      .field(MAPCOL, SchemaBuilder
          .map(Schema.STRING_SCHEMA, Schema.OPTIONAL_FLOAT64_SCHEMA).optional().build())
      .optional()
      .build();

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  private final SchemaRegistryClient schemaRegistryClient = new MockSchemaRegistryClient();

  private KsqlConfig ksqlConfig = new KsqlConfig(ImmutableMap.of());

  private Serializer<Object> serializer;
  private Deserializer<Object> deserializer;
  private Struct orderStruct;
  private GenericRecord avroOrder;

  @Before
  public void setup() {
    final ImmutableMap<String, Object> configs = ImmutableMap.of(
        AbstractKafkaAvroSerDeConfig.AUTO_REGISTER_SCHEMAS, true,
        AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, ""
    );

    deserializer = new KafkaAvroDeserializer(schemaRegistryClient, configs);

    orderStruct = new Struct(ORDER_SCHEMA)
        .put(ORDERTIME, 1511897796092L)
        .put(ORDERID, 1L)
        .put(ITEMID, "item_1")
        .put(ORDERUNITS, 10.0)
        .put(ARRAYCOL, Collections.singletonList(100.0))
        .put(MAPCOL, Collections.singletonMap("key1", 100.0));

    avroOrder = new GenericData.Record(ORDER_AVRO_SCHEMA);
    avroOrder.put(ORDERTIME, 1511897796092L);
    avroOrder.put(ORDERID, 1L);
    avroOrder.put(ITEMID, "item_1");
    avroOrder.put(ORDERUNITS, 10.0);
    avroOrder.put(ARRAYCOL, ImmutableList.of(100.0));
    avroOrder.put(MAPCOL, ImmutableMap.of(new Utf8("key1"), 100.0));
  }

  @Test
  public void shouldSerializeNullValue() {
    // Given:
    givenSerializerForSchema(ORDER_SCHEMA);

    // When:
    final byte[] serializedRow = serializer.serialize(SOME_TOPIC, null);

    // Then:
    assertThat(serializedRow, is(nullValue()));
  }

  @Test
  public void shouldSerializeStructCorrectly() {
    // Given:
    givenSerializerForSchema(ORDER_SCHEMA);

    // When:
    final byte[] serializedRow = serializer.serialize(SOME_TOPIC, orderStruct);

    // Then:
    final GenericRecord deserialized = deserialize(serializedRow);
    assertThat(deserialized, is(avroOrder));
    assertThat(avroSchemaStoredInSchemaRegistry(), is(ORDER_AVRO_SCHEMA));
  }

  @Test
  public void shouldThrowIfNotStruct() {
    // Given:
    givenSerializerForSchema(ORDER_SCHEMA);

    // Then:
    expectedException.expect(SerializationException.class);
    expectedException.expectCause(hasMessage(containsString(
        "java.lang.Integer cannot be cast to org.apache.kafka.connect.data.Struct")));

    // When:
    serializer.serialize(SOME_TOPIC, 10);
  }

  @Test
  public void shouldHandleNestedStruct() {
    // Given:
    final Schema schema = SchemaBuilder
        .struct()
        .field("nested", ORDER_SCHEMA)
        .build();

    final Struct value = new Struct(schema)
        .put("nested", orderStruct);

    givenSerializerForSchema(schema);

    // When:
    final byte[] serializedRow = serializer.serialize(SOME_TOPIC, value);

    // Then:
    final org.apache.avro.Schema nestedSchema = rename(ORDER_AVRO_SCHEMA,
        "KsqlDataSourceSchema_nested");

    avroOrder = new GenericData.Record(nestedSchema);
    avroOrder.put(ORDERTIME, 1511897796092L);
    avroOrder.put(ORDERID, 1L);
    avroOrder.put(ITEMID, "item_1");
    avroOrder.put(ORDERUNITS, 10.0);
    avroOrder.put(ARRAYCOL, ImmutableList.of(100.0));
    avroOrder.put(MAPCOL, ImmutableMap.of(new Utf8("key1"), 100.0));

    final GenericRecord avroValue =
        new GenericData.Record(recordSchema(ImmutableMap.of("nested", nestedSchema)));
    avroValue.put("nested", avroOrder);

    final GenericRecord deserialized = deserialize(serializedRow);
    assertThat(deserialized, is(avroValue));
  }

  @Test
  public void shouldSerializeBoolean() {
    // Given:
    givenSerializerForSchema(Schema.OPTIONAL_BOOLEAN_SCHEMA);

    // When:
    final byte[] bytes = serializer.serialize(SOME_TOPIC, true);

    // Then:
    // Then:
    assertThat(deserialize(bytes), is(true));
    assertThat(avroSchemaStoredInSchemaRegistry(), is(BOOLEAN_AVRO_SCHEMA));
  }

  @Test
  public void shouldThrowIfNotBoolean() {
    // Given:
    givenSerializerForSchema(Schema.OPTIONAL_BOOLEAN_SCHEMA);

    // Then:
    expectedException.expect(SerializationException.class);
    expectedException.expectCause(hasMessage(is(
        "Invalid type for BOOLEAN: class java.lang.Integer")));

    // When:
    serializer.serialize(SOME_TOPIC, 10);
  }

  @Test
  public void shouldSerializeInt() {
    // Given:
    givenSerializerForSchema(Schema.OPTIONAL_INT32_SCHEMA);

    // When:
    final byte[] bytes = serializer.serialize(SOME_TOPIC, 62);

    // Then:
    assertThat(deserialize(bytes), is(62));
    assertThat(avroSchemaStoredInSchemaRegistry(), is(INT_AVRO_SCHEMA));
  }

  @Test
  public void shouldThrowIfNotInt() {
    // Given:
    givenSerializerForSchema(Schema.OPTIONAL_INT32_SCHEMA);

    // Then:
    expectedException.expect(SerializationException.class);
    expectedException.expectCause(hasMessage(CoreMatchers.is(
        "Invalid type for INT32: class java.lang.Boolean")));

    // When:
    serializer.serialize(SOME_TOPIC, true);
  }

  @Test
  public void shouldSerializeBigInt() {
    // Given:
    givenSerializerForSchema(Schema.OPTIONAL_INT64_SCHEMA);

    // When:
    final byte[] bytes = serializer.serialize(SOME_TOPIC, 62L);

    // Then:
    assertThat(deserialize(bytes), is(62L));
    assertThat(avroSchemaStoredInSchemaRegistry(), is(LONG_AVRO_SCHEMA));
  }

  @Test
  public void shouldThrowIfNotBigInt() {
    // Given:
    givenSerializerForSchema(Schema.OPTIONAL_INT64_SCHEMA);

    // Then:
    expectedException.expect(SerializationException.class);
    expectedException.expectCause(hasMessage(CoreMatchers.is(
        "Invalid type for INT64: class java.lang.Boolean")));

    // When:
    serializer.serialize(SOME_TOPIC, true);
  }

  @Test
  public void shouldSerializeDouble() {
    // Given:
    givenSerializerForSchema(Schema.OPTIONAL_FLOAT64_SCHEMA);

    // When:
    final byte[] bytes = serializer.serialize(SOME_TOPIC, 62.0);

    // Then:
    assertThat(deserialize(bytes), is(62.0));
    assertThat(avroSchemaStoredInSchemaRegistry(), is(DOUBLE_AVRO_SCHEMA));
  }

  @Test
  public void shouldThrowIfNotDouble() {
    // Given:
    givenSerializerForSchema(Schema.OPTIONAL_FLOAT64_SCHEMA);

    // Then:
    expectedException.expect(SerializationException.class);
    expectedException.expectCause(hasMessage(CoreMatchers.is(
        "Invalid type for FLOAT64: class org.apache.kafka.connect.data.Struct")));

    // When:
    serializer.serialize(SOME_TOPIC, new Struct(ORDER_SCHEMA));
  }

  @Test
  public void shouldSerializeString() {
    // Given:
    givenSerializerForSchema(Schema.OPTIONAL_STRING_SCHEMA);

    // When:
    final byte[] bytes = serializer.serialize(SOME_TOPIC, "a string");

    // Then:
    assertThat(deserialize(bytes), is("a string"));
    assertThat(avroSchemaStoredInSchemaRegistry(), is(STRING_AVRO_SCHEMA));
  }

  @Test
  public void shouldThrowIfNotString() {
    // Given:
    givenSerializerForSchema(Schema.OPTIONAL_STRING_SCHEMA);

    // Then:
    expectedException.expect(SerializationException.class);
    expectedException.expectCause(hasMessage(CoreMatchers.is(
        "Invalid type for STRING: class org.apache.kafka.connect.data.Struct")));

    // When:
    serializer.serialize(SOME_TOPIC, new Struct(ORDER_SCHEMA));
  }

  @Test
  public void shouldSerializeArray() {
    // Given:
    givenSerializerForSchema(SchemaBuilder
        .array(Schema.OPTIONAL_BOOLEAN_SCHEMA)
        .build()
    );

    final List<Boolean> value = ImmutableList.of(true, false);

    // When:
    final byte[] bytes = serializer.serialize(SOME_TOPIC, value);

    // Then:
    assertThat(deserialize(bytes), is(value));
    assertThat(avroSchemaStoredInSchemaRegistry(), is(BOOLEAN_ARRAY_AVRO_SCHEMA));
  }

  @Test
  public void shouldThrowIfNotArray() {
    // Given:
    givenSerializerForSchema(SchemaBuilder
        .array(Schema.OPTIONAL_BOOLEAN_SCHEMA)
        .build()
    );

    // Then:
    expectedException.expect(SerializationException.class);
    expectedException.expectCause(hasMessage(CoreMatchers.is(
        "java.lang.Boolean cannot be cast to java.util.List")));

    // When:
    serializer.serialize(SOME_TOPIC, true);
  }

  @Test
  public void shouldThrowOnWrongElementType() {
    // Given:
    givenSerializerForSchema(SchemaBuilder
        .array(Schema.BOOLEAN_SCHEMA)
        .build()
    );

    // Then:
    expectedException.expect(SerializationException.class);
    expectedException.expectCause(hasMessage(CoreMatchers.is(
        "Invalid type for BOOLEAN: class java.lang.String")));

    // When:
    serializer.serialize(SOME_TOPIC, ImmutableList.of("not boolean"));
  }

  @Test
  public void shouldSerializeArrayOfArray() {
    // Given:
    givenSerializerForSchema(SchemaBuilder
        .array(SchemaBuilder
            .array(Schema.OPTIONAL_BOOLEAN_SCHEMA)
            .build())
        .build()
    );

    final List<List<Boolean>> value = ImmutableList.of(ImmutableList.of(true, false));

    // When:
    final byte[] bytes = serializer.serialize(SOME_TOPIC, value);

    // Then:
    assertThat(deserialize(bytes), is(value));
  }

  @Test
  public void shouldSerializeArrayOfMap() {
    // Given:
    givenSerializerForSchema(SchemaBuilder
        .array(SchemaBuilder
            .map(Schema.OPTIONAL_STRING_SCHEMA, Schema.OPTIONAL_INT64_SCHEMA)
            .build())
        .build()
    );

    final List<Map<String, Long>> value = ImmutableList.of(ImmutableMap.of("a", 1L));

    // When:
    final byte[] bytes = serializer.serialize(SOME_TOPIC, value);

    // Then:
    final GenericArray<?> expectedElements = buildConnectMapEntries(
        ImmutableMap.of(new Utf8("a"), 1L),
        org.apache.avro.Schema.create(Type.LONG)
    );

    assertThat(deserialize(bytes), is(ImmutableList.of(expectedElements)));
  }

  @Test
  public void shouldSerializeArrayOfStruct() {
    // Given:
    givenSerializerForSchema(SchemaBuilder
        .array(ORDER_SCHEMA)
        .build()
    );

    final List<Struct> value = ImmutableList.of(orderStruct);

    // When:
    final byte[] bytes = serializer.serialize(SOME_TOPIC, value);

    // Then:
    assertThat(deserialize(bytes), is(ImmutableList.of(avroOrder)));
  }

  @Test
  public void shouldSerializeMapWithRequiredKeys() {
    // Given:
    givenSerializerForSchema(SchemaBuilder
        .map(Schema.STRING_SCHEMA, Schema.OPTIONAL_INT32_SCHEMA)
        .build()
    );

    // When:
    final byte[] bytes = serializer.serialize(SOME_TOPIC, ImmutableMap.of("a", 1, "b", 2));

    // Then:
    final Map<?, ?> actual = deserialize(bytes);
    assertThat(actual, is(ImmutableMap.of(new Utf8("a"), 1, new Utf8("b"), 2)));
    assertThat(avroSchemaStoredInSchemaRegistry(), is(REQUIRED_KEY_MAP_AVRO_SCHEMA));
  }

  @Test
  public void shouldSerializeMapWithOptionalKeys() {
    // Given:
    givenSerializerForSchema(SchemaBuilder
        .map(Schema.OPTIONAL_STRING_SCHEMA, Schema.OPTIONAL_INT32_SCHEMA)
        .build()
    );

    // When:
    final byte[] bytes = serializer.serialize(SOME_TOPIC, ImmutableMap.of("a", 1, "b", 2));

    // Then:
    final GenericArray<?> expected = buildConnectMapEntries(
        ImmutableMap.of(new Utf8("a"), 1, new Utf8("b"), 2),
        org.apache.avro.Schema.create(Type.INT)
    );

    final GenericArray<?> actual = deserialize(bytes);
    assertThat(actual, is(expected));
    assertThat(avroSchemaStoredInSchemaRegistry(), is(OPTIONAL_KEY_MAP_AVRO_SCHEMA));
  }

  @Test
  public void shouldThrowIfNotMap() {
    // Given:
    givenSerializerForSchema(SchemaBuilder
        .map(Schema.OPTIONAL_STRING_SCHEMA, Schema.OPTIONAL_INT64_SCHEMA)
        .build()
    );

    // Then:
    expectedException.expect(SerializationException.class);
    expectedException.expectCause(hasMessage(CoreMatchers.is(
        "java.lang.Boolean cannot be cast to java.util.Map")));

    // When:
    serializer.serialize(SOME_TOPIC, true);
  }

  @Test
  public void shouldThrowIfKeyWrongType() {
    // Given:
    givenSerializerForSchema(SchemaBuilder
        .map(Schema.OPTIONAL_STRING_SCHEMA, Schema.OPTIONAL_INT64_SCHEMA)
        .build()
    );

    // Then:
    expectedException.expect(SerializationException.class);
    expectedException.expectCause(hasMessage(CoreMatchers.is(
        "Invalid type for STRING: class java.lang.Integer")));

    // When:
    serializer.serialize(SOME_TOPIC, ImmutableMap.of(1, 2));
  }

  @Test
  public void shouldThrowIfValueWrongType() {
    // Given:
    givenSerializerForSchema(SchemaBuilder
        .map(Schema.OPTIONAL_STRING_SCHEMA, Schema.OPTIONAL_INT64_SCHEMA)
        .build()
    );

    // Then:
    expectedException.expect(SerializationException.class);
    expectedException.expectCause(hasMessage(CoreMatchers.is(
        "Invalid type for INT64: class java.lang.Boolean")));

    // When:
    serializer.serialize(SOME_TOPIC, ImmutableMap.of("a", false));
  }

  @Test
  public void shouldThrowOnMapSchemaWithNonStringKeys() {
    // Given:
    final PersistenceSchema physicalSchema = unwrappedPersistenceSchema(
        SchemaBuilder
            .map(Schema.OPTIONAL_INT32_SCHEMA, Schema.INT32_SCHEMA)
            .build()
    );

    // Then:
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("Avro only supports MAPs with STRING keys");

    // When:
    new KsqlAvroSerdeFactory(KsqlConstants.DEFAULT_AVRO_SCHEMA_FULL_NAME)
        .createSerde(
            physicalSchema,
            ksqlConfig,
            () -> schemaRegistryClient
        );
  }

  @Test
  public void shouldThrowOnNestedMapSchemaWithNonStringKeys() {
    // Given:
    final PersistenceSchema physicalSchema = unwrappedPersistenceSchema(
        SchemaBuilder
            .struct()
            .field("f0", SchemaBuilder
                .map(Schema.OPTIONAL_INT32_SCHEMA, Schema.INT32_SCHEMA)
                .optional()
                .build())
            .build()
    );

    // Then:
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("Avro only supports MAPs with STRING keys");

    // When:
    new KsqlAvroSerdeFactory(KsqlConstants.DEFAULT_AVRO_SCHEMA_FULL_NAME)
        .createSerde(
            physicalSchema,
            ksqlConfig,
            () -> schemaRegistryClient
        );
  }

  @Test
  public void shouldSerializeMapOfMaps() {
    // Given:
    givenSerializerForSchema(SchemaBuilder
        .map(Schema.OPTIONAL_STRING_SCHEMA, SchemaBuilder
            .map(Schema.OPTIONAL_STRING_SCHEMA, Schema.OPTIONAL_INT32_SCHEMA)
            .optional()
            .build())
        .build()
    );

    final Map<String, Map<String, Integer>> value =
        ImmutableMap.of("k", ImmutableMap.of("a", 1, "b", 2));

    // When:
    final byte[] bytes = serializer.serialize(SOME_TOPIC, value);

    // Then:
    final GenericArray<?> inner = buildConnectMapEntries(
        ImmutableMap.of(new Utf8("a"), 1, new Utf8("b"), 2),
        org.apache.avro.Schema.create(Type.INT)
    );

    final GenericArray<?> expected = buildConnectMapEntries(
        ImmutableMap.of(new Utf8("k"), inner),
        AvroTestUtil.connectOptionalKeyMapSchema(
            AvroTestUtil.connectOptionalKeyMapEntrySchema("KsqlDataSourceSchema_MapValue",
            org.apache.avro.Schema.create(Type.INT)))
    );

    final GenericArray<?> actual = deserialize(bytes);
    assertThat(actual, is(expected));
  }

  @Test
  public void shouldSerializeMapOfStruct() {
    // Given:
    givenSerializerForSchema(SchemaBuilder
        .map(Schema.OPTIONAL_STRING_SCHEMA, ORDER_SCHEMA)
        .build()
    );

    final Map<String, Struct> value = ImmutableMap.of("k", orderStruct);

    // When:
    final byte[] bytes = serializer.serialize(SOME_TOPIC, value);

    // Then:
    final org.apache.avro.Schema elementSchema =
        rename(ORDER_AVRO_SCHEMA, "KsqlDataSourceSchema_MapValue");

    avroOrder = new GenericData.Record(elementSchema);
    avroOrder.put(ORDERTIME, 1511897796092L);
    avroOrder.put(ORDERID, 1L);
    avroOrder.put(ITEMID, new Utf8("item_1"));
    avroOrder.put(ORDERUNITS, 10.0);
    avroOrder.put(ARRAYCOL, ImmutableList.of(100.0));
    avroOrder.put(MAPCOL, ImmutableMap.of(new Utf8("key1"), 100.0));

    final GenericArray<?> expected = buildConnectMapEntries(
        ImmutableMap.of(new Utf8("k"), avroOrder),
        elementSchema
    );

    final GenericArray<?> actual = deserialize(bytes);
    assertThat(actual, is(expected));
  }

  @Test
  public void shouldSerializeNullAsNull() {
    // Given:
    givenSerializerForSchema(Schema.OPTIONAL_INT64_SCHEMA);

    // Then:
    assertThat(serializer.serialize(SOME_TOPIC, null), is(nullValue()));
  }

  @Test
  public void shouldHandleNulls() {
    // Given:
    givenSerializerForSchema(ORDER_SCHEMA);

    orderStruct
        .put(ARRAYCOL, null)
        .put(MAPCOL, null);

    // When:
    final byte[] bytes = serializer.serialize(SOME_TOPIC, orderStruct);

    // Then:
    avroOrder.put("ARRAYCOL", null);
    avroOrder.put("MAPCOL", null);

    assertThat(deserialize(bytes), is(avroOrder));
  }

  @Test
  public void shouldIncludeTopicNameInException() {
    // Given:
    givenSerializerForSchema(Schema.OPTIONAL_INT64_SCHEMA);

    // Then:
    expectedException.expectMessage(SOME_TOPIC);

    // When:
    serializer.serialize(SOME_TOPIC, true);
  }

  @Test
  public void shouldNotIncludeBadValueInExceptionAsThatWouldBeASecurityIssue() {
    // Given:
    givenSerializerForSchema(Schema.OPTIONAL_INT64_SCHEMA);

    try {

      // When:
      serializer.serialize(SOME_TOPIC, "personal info: do not log me");

      fail("Invalid test: should throw");

    } catch (final Exception e) {
      assertThat(ExceptionUtils.getStackTrace(e), not(containsString("personal info")));
    }
  }

  @Test
  public void shouldSerializeIntegerField() {
    shouldSerializeFieldTypeCorrectly(
        Schema.OPTIONAL_INT32_SCHEMA,
        123,
        org.apache.avro.SchemaBuilder.builder().intType());
  }

  @Test
  public void shouldSerializeBigintField() {
    shouldSerializeFieldTypeCorrectly(
        Schema.OPTIONAL_INT64_SCHEMA,
        123L,
        org.apache.avro.SchemaBuilder.builder().longType());
  }

  @Test
  public void shouldSerializeBooleanField() {
    shouldSerializeFieldTypeCorrectly(
        Schema.OPTIONAL_BOOLEAN_SCHEMA,
        false,
        org.apache.avro.SchemaBuilder.builder().booleanType());
    shouldSerializeFieldTypeCorrectly(
        Schema.OPTIONAL_BOOLEAN_SCHEMA,
        true,
        org.apache.avro.SchemaBuilder.builder().booleanType());
  }

  @Test
  public void shouldSerializeStringField() {
    shouldSerializeFieldTypeCorrectly(
        Schema.OPTIONAL_STRING_SCHEMA,
        "foobar",
        org.apache.avro.SchemaBuilder.builder().stringType(),
        new Utf8("foobar"));
  }

  @Test
  public void shouldSerializeDoubleField() {
    shouldSerializeFieldTypeCorrectly(
        Schema.OPTIONAL_FLOAT64_SCHEMA,
        1.23456789012345,
        org.apache.avro.SchemaBuilder.builder().doubleType());
  }

  @Test
  public void shouldSerializeDecimalField() {
    final BigDecimal value = new BigDecimal("12.34");
    final ByteBuffer bytes = new DecimalConversion().toBytes(
            value,
            DECIMAL_SCHEMA,
            LogicalTypes.decimal(4, 2));

    shouldSerializeFieldTypeCorrectly(
        DecimalUtil.builder(4, 2).build(),
        value,
        DECIMAL_SCHEMA,
        bytes
    );
  }

  @Test
  public void shouldSerializeArrayField() {
    shouldSerializeFieldTypeCorrectly(
        SchemaBuilder.array(Schema.OPTIONAL_INT32_SCHEMA).optional().build(),
        ImmutableList.of(1, 2, 3),
        org.apache.avro.SchemaBuilder.array().items(
            org.apache.avro.SchemaBuilder.builder()
                .unionOf().nullType().and().intType().endUnion())
    );
  }

  @Test
  public void shouldSerializeMapFieldWithName() {
    final org.apache.avro.Schema avroSchema =
        AvroTestUtil.connectOptionalKeyMapSchema(
        connectMapEntrySchema(KsqlConstants.AVRO_SCHEMA_NAMESPACE + ".KsqlDataSourceSchema_field0"));

    shouldSerializeMap(avroSchema);
  }

  @Test
  public void shouldSerializeMapFieldWithoutNameIfDisabled() {
    ksqlConfig = new KsqlConfig(ImmutableMap.of(
        KsqlConfig.KSQL_USE_NAMED_AVRO_MAPS, false
    ));

    final org.apache.avro.Schema avroSchema =
        AvroTestUtil.connectOptionalKeyMapSchema(legacyMapEntrySchema());

    shouldSerializeMap(avroSchema);
  }

  @Test
  public void shouldSerializeMultipleMapFields() {
    final org.apache.avro.Schema avroInnerSchema0
        = connectMapEntrySchema(
        KsqlConstants.AVRO_SCHEMA_NAMESPACE + ".KsqlDataSourceSchema_field0_inner0");
    final org.apache.avro.Schema avroInnerSchema1 =
        AvroTestUtil.connectOptionalKeyMapEntrySchema(
            KsqlConstants.AVRO_SCHEMA_NAMESPACE + ".KsqlDataSourceSchema_field0_inner1",
            org.apache.avro.Schema.create(Type.STRING));
    final org.apache.avro.Schema avroSchema =
        org.apache.avro.SchemaBuilder.record("KsqlDataSourceSchema_field0")
            .namespace("io.confluent.ksql.avro_schemas")
            .fields()
            .name("inner0").type().unionOf().nullType().and().array()
            .items(avroInnerSchema0).endUnion().nullDefault()
            .name("inner1").type().unionOf().nullType().and().array()
            .items(avroInnerSchema1).endUnion().nullDefault()
            .endRecord();

    final Schema ksqlSchema = SchemaBuilder.struct()
        .field(
            "inner0",
            SchemaBuilder.map(
                Schema.OPTIONAL_STRING_SCHEMA,
                Schema.OPTIONAL_INT64_SCHEMA).optional().build())
        .field("inner1",
            SchemaBuilder.map(
                Schema.OPTIONAL_STRING_SCHEMA,
                Schema.OPTIONAL_STRING_SCHEMA).optional().build())
        .optional()
        .build();

    final Struct value = new Struct(ksqlSchema)
        .put("inner0", ImmutableMap.of("foo", 123L))
        .put("inner1", ImmutableMap.of("bar", "baz"));

    final List<GenericRecord> avroInner0 = Collections.singletonList(
        new GenericRecordBuilder(avroInnerSchema0).set("key", "foo").set("value", 123L).build());
    final List<GenericRecord> avroInner1 = Collections.singletonList(
        new GenericRecordBuilder(avroInnerSchema1).set("key", "bar").set("value", "baz").build());
    final GenericRecord avroValue = new GenericRecordBuilder(avroSchema)
        .set("inner0", avroInner0)
        .set("inner1", avroInner1)
        .build();

    shouldSerializeFieldTypeCorrectly(ksqlSchema, value, avroSchema, avroValue);
  }

  @Test
  public void shouldSerializeStructField() {
    final org.apache.avro.Schema avroSchema
        = org.apache.avro.SchemaBuilder.record(KsqlConstants.AVRO_SCHEMA_NAME + "_field0")
        .namespace(KsqlConstants.AVRO_SCHEMA_NAMESPACE)
        .fields()
        .name("field1")
        .type().unionOf().nullType().and().intType().endUnion()
        .nullDefault()
        .name("field2")
        .type().unionOf().nullType().and().stringType().endUnion()
        .nullDefault()
        .endRecord();
    final GenericRecord avroValue = new GenericData.Record(avroSchema);
    avroValue.put("field1", 123);
    avroValue.put("field2", "foobar");
    final Schema ksqlSchema = SchemaBuilder.struct()
        .field("field1", Schema.OPTIONAL_INT32_SCHEMA)
        .field("field2", Schema.OPTIONAL_STRING_SCHEMA)
        .optional()
        .build();
    final Struct value = new Struct(ksqlSchema);
    value.put("field1", 123);
    value.put("field2", "foobar");
    shouldSerializeFieldTypeCorrectly(ksqlSchema, value, avroSchema, avroValue);
  }

  @Test
  public void shouldEncodeSourceNameIntoFieldName() {
    // Given:
    final Schema ksqlRecordSchema = SchemaBuilder.struct()
        .field("source.field0", Schema.OPTIONAL_INT32_SCHEMA)
        .build();

    final Struct ksqlRecord = new Struct(ksqlRecordSchema)
        .put("source.field0", 123);

    givenSerializerForSchema(ksqlRecordSchema);

    // When:
    final byte[] bytes = serializer.serialize(SOME_TOPIC, ksqlRecord);

    // Then:
    final GenericRecord avroRecord = deserialize(bytes);

    assertThat(avroRecord.getSchema().getFields().size(), is(1));
    assertThat(avroRecord.get("source_field0"), is(123));
  }

  @Test
  public void shouldUseSchemaNameFromPropertyIfExists() {
    // Given:
    final String schemaName = "TestSchemaName1";
    final String schemaNamespace = "com.test.namespace";

    final Schema ksqlSchema = Schema.OPTIONAL_STRING_SCHEMA;
    final Object ksqlValue = "foobar";

    final Schema ksqlRecordSchema = SchemaBuilder.struct()
        .field("field0", ksqlSchema)
        .build();

    final Struct ksqlRecord = new Struct(ksqlRecordSchema)
        .put("field0", ksqlValue);

    final Serializer<Object> serializer =
        new KsqlAvroSerdeFactory(schemaNamespace + "." + schemaName)
            .createSerde(
                PersistenceSchema.from((ConnectSchema) ksqlRecordSchema, false),
                ksqlConfig,
                () -> schemaRegistryClient
            ).serializer();

    // When:
    final byte[] bytes = serializer.serialize(SOME_TOPIC, ksqlRecord);

    // Then:
    final GenericRecord avroRecord = deserialize(bytes);

    assertThat(avroRecord.getSchema().getNamespace(), is(schemaNamespace));
    assertThat(avroRecord.getSchema().getName(), is(schemaName));
  }

  private static org.apache.avro.Schema legacyMapEntrySchema() {
    final String name = AvroData.NAMESPACE + "." + AvroData.MAP_ENTRY_TYPE_NAME;
    return org.apache.avro.SchemaBuilder.record(name)
        .fields()
        .optionalString("key")
        .name("value")
        .type().unionOf().nullType().and().longType().endUnion()
        .nullDefault()
        .endRecord();
  }

  private static org.apache.avro.Schema connectMapEntrySchema(final String name) {
    return AvroTestUtil
        .connectOptionalKeyMapEntrySchema(name, org.apache.avro.Schema.create(Type.LONG));
  }

  private static org.apache.avro.Schema recordSchema(
      final Map<String, org.apache.avro.Schema> fields
  ) {
    final FieldAssembler<org.apache.avro.Schema> builder = org.apache.avro.SchemaBuilder
        .builder("io.confluent.ksql.avro_schemas")
        .record("KsqlDataSourceSchema")
        .fields();

    fields.forEach((k, v) -> builder
        .name(k)
        .type().unionOf().nullType().and().type(v).endUnion()
        .nullDefault());

    return builder.endRecord();
  }

  private void shouldSerializeMap(final org.apache.avro.Schema avroSchema) {
    // Given;
    final Map<String, Long> value = ImmutableMap.of("foo", 123L);
    final List<GenericRecord> avroValue = new LinkedList<>();
    for (final Map.Entry<String, Long> entry : value.entrySet()) {
      final GenericRecord record = new GenericData.Record(avroSchema.getElementType());
      record.put("key", entry.getKey());
      record.put("value", entry.getValue());
      avroValue.add(record);
    }

    // Then:
    shouldSerializeFieldTypeCorrectly(
        SchemaBuilder.map(Schema.OPTIONAL_STRING_SCHEMA, Schema.OPTIONAL_INT64_SCHEMA)
            .optional()
            .build(),
        value,
        avroSchema,
        avroValue);
  }

  @SuppressWarnings("unchecked")
  private <T> T deserialize(final byte[] serializedRow) {
    return (T) deserializer.deserialize(SOME_TOPIC, serializedRow);
  }

  private void shouldSerializeFieldTypeCorrectly(
      final Schema ksqlSchema,
      final Object ksqlValue,
      final org.apache.avro.Schema avroSchema
  ) {
    shouldSerializeFieldTypeCorrectly(ksqlSchema, ksqlValue, avroSchema, ksqlValue);
  }

  private void shouldSerializeFieldTypeCorrectly(final Schema ksqlSchema,
      final Object ksqlValue,
      final org.apache.avro.Schema avroSchema,
      final Object avroValue
  ) {
    // Given:
    final Schema schema = SchemaBuilder.struct()
        .field("field0", ksqlSchema)
        .build();
    givenSerializerForSchema(schema);

    final Struct ksqlRecord = new Struct(schema)
        .put("field0", ksqlValue);

    // When:
    final byte[] bytes = serializer.serialize(SOME_TOPIC, ksqlRecord);

    // Then:
    final GenericRecord avroRecord = deserialize(bytes);
    assertThat(avroRecord.getSchema().getNamespace(), equalTo(KsqlConstants.AVRO_SCHEMA_NAMESPACE));
    assertThat(avroRecord.getSchema().getName(), equalTo(KsqlConstants.AVRO_SCHEMA_NAME));
    assertThat(avroRecord.getSchema().getFields().size(), equalTo(1));
    final org.apache.avro.Schema.Field field = avroRecord.getSchema().getFields().get(0);
    assertThat(field.schema().getType(), equalTo(org.apache.avro.Schema.Type.UNION));
    assertThat(field.schema().getTypes().size(), equalTo(2));
    assertThat(
        field.schema().getTypes().get(0),
        equalTo(org.apache.avro.Schema.create(org.apache.avro.Schema.Type.NULL)));
    assertThat(field.schema().getTypes().get(1), equalTo(avroSchema));
    assertThat(avroRecord.get("field0"), equalTo(avroValue));
  }

  private void givenSerializerForSchema(final Schema schema) {
    final boolean unwrap = schema.type() != Schema.Type.STRUCT;
    final Schema ksqlSchema = unwrap
        ? SchemaBuilder.struct().field("f0", schema).build()
        :  schema;

    serializer = new KsqlAvroSerdeFactory(KsqlConstants.DEFAULT_AVRO_SCHEMA_FULL_NAME)
        .createSerde(
            PersistenceSchema.from((ConnectSchema) ksqlSchema, unwrap),
            ksqlConfig,
            () -> schemaRegistryClient
        ).serializer();
  }

  private org.apache.avro.Schema avroSchemaStoredInSchemaRegistry() {
    try {
      final SchemaMetadata schemaMetadata = schemaRegistryClient
          .getLatestSchemaMetadata(SOME_TOPIC + KsqlConstants.SCHEMA_REGISTRY_VALUE_SUFFIX);

      return parseAvroSchema(schemaMetadata.getSchema());
    } catch (final Exception e) {
      throw new RuntimeException("Failed to get schema from SR", e);
    }
  }

  private static org.apache.avro.Schema parseAvroSchema(final String avroSchema) {
    return new org.apache.avro.Schema.Parser().parse(avroSchema);
  }

  private static GenericArray<GenericRecord> buildConnectMapEntries(
      final Map<Utf8, Object> data,
      final org.apache.avro.Schema valueSchema
  ) {
    final org.apache.avro.Schema entrySchema =
        AvroTestUtil.connectOptionalKeyMapEntrySchema("KsqlDataSourceSchema", valueSchema);

    final org.apache.avro.Schema arraySchema =
        AvroTestUtil.connectOptionalKeyMapSchema(entrySchema);

    final Array<GenericRecord> entries = new Array<>(data.size(), arraySchema);

    data.entrySet().stream()
        .map(e -> new GenericRecordBuilder(entrySchema).set("key", e.getKey())
            .set("value", e.getValue()).build())
        .forEach(entries::add);

    return entries;
  }

  private static org.apache.avro.Schema rename(
      final org.apache.avro.Schema schema,
      final String newName
  ) {
    final FieldAssembler<org.apache.avro.Schema> builder = org.apache.avro.SchemaBuilder
        .builder(schema.getNamespace())
        .record(newName)
        .fields();

    schema.getFields().forEach(f -> builder
        .name(f.name())
        .doc(f.doc())
        .type(f.schema())
        .withDefault(f.defaultVal() == org.apache.avro.Schema.NULL_VALUE ? null : f.defaultVal())
    );

    return builder.endRecord();
  }

  private static PersistenceSchema unwrappedPersistenceSchema(final Schema fieldSchema) {
    final ConnectSchema connectSchema = (ConnectSchema) SchemaBuilder
        .struct()
        .field("f0", fieldSchema)
        .build();

    return PersistenceSchema.from(connectSchema, true);
  }
}
