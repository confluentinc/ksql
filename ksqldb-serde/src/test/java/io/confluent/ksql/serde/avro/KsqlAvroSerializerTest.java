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

import static io.confluent.ksql.util.KsqlConstants.getSRSubject;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.kafka.connect.data.Schema.FLOAT64_SCHEMA;
import static org.apache.kafka.connect.data.Schema.INT64_SCHEMA;
import static org.apache.kafka.connect.data.Schema.OPTIONAL_BOOLEAN_SCHEMA;
import static org.apache.kafka.connect.data.Schema.OPTIONAL_BYTES_SCHEMA;
import static org.apache.kafka.connect.data.Schema.OPTIONAL_FLOAT64_SCHEMA;
import static org.apache.kafka.connect.data.Schema.OPTIONAL_INT32_SCHEMA;
import static org.apache.kafka.connect.data.Schema.OPTIONAL_INT64_SCHEMA;
import static org.apache.kafka.connect.data.Schema.OPTIONAL_STRING_SCHEMA;
import static org.apache.kafka.connect.data.Schema.STRING_SCHEMA;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.*;
import static org.junit.internal.matchers.ThrowableMessageMatcher.hasMessage;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.confluent.connect.avro.AvroData;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.ksql.serde.connect.ConnectProperties;
import io.confluent.ksql.util.DecimalUtil;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.hamcrest.CoreMatchers;
import org.hamcrest.Matcher;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@SuppressWarnings({"SameParameterValue", "rawtypes", "unchecked"})
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
  private static final org.apache.avro.Schema ORDER_AVRO_SCHEMA_WITH_CONNECT = parseAvroSchema("{"
      + "\"namespace\": \"io.confluent.ksql.avro_schemas\","
      + "\"name\": \"KsqlDataSourceSchema\","
      + "\"type\": \"record\","
      + "\"fields\": ["
      + " {\"name\": \"ORDERTIME\", \"type\": [\"null\",\"long\"], \"default\": null},"
      + " {\"name\": \"ORDERID\",  \"type\": [\"null\",\"long\"], \"default\": null},"
      + " {\"name\": \"ITEMID\", \"type\": [\"null\",\"string\"], \"default\": null},"
      + " {\"name\": \"ORDERUNITS\", \"type\": [\"null\",\"double\"], \"default\": null},"
      + " {\"name\": \"ARRAYCOL\", \"type\": [\"null\",{\"type\": \"array\", \"items\": [\"null\",\"double\"]}], \"default\": null},"
      + " {\"name\": \"MAPCOL\", \"type\": [\"null\",{\"type\": \"map\", \"values\": [\"null\",\"double\"], \"connect.name\":\"io.confluent.ksql.avro_schemas.KsqlDataSourceSchema_MAPCOL\"}], \"default\": null}"
      + " ],"
      + "\"connect.name\":\"io.confluent.ksql.avro_schemas.KsqlDataSourceSchema\""
      + "}");
  private static final org.apache.avro.Schema MAP_VALUE_ORDER_AVRO_SCHEMA = parseAvroSchema("{"
      + "\"namespace\": \"io.confluent.ksql.avro_schemas\","
      + "\"name\": \"KsqlDataSourceSchema_MapValue\","
      + "\"type\": \"record\","
      + "\"fields\": ["
      + " {\"name\": \"ORDERTIME\", \"type\": [\"null\",\"long\"], \"default\": null},"
      + " {\"name\": \"ORDERID\",  \"type\": [\"null\",\"long\"], \"default\": null},"
      + " {\"name\": \"ITEMID\", \"type\": [\"null\",\"string\"], \"default\": null},"
      + " {\"name\": \"ORDERUNITS\", \"type\": [\"null\",\"double\"], \"default\": null},"
      + " {\"name\": \"ARRAYCOL\", \"type\": [\"null\",{\"type\": \"array\", \"items\": [\"null\",\"double\"]}], \"default\": null},"
      + " {\"name\": \"MAPCOL\", \"type\": [\"null\",{\"type\": \"map\", \"values\": [\"null\",\"double\"], \"connect.name\": \"io.confluent.ksql.avro_schemas.KsqlDataSourceSchema_MapValue_MAPCOL\"}], \"default\": null}"
      + " ],"
      + "\"connect.name\": \"io.confluent.ksql.avro_schemas.KsqlDataSourceSchema_MapValue\""
      + "}");
  private static final org.apache.avro.Schema NESTED_ORDER_AVRO_SCHEMA = parseAvroSchema("{"
    + "\"namespace\": \"io.confluent.ksql.avro_schemas\","
    + "\"name\": \"KsqlDataSourceSchema_nested\","
    + "\"type\": \"record\","
    + "\"fields\": ["
    + " {\"name\": \"ORDERTIME\", \"type\": [\"null\",\"long\"], \"default\": null},"
    + " {\"name\": \"ORDERID\",  \"type\": [\"null\",\"long\"], \"default\": null},"
    + " {\"name\": \"ITEMID\", \"type\": [\"null\",\"string\"], \"default\": null},"
    + " {\"name\": \"ORDERUNITS\", \"type\": [\"null\",\"double\"], \"default\": null},"
    + " {\"name\": \"ARRAYCOL\", \"type\": [\"null\",{\"type\": \"array\", \"items\": [\"null\",\"double\"]}], \"default\": null},"
    + " {\"name\": \"MAPCOL\", \"type\": [\"null\",{\"type\": \"map\", \"values\": [\"null\",\"double\"], \"connect.name\": \"io.confluent.ksql.avro_schemas.KsqlDataSourceSchema_nested_MAPCOL\"}], \"default\": null}"
    + " ],"
    + "\"connect.name\": \"io.confluent.ksql.avro_schemas.KsqlDataSourceSchema_nested\""
    + "}");

  private static final org.apache.avro.Schema ORDER_SCHEMA_WITH_EXTRA_FIELD = parseAvroSchema("{"
      + "\"namespace\": \"io.confluent.ksql.somename\","
      + "\"name\": \"somename\","
      + "\"type\": \"record\","
      + "\"fields\": ["
      + " {\"name\": \"ORDERTIME\", \"type\": [\"null\",\"long\"], \"default\": null},"
      + " {\"name\": \"ORDERID\",  \"type\": [\"null\",\"long\"], \"default\": null},"
      + " {\"name\": \"ITEMID\", \"type\": [\"null\",\"string\"], \"default\": null},"
      + " {\"name\": \"ORDERUNITS\", \"type\": [\"null\",\"double\"], \"default\": null},"
      + " {\"name\": \"ARRAYCOL\", \"type\": [\"null\",{\"type\": \"array\", \"items\": [\"null\",\"double\"]}], \"default\": null},"
      + " {\"name\": \"MAPCOL\", \"type\": [\"null\",{\"type\": \"map\", \"values\": [\"null\",\"double\"]}], \"default\": null},"
      + " {\"name\": \"EXTRA\", \"type\":[\"null\", {\"type\": \"record\", \"name\": \"someothername\", \"fields\": [{\"name\": \"F\", \"type\": [\"null\",\"long\"], \"default\": null}]}], \"default\": null}"
      + " ]"
      + "}");

  private static final org.apache.avro.Schema ORDER_SCHEMA_WITH_NON_OPTIONAL_EXTRA_FIELD = parseAvroSchema("{"
      + "\"namespace\": \"io.confluent.ksql.somename\","
      + "\"name\": \"somename\","
      + "\"type\": \"record\","
      + "\"fields\": ["
      + " {\"name\": \"ORDERTIME\", \"type\": [\"null\",\"long\"], \"default\": null},"
      + " {\"name\": \"ORDERID\",  \"type\": [\"null\",\"long\"], \"default\": null},"
      + " {\"name\": \"ITEMID\", \"type\": [\"null\",\"string\"], \"default\": null},"
      + " {\"name\": \"ORDERUNITS\", \"type\": [\"null\",\"double\"], \"default\": null},"
      + " {\"name\": \"ARRAYCOL\", \"type\": [\"null\",{\"type\": \"array\", \"items\": [\"null\",\"double\"]}], \"default\": null},"
      + " {\"name\": \"MAPCOL\", \"type\": [\"null\",{\"type\": \"map\", \"values\": [\"null\",\"double\"]}], \"default\": null},"
      + " {\"name\": \"EXTRA\", \"type\": \"long\"}"
      + " ]"
      + "}");

  private static final org.apache.avro.Schema ORDER_SCHEMA_WITH_DEFAULT_EXTRA_FIELD = parseAvroSchema("{"
      + "\"namespace\": \"io.confluent.ksql.somename\","
      + "\"name\": \"somename\","
      + "\"type\": \"record\","
      + "\"fields\": ["
      + " {\"name\": \"ORDERTIME\", \"type\": [\"null\",\"long\"], \"default\": null},"
      + " {\"name\": \"ORDERID\",  \"type\": [\"null\",\"long\"], \"default\": null},"
      + " {\"name\": \"ITEMID\", \"type\": [\"null\",\"string\"], \"default\": null},"
      + " {\"name\": \"ORDERUNITS\", \"type\": [\"null\",\"double\"], \"default\": null},"
      + " {\"name\": \"ARRAYCOL\", \"type\": [\"null\",{\"type\": \"array\", \"items\": [\"null\",\"double\"]}], \"default\": null},"
      + " {\"name\": \"MAPCOL\", \"type\": [\"null\",{\"type\": \"map\", \"values\": [\"null\",\"double\"]}], \"default\": null},"
      + " {\"name\": \"EXTRA\", \"type\": \"long\", \"default\": 123}"
      + " ]"
      + "}");

  private static final AvroSchema ORDER_PARSED_AVRO_SCHEMA = new AvroSchema(ORDER_AVRO_SCHEMA);
  private static final AvroSchema ORDER_AVRO_SCHEMA_WITH_EXTRA_FIELD = new AvroSchema(ORDER_SCHEMA_WITH_EXTRA_FIELD);
  private static final AvroSchema ORDER_AVRO_SCHEMA_WITH_NON_OPTIONAL_EXTRA_FIELD = new AvroSchema(ORDER_SCHEMA_WITH_NON_OPTIONAL_EXTRA_FIELD);
  private static final AvroSchema ORDER_AVRO_SCHEMA_WITH_DEFAULT_EXTRA_FIELD = new AvroSchema(ORDER_SCHEMA_WITH_DEFAULT_EXTRA_FIELD);

  private static final org.apache.avro.Schema BOOLEAN_AVRO_SCHEMA =
      parseAvroSchema("{\"type\": \"boolean\"}");

  private static final AvroSchema BOOLEAN_PARSED_AVRO_SCHEMA = new AvroSchema(BOOLEAN_AVRO_SCHEMA);

  private static final org.apache.avro.Schema INT_AVRO_SCHEMA =
      parseAvroSchema("{\"type\": \"int\"}");

  private static final AvroSchema INT_PARSED_AVRO_SCHEMA = new AvroSchema(INT_AVRO_SCHEMA);

  private static final org.apache.avro.Schema LONG_AVRO_SCHEMA =
      parseAvroSchema("{\"type\": \"long\"}");

  private static final org.apache.avro.Schema DOUBLE_AVRO_SCHEMA =
      parseAvroSchema("{\"type\": \"double\"}");

  private static final org.apache.avro.Schema STRING_AVRO_SCHEMA =
      parseAvroSchema("{\"type\": \"string\"}");

  private static final org.apache.avro.Schema BOOLEAN_ARRAY_AVRO_SCHEMA =
      parseAvroSchema("{\"type\": \"array\", \"items\": [\"null\", \"boolean\"]}");

  private static final org.apache.avro.Schema REQUIRED_KEY_MAP_AVRO_SCHEMA =
      parseAvroSchema("{\"type\": \"map\", \"values\": [\"null\", \"int\"],"
          + "\"connect.name\":\"io.confluent.ksql.avro_schemas.KsqlDataSourceSchema\"}");

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
          + "\"connect.internal.type\":\"MapEntry\"},"
          + "\"connect.name\":\"io.confluent.ksql.avro_schemas.KsqlDataSourceSchema\"}");

  private static final org.apache.avro.Schema DECIMAL_SCHEMA =
      parseAvroSchema(
          "{"
              + "\"type\": \"bytes\","
              + "\"logicalType\": \"decimal\","
              + "\"precision\": 4,"
              + "\"scale\": 2,"
              + "\"connect.version\": 1,"
              + "\"connect.parameters\":{\"scale\":\"2\",\"connect.decimal.precision\":\"4\"},"
              + "\"connect.name\":\"org.apache.kafka.connect.data.Decimal\""
              + "}");


  private static final org.apache.avro.Schema TIMESTAMP_SCHEMA =
      parseAvroSchema(
          "{"
              + "\"type\": \"long\","
              + "\"logicalType\": \"timestamp-millis\","
              + "\"connect.version\":1,"
              + "\"connect.name\":\"org.apache.kafka.connect.data.Timestamp\""
              + "}");
  private static final org.apache.avro.Schema TIME_SCHEMA =
      parseAvroSchema(
          "{"
              + "\"type\": \"int\","
              + "\"logicalType\": \"time-millis\","
              + "\"connect.version\":1,"
              + "\"connect.name\":\"org.apache.kafka.connect.data.Time\""
              + "}");
  private static final org.apache.avro.Schema DATE_SCHEMA =
      parseAvroSchema(
          "{"
              + "\"type\": \"int\","
              + "\"logicalType\": \"date\","
              + "\"connect.version\":1,"
              + "\"connect.name\":\"org.apache.kafka.connect.data.Date\""
              + "}");

  private static final String SOME_TOPIC = "bob";

  private static final String ORDERTIME = "ORDERTIME";
  private static final String ORDERID = "ORDERID";
  private static final String ITEMID = "ITEMID";
  private static final String ORDERUNITS = "ORDERUNITS";
  private static final String ARRAYCOL = "ARRAYCOL";
  private static final String MAPCOL = "MAPCOL";
  private static final String EXTRA = "EXTRA";

  private static final Schema ORDER_SCHEMA = SchemaBuilder.struct()
      .field(ORDERTIME, OPTIONAL_INT64_SCHEMA)
      .field(ORDERID, OPTIONAL_INT64_SCHEMA)
      .field(ITEMID, OPTIONAL_STRING_SCHEMA)
      .field(ORDERUNITS, OPTIONAL_FLOAT64_SCHEMA)
      .field(ARRAYCOL, SchemaBuilder
          .array(OPTIONAL_FLOAT64_SCHEMA).optional().build())
      .field(MAPCOL, SchemaBuilder
          .map(STRING_SCHEMA, OPTIONAL_FLOAT64_SCHEMA).optional().build())
      .optional()
      .build();

  private static final Schema NON_OPTIONAL_ORDER_SCHEMA = SchemaBuilder.struct()
      .field(ORDERTIME, INT64_SCHEMA)
      .field(ORDERID, INT64_SCHEMA)
      .field(ITEMID, STRING_SCHEMA)
      .field(ORDERUNITS, FLOAT64_SCHEMA)
      .field(ARRAYCOL, SchemaBuilder
          .array(FLOAT64_SCHEMA).optional().build())
      .field(MAPCOL, SchemaBuilder
          .map(STRING_SCHEMA, OPTIONAL_FLOAT64_SCHEMA).build())
      .build();

  private final SchemaRegistryClient schemaRegistryClient = new MockSchemaRegistryClient();

  private final KsqlConfig ksqlConfig = new KsqlConfig(ImmutableMap.of());

  private Deserializer<Object> deserializer;
  private Struct orderStruct;
  private GenericRecord avroOrder;
  private GenericRecord avroOrderWithExtraField;
  private GenericRecord avroOrderWithExtraDefaultField;

  @Before
  public void setup() {
    final ImmutableMap<String, Object> configs = ImmutableMap.of(
        AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS, true,
        AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, ""
    );

    deserializer = new KafkaAvroDeserializer(schemaRegistryClient, configs);

    orderStruct = new Struct(ORDER_SCHEMA)
        .put(ORDERTIME, 1511897796092L)
        .put(ORDERID, 1L)
        .put(ITEMID, "item_1")
        .put(ORDERUNITS, 10.0)
        .put(ARRAYCOL, Collections.singletonList(100.0))
        .put(MAPCOL, Collections.singletonMap("key1", 100.0));

    avroOrder = new GenericData.Record(ORDER_AVRO_SCHEMA_WITH_CONNECT);
    avroOrder.put(ORDERTIME, 1511897796092L);
    avroOrder.put(ORDERID, 1L);
    avroOrder.put(ITEMID, "item_1");
    avroOrder.put(ORDERUNITS, 10.0);
    avroOrder.put(ARRAYCOL, ImmutableList.of(100.0));
    avroOrder.put(MAPCOL, ImmutableMap.of(new Utf8("key1"), 100.0));

    avroOrderWithExtraField = new GenericData.Record(ORDER_SCHEMA_WITH_EXTRA_FIELD);
    avroOrderWithExtraField.put(ORDERTIME, 1511897796092L);
    avroOrderWithExtraField.put(ORDERID, 1L);
    avroOrderWithExtraField.put(ITEMID, "item_1");
    avroOrderWithExtraField.put(ORDERUNITS, 10.0);
    avroOrderWithExtraField.put(ARRAYCOL, ImmutableList.of(100.0));
    avroOrderWithExtraField.put(MAPCOL, ImmutableMap.of(new Utf8("key1"), 100.0));
    avroOrderWithExtraField.put(EXTRA, null);

    avroOrderWithExtraDefaultField = new GenericData.Record(ORDER_SCHEMA_WITH_DEFAULT_EXTRA_FIELD);
    avroOrderWithExtraDefaultField .put(ORDERTIME, 1511897796092L);
    avroOrderWithExtraDefaultField.put(ORDERID, 1L);
    avroOrderWithExtraDefaultField.put(ITEMID, "item_1");
    avroOrderWithExtraDefaultField.put(ORDERUNITS, 10.0);
    avroOrderWithExtraDefaultField.put(ARRAYCOL, ImmutableList.of(100.0));
    avroOrderWithExtraDefaultField.put(MAPCOL, ImmutableMap.of(new Utf8("key1"), 100.0));
    avroOrderWithExtraDefaultField.put(EXTRA, 123L);
  }

  @Test
  public void shouldSerializeNullValue() {
    // Given:
    final Serializer<Struct> serializer = givenSerializerForSchema(ORDER_SCHEMA, Struct.class);

    // When:
    final byte[] serializedRow = serializer.serialize(SOME_TOPIC, null);

    // Then:
    assertThat(serializedRow, is(nullValue()));
  }

  @Test
  public void shouldSerializeStructCorrectly() {
    // Given:
    final Serializer<Struct> serializer = givenSerializerForSchema(ORDER_SCHEMA, Struct.class);
    // When:
    final byte[] serializedRow = serializer.serialize(SOME_TOPIC, orderStruct);

    // Then:
    final GenericRecord deserialized = deserialize(serializedRow);
    assertThat(deserialized, is(avroOrder));
    assertThat(avroSchemaStoredInSchemaRegistry(), is(ORDER_AVRO_SCHEMA_WITH_CONNECT));
  }

  @Test
  public void shouldThrowIfNotStruct() {
    // Given:
    final Serializer serializer = givenSerializerForSchema(ORDER_SCHEMA, Struct.class);

    // When:
    final Exception e = assertThrows(
        SerializationException.class,
        () -> serializer.serialize(SOME_TOPIC, 10)
    );

    // Then:
    assertThat(
        e.getCause(),
        hasMessage(
            allOf(
                containsString("java.lang.Integer"),
                containsString("cannot be cast"),
                containsString("org.apache.kafka.connect.data.Struct")
            )
        )
    );
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

    final Serializer<Struct> serializer = givenSerializerForSchema(schema, Struct.class);

    // When:
    final byte[] serializedRow = serializer.serialize(SOME_TOPIC, value);

    // Then:

    avroOrder = new GenericData.Record(NESTED_ORDER_AVRO_SCHEMA);
    avroOrder.put(ORDERTIME, 1511897796092L);
    avroOrder.put(ORDERID, 1L);
    avroOrder.put(ITEMID, "item_1");
    avroOrder.put(ORDERUNITS, 10.0);
    avroOrder.put(ARRAYCOL, ImmutableList.of(100.0));
    avroOrder.put(MAPCOL, ImmutableMap.of(new Utf8("key1"), 100.0));

    final GenericRecord avroValue =
        new GenericData.Record(recordSchema(ImmutableMap.of("nested", NESTED_ORDER_AVRO_SCHEMA)));
    avroValue.put("nested", avroOrder);
    avroValue.getSchema().addProp(
        "connect.name",
        AvroProperties.AVRO_SCHEMA_NAMESPACE + ".KsqlDataSourceSchema");
    final GenericRecord deserialized = deserialize(serializedRow);
    assertThat(deserialized, is(avroValue));
  }

  @Test
  public void shouldSerializeBoolean() {
    // Given:
    final Serializer<Boolean> serializer =
        givenSerializerForSchema(OPTIONAL_BOOLEAN_SCHEMA, Boolean.class);

    // When:
    final byte[] bytes = serializer.serialize(SOME_TOPIC, true);

    // Then:
    assertThat(deserialize(bytes), is(true));
    assertThat(avroSchemaStoredInSchemaRegistry(), is(BOOLEAN_AVRO_SCHEMA));
  }

  @Test
  public void shouldThrowIfNotBoolean() {
    // Given:
    final Serializer serializer =
        givenSerializerForSchema(OPTIONAL_BOOLEAN_SCHEMA, Boolean.class);

    // When:
    final Exception e = assertThrows(
        SerializationException.class,
        () -> serializer.serialize(SOME_TOPIC, 10)
    );

    // Then:
    assertThat(e.getCause(), (hasMessage(is(
        "Invalid type for BOOLEAN: class java.lang.Integer"))));
  }

  @Test
  public void shouldSerializeInt() {
    // Given:
    final Serializer<Integer> serializer =
        givenSerializerForSchema(OPTIONAL_INT32_SCHEMA, Integer.class);

    // When:
    final byte[] bytes = serializer.serialize(SOME_TOPIC, 62);

    // Then:
    assertThat(deserialize(bytes), is(62));
    assertThat(avroSchemaStoredInSchemaRegistry(), is(INT_AVRO_SCHEMA));
  }

  @Test
  public void shouldThrowIfNotInt() {
    // Given:
    final Serializer serializer =
        givenSerializerForSchema(OPTIONAL_INT32_SCHEMA, Integer.class);

    // When:
    final Exception e = assertThrows(
        SerializationException.class,
        () -> serializer.serialize(SOME_TOPIC, true)
    );

    // Then:
    assertThat(e.getCause(), (hasMessage(CoreMatchers.is(
        "Invalid type for INT32: class java.lang.Boolean"))));
  }

  @Test
  public void shouldSerializeBigInt() {
    // Given:
    final Serializer<Long> serializer =
        givenSerializerForSchema(OPTIONAL_INT64_SCHEMA, Long.class);

    // When:
    final byte[] bytes = serializer.serialize(SOME_TOPIC, 62L);

    // Then:
    assertThat(deserialize(bytes), is(62L));
    assertThat(avroSchemaStoredInSchemaRegistry(), is(LONG_AVRO_SCHEMA));
  }

  @Test
  public void shouldThrowIfNotBigInt() {
    // Given:
    final Serializer serializer =
        givenSerializerForSchema(OPTIONAL_INT64_SCHEMA, Long.class);

    // When:
    final Exception e = assertThrows(
        SerializationException.class,
        () -> serializer.serialize(SOME_TOPIC, true)
    );

    // Then:
    assertThat(e.getCause(), (hasMessage(CoreMatchers.is(
        "Invalid type for INT64: class java.lang.Boolean"))));
  }

  @Test
  public void shouldSerializeDouble() {
    // Given:
    final Serializer<Double> serializer =
        givenSerializerForSchema(OPTIONAL_FLOAT64_SCHEMA, Double.class);

    // When:
    final byte[] bytes = serializer.serialize(SOME_TOPIC, 62.0);

    // Then:
    assertThat(deserialize(bytes), is(62.0));
    assertThat(avroSchemaStoredInSchemaRegistry(), is(DOUBLE_AVRO_SCHEMA));
  }

  @Test
  public void shouldThrowIfNotDouble() {
    // Given:
    final Serializer serializer =
        givenSerializerForSchema(OPTIONAL_FLOAT64_SCHEMA, Double.class);

    // When:
    final Exception e = assertThrows(
        SerializationException.class,
        () -> serializer.serialize(SOME_TOPIC, new Struct(ORDER_SCHEMA))
    );

    // Then:
    assertThat(e.getCause(), (hasMessage(CoreMatchers.is(
        "Invalid type for FLOAT64: class org.apache.kafka.connect.data.Struct"))));
  }

  @Test
  public void shouldSerializeString() {
    // Given:
    final Serializer<String> serializer =
        givenSerializerForSchema(OPTIONAL_STRING_SCHEMA, String.class);

    // When:
    final byte[] bytes = serializer.serialize(SOME_TOPIC, "a string");

    // Then:
    assertThat(deserialize(bytes), is("a string"));
    assertThat(avroSchemaStoredInSchemaRegistry(), is(STRING_AVRO_SCHEMA));
  }

  @Test
  public void shouldThrowIfNotString() {
    // Given:
    final Serializer serializer =
        givenSerializerForSchema(OPTIONAL_STRING_SCHEMA, String.class);

    // When:
    final Exception e = assertThrows(
        SerializationException.class,
        () -> serializer.serialize(SOME_TOPIC, new Struct(ORDER_SCHEMA))
    );

    // Then:
    assertThat(e.getCause(), (hasMessage(CoreMatchers.is(
        "Invalid type for STRING: class org.apache.kafka.connect.data.Struct"))));
  }

  @Test
  public void shouldSerializeArray() {
    // Given:
    final Serializer<List> serializer = givenSerializerForSchema(
        SchemaBuilder.array(OPTIONAL_BOOLEAN_SCHEMA).build(),
        List.class
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
    final Serializer serializer = givenSerializerForSchema(
        SchemaBuilder.array(OPTIONAL_BOOLEAN_SCHEMA).build(),
        List.class
    );

    // When:
    final Exception e = assertThrows(
        SerializationException.class,
        () -> serializer.serialize(SOME_TOPIC, true)
    );

    // Then:
    assertThat(
        e.getCause(),
        hasMessage(
            allOf(
                containsString("java.lang.Boolean"),
                containsString("cannot be cast"),
                containsString("java.util.List")
            )
        )
    );
  }

  @Test
  public void shouldSerializeTime() {
    // Given:
    final Serializer<java.sql.Time> serializer =
        givenSerializerForSchema(Time.SCHEMA, java.sql.Time.class);
    final java.sql.Time value = new java.sql.Time(500);

    // When:
    final byte[] bytes = serializer.serialize(SOME_TOPIC, value);

    // Then:
    assertThat(deserialize(bytes), is(500));
    assertThat(avroSchemaStoredInSchemaRegistry(), is(TIME_SCHEMA));
  }

  @Test
  public void shouldSerializeDate() {
    // Given:
    final Serializer<java.sql.Date> serializer =
        givenSerializerForSchema(Date.SCHEMA, java.sql.Date.class);
    final java.sql.Date value = new java.sql.Date(864000000L);

    // When:
    final byte[] bytes = serializer.serialize(SOME_TOPIC, value);

    // Then:
    assertThat(deserialize(bytes), is(10));
    assertThat(avroSchemaStoredInSchemaRegistry(), is(DATE_SCHEMA));
  }

  @Test
  public void shouldSerializeTimestamp() {
    // Given:
    final Serializer<java.sql.Timestamp> serializer =
        givenSerializerForSchema(Timestamp.SCHEMA, java.sql.Timestamp.class);
    final java.sql.Timestamp value = new java.sql.Timestamp(500);

    // When:
    final byte[] bytes = serializer.serialize(SOME_TOPIC, value);

    // Then:
    assertThat(deserialize(bytes), is(500L));
    assertThat(avroSchemaStoredInSchemaRegistry(), is(TIMESTAMP_SCHEMA));
  }

  @Test
  public void shouldThrowOnWrongElementType() {
    // Given:
    final Serializer<List> serializer = givenSerializerForSchema(
        SchemaBuilder.array(Schema.BOOLEAN_SCHEMA).build(),
        List.class
    );

    // When:
    final Exception e = assertThrows(
        SerializationException.class,
        () -> serializer.serialize(SOME_TOPIC, ImmutableList.of("not boolean"))
    );

    // Then:
    assertThat(e.getCause(), (hasMessage(CoreMatchers.is(
        "Invalid type for BOOLEAN: class java.lang.String"))));
  }

  @Test
  public void shouldSerializeArrayOfArray() {
    // Given:
    final Serializer<List> serializer = givenSerializerForSchema(
        SchemaBuilder.array(
            SchemaBuilder
                .array(OPTIONAL_BOOLEAN_SCHEMA)
                .build()
        ).build(),
        List.class
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
    final Serializer<List> serializer = givenSerializerForSchema(
        SchemaBuilder.array(
            SchemaBuilder
                .map(OPTIONAL_STRING_SCHEMA, OPTIONAL_INT64_SCHEMA)
                .build()
        ).build(),
        List.class
    );

    final List<Map<String, Long>> value = ImmutableList.of(ImmutableMap.of("a", 1L));

    // When:
    final byte[] bytes = serializer.serialize(SOME_TOPIC, value);

    // Then:
    final GenericArray<?> expectedElements = buildConnectMapEntries(
        ImmutableMap.of(new Utf8("a"), 1L),
        org.apache.avro.Schema.create(Type.LONG)
    );

    final Matcher<List<? extends GenericArray<?>>> matcher = is(
        ImmutableList.of(expectedElements));
    final List<? extends GenericArray<?>> deserialize = deserialize(bytes);
    assertThat(deserialize, matcher);
  }

  @Test
  public void shouldSerializeArrayOfStruct() {
    // Given:
    final Serializer<List> serializer = givenSerializerForSchema(
        SchemaBuilder.array(ORDER_SCHEMA).build(),
        List.class
    );

    final List<Struct> value = ImmutableList.of(orderStruct);

    // When:
    final byte[] bytes = serializer.serialize(SOME_TOPIC, value);

    // Then:
    assertEquals(ImmutableList.of(avroOrder), deserialize(bytes));
  }

  @Test
  public void shouldSerializeMapWithRequiredKeys() {
    // Given:
    final Serializer<Map> serializer = givenSerializerForSchema(
        SchemaBuilder.map(STRING_SCHEMA, OPTIONAL_INT32_SCHEMA).build(),
        Map.class
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
    final Serializer<Map> serializer = givenSerializerForSchema(SchemaBuilder
        .map(OPTIONAL_STRING_SCHEMA, OPTIONAL_INT32_SCHEMA)
        .build(), Map.class);

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
    final Serializer serializer = givenSerializerForSchema(SchemaBuilder
        .map(OPTIONAL_STRING_SCHEMA, OPTIONAL_INT64_SCHEMA)
        .build(), Map.class);

    // When:
    final Exception e = assertThrows(
        SerializationException.class,
        () -> serializer.serialize(SOME_TOPIC, true)
    );

    // Then:
    assertThat(
        e.getCause(),
        hasMessage(
            allOf(
                containsString("java.lang.Boolean"),
                containsString("cannot be cast"),
                containsString("java.util.Map")
            )
        )
    );
  }

  @Test
  public void shouldThrowIfKeyWrongType() {
    // Given:
    final Serializer<Map> serializer = givenSerializerForSchema(SchemaBuilder
        .map(OPTIONAL_STRING_SCHEMA, OPTIONAL_INT64_SCHEMA)
        .build(), Map.class);

    // When:
    final Exception e = assertThrows(
        SerializationException.class,
        () -> serializer.serialize(SOME_TOPIC, ImmutableMap.of(1, 2))
    );

    // Then:
    assertThat(e.getCause(), (hasMessage(CoreMatchers.is(
        "Invalid type for STRING: class java.lang.Integer"))));
  }

  @Test
  public void shouldThrowIfValueWrongType() {
    // Given:
    final Serializer<Map> serializer = givenSerializerForSchema(SchemaBuilder
        .map(OPTIONAL_STRING_SCHEMA, OPTIONAL_INT64_SCHEMA)
        .build(), Map.class);

    // When:
    final Exception e = assertThrows(
        SerializationException.class,
        () -> serializer.serialize(SOME_TOPIC, ImmutableMap.of("a", false))
    );

    // Then:
    assertThat(e.getCause(), (hasMessage(CoreMatchers.is(
        "Invalid type for INT64: class java.lang.Boolean"))));
  }

  @Test
  public void shouldThrowOnMapSchemaWithNonStringKeys() {
    // Given:
    final ConnectSchema schema = (ConnectSchema)
        SchemaBuilder
            .map(OPTIONAL_INT32_SCHEMA, Schema.INT32_SCHEMA)
            .build();

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> new KsqlAvroSerdeFactory(
            ImmutableMap.of(
                ConnectProperties.FULL_SCHEMA_NAME, AvroProperties.DEFAULT_AVRO_SCHEMA_FULL_NAME)
        ).createSerde(
                schema,
                ksqlConfig,
                () -> schemaRegistryClient,
                Map.class,
                false)
    );

    // Then:
    assertThat(e.getMessage(), containsString("Avro only supports MAPs with STRING keys"));
  }

  @Test
  public void shouldThrowOnNestedMapSchemaWithNonStringKeys() {
    // Given:
    final ConnectSchema schema = (ConnectSchema) SchemaBuilder
        .struct()
        .field("f0", SchemaBuilder
            .map(OPTIONAL_INT32_SCHEMA, Schema.INT32_SCHEMA)
            .optional()
            .build())
        .build();

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> new KsqlAvroSerdeFactory(
            ImmutableMap.of(
                ConnectProperties.FULL_SCHEMA_NAME, AvroProperties.DEFAULT_AVRO_SCHEMA_FULL_NAME)
        ).createSerde(
                schema,
                ksqlConfig,
                () -> schemaRegistryClient,
                Struct.class,
                false)
    );

    // Then:
    assertThat(e.getMessage(), containsString("Avro only supports MAPs with STRING keys"));
  }

  @Test
  public void shouldSerializeMapOfMaps() {
    // Given:
    final Serializer<Map> serializer = givenSerializerForSchema(SchemaBuilder
        .map(OPTIONAL_STRING_SCHEMA, SchemaBuilder
            .map(OPTIONAL_STRING_SCHEMA, OPTIONAL_INT32_SCHEMA)
            .optional()
            .build())
        .build(), Map.class);

    final Map<String, Map<String, Integer>> value =
        ImmutableMap.of("k", ImmutableMap.of("a", 1, "b", 2));

    // When:
    final byte[] bytes = serializer.serialize(SOME_TOPIC, value);

    // Then:
    final GenericArray<?> inner = buildConnectMapEntries(
        ImmutableMap.of(new Utf8("a"), 1, new Utf8("b"), 2),
        org.apache.avro.Schema.create(Type.INT)
    );
    org.apache.avro.Schema valueSchema = AvroTestUtil.connectOptionalKeyMapSchema(
        AvroTestUtil.connectOptionalKeyMapEntrySchema("KsqlDataSourceSchema_MapValue",
            org.apache.avro.Schema.create(Type.INT)));
    valueSchema.addProp("connect.name", AvroProperties.AVRO_SCHEMA_NAMESPACE + ".KsqlDataSourceSchema_MapValue");
    final GenericArray<?> expected = buildConnectMapEntries(
        ImmutableMap.of(new Utf8("k"), inner), valueSchema);

    final GenericArray<?> actual = deserialize(bytes);
    assertThat(actual, is(expected));
  }

  @Test
  public void shouldSerializeMapOfStruct() {
    // Given:
    final Serializer<Map> serializer = givenSerializerForSchema(SchemaBuilder
        .map(OPTIONAL_STRING_SCHEMA, ORDER_SCHEMA)
        .build(), Map.class);

    final Map<String, Struct> value = ImmutableMap.of("k", orderStruct);

    // When:
    final byte[] bytes = serializer.serialize(SOME_TOPIC, value);

    // Then:
    avroOrder = new GenericData.Record(MAP_VALUE_ORDER_AVRO_SCHEMA);
    avroOrder.put(ORDERTIME, 1511897796092L);
    avroOrder.put(ORDERID, 1L);
    avroOrder.put(ITEMID, new Utf8("item_1"));
    avroOrder.put(ORDERUNITS, 10.0);
    avroOrder.put(ARRAYCOL, ImmutableList.of(100.0));
    avroOrder.put(MAPCOL, ImmutableMap.of(new Utf8("key1"), 100.0));

    final GenericArray<?> expected = buildConnectMapEntries(
        ImmutableMap.of(new Utf8("k"), avroOrder),
        MAP_VALUE_ORDER_AVRO_SCHEMA
    );

    assertEquals(deserialize(bytes), expected);
  }

  @Test
  public void shouldSerializeNullAsNull() {
    // Given:
    final Serializer<Long> serializer =
        givenSerializerForSchema(OPTIONAL_INT64_SCHEMA, Long.class);

    // Then:
    assertThat(serializer.serialize(SOME_TOPIC, null), is(nullValue()));
  }

  @Test
  public void shouldHandleNulls() {
    // Given:
    final Serializer<Struct> serializer = givenSerializerForSchema(ORDER_SCHEMA, Struct.class);

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
    final Serializer serializer = givenSerializerForSchema(OPTIONAL_INT64_SCHEMA, Long.class);

    // When:
    final Exception e = assertThrows(
        Exception.class,
        () -> serializer.serialize(SOME_TOPIC, true)
    );

    // Then:
    assertThat(e.getMessage(), containsString(SOME_TOPIC));
  }

  @Test
  public void shouldNotIncludeBadValueInExceptionAsThatWouldBeASecurityIssue() {
    // Given:
    final Serializer serializer = givenSerializerForSchema(OPTIONAL_INT64_SCHEMA, Long.class);

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
        OPTIONAL_INT32_SCHEMA,
        123,
        org.apache.avro.SchemaBuilder.builder().intType()
    );
  }

  @Test
  public void shouldSerializeBigintField() {
    shouldSerializeFieldTypeCorrectly(
        OPTIONAL_INT64_SCHEMA,
        123L,
        org.apache.avro.SchemaBuilder.builder().longType()
    );
  }

  @Test
  public void shouldSerializeBytesField() {
    shouldSerializeFieldTypeCorrectly(
        OPTIONAL_BYTES_SCHEMA,
        "abc".getBytes(UTF_8),
        org.apache.avro.SchemaBuilder.builder().bytesType(),
        ByteBuffer.wrap("abc".getBytes(UTF_8))
    );
  }

  @Test
  public void shouldSerializeBooleanField() {
    shouldSerializeFieldTypeCorrectly(
        OPTIONAL_BOOLEAN_SCHEMA,
        false,
        org.apache.avro.SchemaBuilder.builder().booleanType()
    );
    shouldSerializeFieldTypeCorrectly(
        OPTIONAL_BOOLEAN_SCHEMA,
        true,
        org.apache.avro.SchemaBuilder.builder().booleanType()
    );
  }

  @Test
  public void shouldSerializeStringField() {
    shouldSerializeFieldTypeCorrectly(
        OPTIONAL_STRING_SCHEMA,
        "foobar",
        org.apache.avro.SchemaBuilder.builder().stringType(),
        new Utf8("foobar")
    );
  }

  @Test
  public void shouldSerializeDoubleField() {
    shouldSerializeFieldTypeCorrectly(
        OPTIONAL_FLOAT64_SCHEMA,
        1.23456789012345,
        org.apache.avro.SchemaBuilder.builder().doubleType()
    );
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
        SchemaBuilder.array(OPTIONAL_INT32_SCHEMA).optional().build(),
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
            connectMapEntrySchema(AvroProperties.AVRO_SCHEMA_NAMESPACE + ".KsqlDataSourceSchema_field0"));
    avroSchema.addProp("connect.name", AvroProperties.AVRO_SCHEMA_NAMESPACE + ".KsqlDataSourceSchema_field0");
    shouldSerializeMap(avroSchema);
  }

  @Test
  public void shouldSerializeMultipleMapFields() {
    final org.apache.avro.Schema avroInnerSchema0
        = connectMapEntrySchema(
        AvroProperties.AVRO_SCHEMA_NAMESPACE + ".KsqlDataSourceSchema_field0_inner0");
    final org.apache.avro.Schema avroInnerSchema1 =
        AvroTestUtil.connectOptionalKeyMapEntrySchema(
            AvroProperties.AVRO_SCHEMA_NAMESPACE + ".KsqlDataSourceSchema_field0_inner1",
            org.apache.avro.Schema.create(Type.STRING));
    final org.apache.avro.Schema avroSchema =
        org.apache.avro.SchemaBuilder.record("KsqlDataSourceSchema_field0")
            .namespace("io.confluent.ksql.avro_schemas")
            .fields()
            .name("inner0").type().unionOf().nullType().and().array()
            .prop("connect.name",AvroProperties.AVRO_SCHEMA_NAMESPACE + ".KsqlDataSourceSchema_field0_inner0")
            .items(avroInnerSchema0).endUnion().nullDefault()
            .name("inner1").type().unionOf().nullType().and().array()
            .prop("connect.name",AvroProperties.AVRO_SCHEMA_NAMESPACE + ".KsqlDataSourceSchema_field0_inner1")
            .items(avroInnerSchema1).endUnion().nullDefault()
            .endRecord();
    avroSchema.addProp("connect.name", AvroProperties.AVRO_SCHEMA_NAMESPACE + ".KsqlDataSourceSchema_field0");

    final Schema ksqlSchema = SchemaBuilder.struct()
        .field(
            "inner0",
            SchemaBuilder.map(
                OPTIONAL_STRING_SCHEMA,
                OPTIONAL_INT64_SCHEMA).optional().build())
        .field("inner1",
            SchemaBuilder.map(
                OPTIONAL_STRING_SCHEMA,
                OPTIONAL_STRING_SCHEMA).optional().build())
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
        = org.apache.avro.SchemaBuilder.record(AvroProperties.AVRO_SCHEMA_NAME + "_field0")
        .namespace(AvroProperties.AVRO_SCHEMA_NAMESPACE)
        .fields()
        .name("field1")
        .type().unionOf().nullType().and().intType().endUnion()
        .nullDefault()
        .name("field2")
        .type().unionOf().nullType().and().stringType().endUnion()
        .nullDefault()
        .endRecord();
    final GenericRecord avroValue = new GenericData.Record(avroSchema);
    avroSchema.addProp("connect.name", AvroProperties.AVRO_SCHEMA_NAMESPACE + ".KsqlDataSourceSchema_field0");
    avroValue.put("field1", 123);
    avroValue.put("field2", "foobar");
    final Schema ksqlSchema = SchemaBuilder.struct()
        .field("field1", OPTIONAL_INT32_SCHEMA)
        .field("field2", OPTIONAL_STRING_SCHEMA)
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
        .field("source.field0", OPTIONAL_INT32_SCHEMA)
        .build();

    final Struct ksqlRecord = new Struct(ksqlRecordSchema)
        .put("source.field0", 123);

    final Serializer<Struct> serializer = givenSerializerForSchema(ksqlRecordSchema, Struct.class);

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

    final Schema ksqlSchema = OPTIONAL_STRING_SCHEMA;
    final Object ksqlValue = "foobar";

    final Schema ksqlRecordSchema = SchemaBuilder.struct()
        .field("field0", ksqlSchema)
        .build();

    final Struct ksqlRecord = new Struct(ksqlRecordSchema)
        .put("field0", ksqlValue);

    final Serializer<Struct> serializer =
        new KsqlAvroSerdeFactory(
            ImmutableMap.of(
                ConnectProperties.FULL_SCHEMA_NAME, schemaNamespace + "." + schemaName)
        ).createSerde(
                (ConnectSchema) ksqlRecordSchema,
                ksqlConfig,
                () -> schemaRegistryClient,
                Struct.class,
                false).serializer();

    // When:
    final byte[] bytes = serializer.serialize(SOME_TOPIC, ksqlRecord);

    // Then:
    final GenericRecord avroRecord = deserialize(bytes);

    assertThat(avroRecord.getSchema().getNamespace(), is(schemaNamespace));
    assertThat(avroRecord.getSchema().getName(), is(schemaName));
  }

  @Test
  public void shouldSerializePrimitiveWithSchemaId() throws Exception {
    // Given:
    int boolSchemaId = givenPhysicalSchema(getSRSubject(SOME_TOPIC, false), BOOLEAN_PARSED_AVRO_SCHEMA);
    final Serializer<Boolean> boolSerializer = givenSerializerForSchema(OPTIONAL_BOOLEAN_SCHEMA,
        Boolean.class, Optional.of(boolSchemaId), Optional.empty());

    int intSchemaId = givenPhysicalSchema(getSRSubject(SOME_TOPIC, false), INT_PARSED_AVRO_SCHEMA);
    final Serializer<Integer> intSerializer = givenSerializerForSchema(OPTIONAL_INT32_SCHEMA,
        Integer.class, Optional.of(intSchemaId), Optional.empty());

    // When:
    final byte[] boolBytes = boolSerializer.serialize(SOME_TOPIC, true);
    final byte[] intBytes = intSerializer.serialize(SOME_TOPIC, 123);

    // Then:
    assertThat(deserialize(boolBytes), is(true));
    assertThat(deserialize(intBytes), is(123));
  }

  @Test
  public void shouldSerializeStructWithSchemaId() throws Exception {
    // Given:
    int schemaId = givenPhysicalSchema(getSRSubject(SOME_TOPIC, false), ORDER_PARSED_AVRO_SCHEMA);
    final Serializer<Struct> serializer = givenSerializerForSchema(ORDER_SCHEMA,
        Struct.class, Optional.of(schemaId), Optional.empty());

    // When:
    final byte[] bytes = serializer.serialize(SOME_TOPIC, orderStruct);

    // Then:
    avroOrder = new GenericData.Record(ORDER_AVRO_SCHEMA);
    avroOrder.put(ORDERTIME, 1511897796092L);
    avroOrder.put(ORDERID, 1L);
    avroOrder.put(ITEMID, "item_1");
    avroOrder.put(ORDERUNITS, 10.0);
    avroOrder.put(ARRAYCOL, ImmutableList.of(100.0));
    avroOrder.put(MAPCOL, ImmutableMap.of(new Utf8("key1"), 100.0));
    final GenericRecord deserialized = deserialize(bytes);
    assertThat(deserialized, is(avroOrder));
  }

  @Test
  public void shouldSerializeStructWithExtraFieldWithSchemaId() throws Exception {
    // Given:
    int schemaId = givenPhysicalSchema(getSRSubject(SOME_TOPIC, false),
        ORDER_AVRO_SCHEMA_WITH_EXTRA_FIELD);
    final Serializer<Struct> serializer = givenSerializerForSchema(ORDER_SCHEMA,
        Struct.class, Optional.of(schemaId), Optional.empty());

    // When:
    final byte[] bytes = serializer.serialize(SOME_TOPIC, orderStruct);

    // Then:
    final GenericRecord deserialized = deserialize(bytes);
    assertThat(deserialized, is(avroOrderWithExtraField));
  }

  @Test
  public void shouldSerializeStructWithExtraDefaultFieldWithSchemaId() throws Exception {
    // Given:
    int schemaId = givenPhysicalSchema(getSRSubject(SOME_TOPIC, false),
        ORDER_AVRO_SCHEMA_WITH_DEFAULT_EXTRA_FIELD);
    final Serializer<Struct> serializer = givenSerializerForSchema(ORDER_SCHEMA,
        Struct.class, Optional.of(schemaId), Optional.empty());

    // When:
    final byte[] bytes = serializer.serialize(SOME_TOPIC, orderStruct);

    // Then:
    final GenericRecord deserialized = deserialize(bytes);
    assertThat(deserialized, is(avroOrderWithExtraDefaultField));
  }

  @Test
  public void shouldThrowSerializeNonOptionalStruct() throws Exception {
    // Given:
    int schemaId = givenPhysicalSchema(getSRSubject(SOME_TOPIC, false),
        ORDER_AVRO_SCHEMA_WITH_NON_OPTIONAL_EXTRA_FIELD);
    final Serializer<Struct> serializer = givenSerializerForSchema(ORDER_SCHEMA,
        Struct.class, Optional.of(schemaId), Optional.empty());

    // When:
    final Exception e = assertThrows(
        SerializationException.class,
        () -> serializer.serialize(SOME_TOPIC, orderStruct)
    );

    // Then:
    assertThat(e.getMessage(), containsString("Missing default value for required Avro "
        + "field: [EXTRA]. This field appears in Avro schema in Schema Registry"));
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
        SchemaBuilder.map(OPTIONAL_STRING_SCHEMA, OPTIONAL_INT64_SCHEMA)
            .optional()
            .build(),
        value,
        avroSchema,
        avroValue
    );
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

  private void shouldSerializeFieldTypeCorrectly(
      final Schema ksqlSchema,
      final Object ksqlValue,
      final org.apache.avro.Schema avroSchema,
      final Object avroValue
  ) {
    // Given:
    final Schema schema = SchemaBuilder.struct()
        .field("field0", ksqlSchema)
        .build();

    final Serializer<Struct> serializer = givenSerializerForSchema(schema, Struct.class);

    final Struct ksqlRecord = new Struct(schema)
        .put("field0", ksqlValue);

    // When:
    final byte[] bytes = serializer.serialize(SOME_TOPIC, ksqlRecord);

    // Then:
    final GenericRecord avroRecord = deserialize(bytes);
    assertThat(avroRecord.getSchema().getNamespace(), equalTo(AvroProperties.AVRO_SCHEMA_NAMESPACE));
    assertThat(avroRecord.getSchema().getName(), equalTo(AvroProperties.AVRO_SCHEMA_NAME));
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

  private <T> Serializer<T> givenSerializerForSchema(
      final Schema schema,
      final Class<T> targetType
  ) {
    return givenSerializerForSchema(schema, targetType, Optional.empty(), Optional.empty());
  }

  private <T> Serializer<T> givenSerializerForSchema(
      final Schema schema,
      final Class<T> targetType,
      final Optional<Integer> schemaId,
      final Optional<String> schemaName
  ) {
    final ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
    if (schemaName.isPresent()) {
      builder.put(ConnectProperties.FULL_SCHEMA_NAME, schemaName.get());
    } else {
      builder.put(ConnectProperties.FULL_SCHEMA_NAME, AvroProperties.DEFAULT_AVRO_SCHEMA_FULL_NAME);
    }
    schemaId.ifPresent(integer -> builder.put(ConnectProperties.SCHEMA_ID, String.valueOf(integer)));
    return new KsqlAvroSerdeFactory(builder.build())
        .createSerde(
            (ConnectSchema) schema,
            ksqlConfig,
            () -> schemaRegistryClient,
            targetType,
            false).serializer();
  }

  private int givenPhysicalSchema(
      final String subject,
      final AvroSchema physicalSchema
  ) throws Exception {
    return schemaRegistryClient.register(subject, physicalSchema);
  }

  private org.apache.avro.Schema avroSchemaStoredInSchemaRegistry() {
    try {
      final SchemaMetadata schemaMetadata = schemaRegistryClient
          .getLatestSchemaMetadata(getSRSubject(SOME_TOPIC, false));

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
    entries.getSchema().addProp(
        "connect.name", AvroProperties.AVRO_SCHEMA_NAMESPACE + "." + entrySchema.getName());
    return entries;
  }
}
