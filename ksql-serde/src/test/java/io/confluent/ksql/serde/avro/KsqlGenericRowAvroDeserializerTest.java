/**
 * Copyright 2017 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.confluent.ksql.serde.avro;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.confluent.connect.avro.AvroConverter;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.util.KsqlConfig;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.LogicalTypes;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.Assert;
import org.junit.Test;


public class KsqlGenericRowAvroDeserializerTest {
  private final String schemaStr = "{"
                     + "\"namespace\": \"kql\","
                     + " \"name\": \"orders\","
                     + " \"type\": \"record\","
                     + " \"fields\": ["
                     + "     {\"name\": \"orderTime\", \"type\": \"long\"},"
                     + "     {\"name\": \"orderId\",  \"type\": \"long\"},"
                     + "     {\"name\": \"itemId\", \"type\": \"string\"},"
                     + "     {\"name\": \"orderUnits\", \"type\": \"double\"},"
                     + "     {\"name\": \"arrayCol\", \"type\": {\"type\": \"array\", \"items\": "
                     + "\"double\"}},"
                     + "     {\"name\": \"mapCol\", \"type\": {\"type\": \"map\", \"values\": "
                     + "\"double\"}}"
                     + " ]"
                     + "}";

  private final Schema schema;
  private final org.apache.avro.Schema avroSchema;
  private final KsqlConfig ksqlConfig;

  public KsqlGenericRowAvroDeserializerTest() {
    final org.apache.avro.Schema.Parser parser = new org.apache.avro.Schema.Parser();
    avroSchema = parser.parse(schemaStr);
    schema = SchemaBuilder.struct()
        .field("ORDERTIME".toUpperCase(), Schema.OPTIONAL_INT64_SCHEMA)
        .field("ORDERID".toUpperCase(), Schema.OPTIONAL_INT64_SCHEMA)
        .field("ITEMID".toUpperCase(), Schema.OPTIONAL_STRING_SCHEMA)
        .field("ORDERUNITS".toUpperCase(), Schema.OPTIONAL_FLOAT64_SCHEMA)
        .field(
            "ARRAYCOL".toUpperCase(),
            SchemaBuilder.array(Schema.OPTIONAL_FLOAT64_SCHEMA).optional().build())
        .field(
            "MAPCOL".toUpperCase(),
            SchemaBuilder.map(
                Schema.STRING_SCHEMA,
                Schema.OPTIONAL_FLOAT64_SCHEMA).optional().build())
        .optional()
        .build();

    ksqlConfig = new KsqlConfig(
        Collections.singletonMap(
            KsqlConfig.SCHEMA_REGISTRY_URL_PROPERTY,
            "fake-schema-registry-url"));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void shouldDeserializeCorrectly() {
    final SchemaRegistryClient schemaRegistryClient = new MockSchemaRegistryClient();
    final List<Object> columns = Arrays.asList(
        1511897796092L, 1L, "item_1", 10.0, Collections.singletonList(100.0),
        Collections.singletonMap("key1", 100.0));

    final GenericRow genericRow = new GenericRow(columns);
    final GenericRow row = serializeDeserializeRow(
        schema, "t1", schemaRegistryClient, avroSchema, genericRow);
    Assert.assertNotNull(row);
    assertThat("Incorrect deserializarion", row.getColumns().size(), equalTo(6));
    assertThat("Incorrect deserializarion", row.getColumns().get(0), equalTo(1511897796092L));
    assertThat("Incorrect deserializarion", row.getColumns().get(1), equalTo
        (1L));
    assertThat("Incorrect deserializarion", row.getColumns().get(2), equalTo
        ( "item_1"));
    assertThat("Incorrect deserializarion", row.getColumns().get(3), equalTo
        ( 10.0));
    assertThat("Incorrect deserializarion", ((List<Double>)row.getColumns().get(4)).size(), equalTo
        (1));
    assertThat("Incorrect deserializarion", ((Map)row.getColumns().get(5)).size(), equalTo
        (1));
  }

  @Test
  public void shouldDeserializeIfThereAreRedundantFields() {
    final Schema newSchema = SchemaBuilder.struct()
        .field("ordertime".toUpperCase(), Schema.OPTIONAL_INT64_SCHEMA)
        .field("orderid".toUpperCase(), Schema.OPTIONAL_INT64_SCHEMA)
        .field("itemid".toUpperCase(), Schema.OPTIONAL_STRING_SCHEMA)
        .field("orderunits".toUpperCase(), Schema.OPTIONAL_FLOAT64_SCHEMA)
        .build();
    final SchemaRegistryClient schemaRegistryClient = new MockSchemaRegistryClient();
    final List<Object> columns = Arrays.asList(
        1511897796092L, 1L, "item_1", 10.0, Collections.emptyList(), Collections.emptyMap());

    final GenericRow genericRow = new GenericRow(columns);
    final GenericRow row = serializeDeserializeRow(
        newSchema, "t1", schemaRegistryClient, avroSchema, genericRow);

    Assert.assertNotNull(row);
    assertThat("Incorrect deserializarion", row.getColumns().size(), equalTo(4));
    assertThat("Incorrect deserializarion", row.getColumns().get(0), equalTo(1511897796092L));
    assertThat("Incorrect deserializarion", row.getColumns().get(1), equalTo
        (1L));
    assertThat("Incorrect deserializarion", row.getColumns().get(2), equalTo
        ( "item_1"));
  }


  @Test
  public void shouldDeserializeWithMissingFields() {
    final String schemaStr1 = "{"
                        + "\"namespace\": \"kql\","
                        + " \"name\": \"orders\","
                        + " \"type\": \"record\","
                        + " \"fields\": ["
                        + "     {\"name\": \"orderTime\", \"type\": \"long\"},"
                        + "     {\"name\": \"orderId\",  \"type\": \"long\"},"
                        + "     {\"name\": \"itemId\", \"type\": \"string\"},"
                        + "     {\"name\": \"orderUnits\", \"type\": \"double\"}"
                        + " ]"
                        + "}";
    final org.apache.avro.Schema.Parser parser = new org.apache.avro.Schema.Parser();
    final org.apache.avro.Schema avroSchema1 = parser.parse(schemaStr1);
    final SchemaRegistryClient schemaRegistryClient = new MockSchemaRegistryClient();
    final List<Object> columns = Arrays.asList(1511897796092L, 1L, "item_1", 10.0);

    final GenericRow genericRow = new GenericRow(columns);
    final GenericRow row = serializeDeserializeRow(
        schema, "t1", schemaRegistryClient, avroSchema1, genericRow);

    assertThat("Incorrect deserializarion", row.getColumns().size(), equalTo(6));
    assertThat("Incorrect deserializarion", row.getColumns().get(0), equalTo(1511897796092L));
    assertThat("Incorrect deserializarion", row.getColumns().get(1), equalTo
        (1L));
    assertThat("Incorrect deserializarion", row.getColumns().get(2), equalTo
        ( "item_1"));
    Assert.assertNull(row.getColumns().get(4));
    Assert.assertNull(row.getColumns().get(5));
  }

  private GenericRow serializeDeserializeAvroRecord(final Schema schema,
                                                    final String topicName,
                                                    final SchemaRegistryClient schemaRegistryClient,
                                                    final GenericRecord avroRecord) {
    final Map<String, Object> map = new HashMap<>();
    map.put(AbstractKafkaAvroSerDeConfig.AUTO_REGISTER_SCHEMAS, true);
    map.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "");
    final KafkaAvroSerializer kafkaAvroSerializer = new KafkaAvroSerializer(schemaRegistryClient, map);

    final byte[] bytes = kafkaAvroSerializer.serialize(topicName, avroRecord);

    final Deserializer<GenericRow> deserializer =
        new KsqlAvroTopicSerDe().getGenericRowSerde(
            schema, ksqlConfig, false,
            () -> schemaRegistryClient).deserializer();

    return deserializer.deserialize(topicName, bytes);
  }

  private GenericRow serializeDeserializeRow(final Schema schema,
                                             final String topicName,
                                             final SchemaRegistryClient schemaRegistryClient,
                                             final org.apache.avro.Schema rowAvroSchema,
                                             final GenericRow genericRow) {
    final GenericRecord avroRecord = new GenericData.Record(rowAvroSchema);
    final List<org.apache.avro.Schema.Field> fields = rowAvroSchema.getFields();
    for (int i = 0; i < genericRow.getColumns().size(); i++) {
      avroRecord.put(fields.get(i).name(), genericRow.getColumns().get(i));
    }
    return serializeDeserializeAvroRecord(schema, topicName, schemaRegistryClient, avroRecord);
  }

  private void shouldDeserializeTypeCorrectly(final org.apache.avro.Schema avroSchema,
                                              final Object avroValue,
                                              final Schema ksqlSchema) {
    shouldDeserializeTypeCorrectly(avroSchema, avroValue, ksqlSchema, avroValue);
  }

  private void shouldDeserializeTypeCorrectly(final org.apache.avro.Schema avroSchema,
                                              final Object avroValue,
                                              final Schema ksqlSchema,
                                              final Object ksqlValue) {
    final SchemaRegistryClient schemaRegistryClient = new MockSchemaRegistryClient();

    final org.apache.avro.Schema avroRecordSchema = org.apache.avro.SchemaBuilder.record("test_row")
        .fields()
        .name("field0")
        .type(avroSchema)
        .noDefault()
        .endRecord();
    final Schema ksqlRecordSchema = SchemaBuilder.struct().field("field0", ksqlSchema).build();

    final GenericRecord avroRecord = new GenericData.Record(avroRecordSchema);
    avroRecord.put("field0", avroValue);

    final GenericRow row = serializeDeserializeAvroRecord(
        ksqlRecordSchema, "test-topic", schemaRegistryClient, avroRecord);

    assertThat(row.getColumns().size(), equalTo(1));
    assertThat(row.getColumns().get(0), equalTo(ksqlValue));
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

  private void shouldDeserializeConnectTypeCorrectly(final Schema connectSchema,
                                                     final Object connectValue,
                                                     final Schema ksqlSchema,
                                                     final Object ksqlValue) {
    final SchemaRegistryClient schemaRegistryClient = new MockSchemaRegistryClient();

    final Schema connectRecordSchema = SchemaBuilder.struct()
        .field("field0", connectSchema)
        .build();
    final Struct connectRecord = new Struct(connectRecordSchema);
    connectRecord.put("field0", connectValue);

    final AvroConverter converter = new AvroConverter(schemaRegistryClient);
    converter.configure(
        ImmutableMap.of(
            AbstractKafkaAvroSerDeConfig.AUTO_REGISTER_SCHEMAS, true,
            AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, ""
        ),
        false
    );

    final byte[] bytes = converter.fromConnectData("topic", connectRecordSchema, connectRecord);

    final Schema ksqlRecordSchema = SchemaBuilder.struct()
        .field("field0", ksqlSchema)
        .optional()
        .build();

    final Deserializer<GenericRow> deserializer =
        new KsqlAvroTopicSerDe().getGenericRowSerde(
            ksqlRecordSchema, ksqlConfig, false,
            () -> schemaRegistryClient).deserializer();

    final GenericRow row = deserializer.deserialize("topic", bytes);

    assertThat(row.getColumns().size(), equalTo(1));
    assertThat(row.getColumns().get(0), equalTo(ksqlValue));
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
}
