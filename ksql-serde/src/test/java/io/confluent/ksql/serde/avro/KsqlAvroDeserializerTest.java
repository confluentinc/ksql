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
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.confluent.connect.avro.AvroConverter;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.ksql.logging.processing.ProcessingLogContext;
import io.confluent.ksql.schema.persistence.PersistenceSchema;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlConstants;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.apache.avro.LogicalTypes;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class KsqlAvroDeserializerTest {

  private static final String AVRO_SCHEMA_STRING = "{"
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

  private static final org.apache.avro.Schema AVRO_SCHEMA = new org.apache.avro.Schema.Parser()
      .parse(AVRO_SCHEMA_STRING);

  private static final Schema SCHEMA = SchemaBuilder.struct()
      .field("ORDERTIME", Schema.OPTIONAL_INT64_SCHEMA)
      .field("ORDERID", Schema.OPTIONAL_INT64_SCHEMA)
      .field("ITEMID", Schema.OPTIONAL_STRING_SCHEMA)
      .field("ORDERUNITS", Schema.OPTIONAL_FLOAT64_SCHEMA)
      .field("ARRAYCOL", SchemaBuilder
          .array(Schema.OPTIONAL_FLOAT64_SCHEMA).optional().build())
      .field("MAPCOL", SchemaBuilder
          .map(Schema.STRING_SCHEMA, Schema.OPTIONAL_FLOAT64_SCHEMA).optional().build())
      .optional()
      .build();

  private static final KsqlConfig KSQL_CONFIG = new KsqlConfig(Collections.singletonMap(
          KsqlConfig.SCHEMA_REGISTRY_URL_PROPERTY, "fake-schema-registry-url"));

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();


  private SchemaRegistryClient schemaRegistryClient;
  private AvroConverter converter;

  @Before
  public void setUp() {
    schemaRegistryClient = new MockSchemaRegistryClient();
    converter = new AvroConverter(schemaRegistryClient);
    converter.configure(
        ImmutableMap.of(
            AbstractKafkaAvroSerDeConfig.AUTO_REGISTER_SCHEMAS, true,
            AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, ""
        ),
        false
    );
  }

  @Test
  public void shouldDeserializeNull() {
    // Given:
    final Deserializer<?> deserializer = deserializer(SCHEMA);

    // When:
    final Object row = deserializer.deserialize("topic", null);

    // Then:
    assertThat(row, is(nullValue()));
  }

  @Test
  public void shouldDeserializeCorrectly() {
    // Given:
    final Struct data = new Struct(SCHEMA)
        .put("ORDERTIME", 1511897796092L)
        .put("ORDERID", 1L)
        .put("ITEMID", "item_1")
        .put("ORDERUNITS", 10.0)
        .put("ARRAYCOL", Collections.singletonList(100.0))
        .put("MAPCOL", Collections.singletonMap("key1", 100.0));

    // When:
    final Struct row = serializeDeserializeRow(SCHEMA, AVRO_SCHEMA, data);

    // Then:
    assertThat(row.schema(), is(SCHEMA));
    assertThat(row.get("ORDERTIME"), equalTo(1511897796092L));
    assertThat(row.get("ORDERID"), equalTo(1L));
    assertThat(row.get("ITEMID"), equalTo("item_1"));
    assertThat(row.get("ORDERUNITS"), equalTo(10.0));
    assertThat(row.get("ARRAYCOL"), equalTo(ImmutableList.of(100.0)));
    assertThat(row.get("MAPCOL"), equalTo(ImmutableMap.of("key1", 100.0)));
  }

  @Test
  public void shouldDeserializeIfThereAreRedundantFields() {
    // Given:
    final Schema newSchema = SchemaBuilder.struct()
        .field("ORDERTIME", Schema.OPTIONAL_INT64_SCHEMA)
        .field("ORDERID", Schema.OPTIONAL_INT64_SCHEMA)
        .field("ITEMID", Schema.OPTIONAL_STRING_SCHEMA)
        .field("ORDERUNITS", Schema.OPTIONAL_FLOAT64_SCHEMA)
        .build();

    final Struct data = new Struct(SCHEMA)
        .put("ORDERTIME", 1511897796092L)
        .put("ORDERID", 1L)
        .put("ITEMID", "item_1")
        .put("ORDERUNITS", 10.0)
        .put("ARRAYCOL", Collections.singletonList(100.0))
        .put("MAPCOL", Collections.singletonMap("key1", 100.0));

    // When:
    final Struct row = serializeDeserializeRow(newSchema, AVRO_SCHEMA, data);

    // Then:
    assertThat(row.schema(), is(newSchema));
    assertThat(row.get("ORDERTIME"), equalTo(1511897796092L));
    assertThat(row.get("ORDERID"), equalTo(1L));
    assertThat(row.get("ITEMID"), equalTo("item_1"));
    assertThat(row.get("ORDERUNITS"), equalTo(10.0));
  }

  @Test
  public void shouldDeserializeWithMissingFields() {
    // Given:
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

    final org.apache.avro.Schema avroSchema1 = new org.apache.avro.Schema.Parser()
        .parse(schemaStr1);

    final Struct data = new Struct(SCHEMA)
        .put("ORDERTIME", 1511897796092L)
        .put("ORDERID", 1L)
        .put("ITEMID", "item_1")
        .put("ORDERUNITS", 10.0);

    // When:
    final Struct row = serializeDeserializeRow(SCHEMA, avroSchema1, data);

    // Then:
    assertThat(row.schema(), is(SCHEMA));
    assertThat(row.get("ORDERTIME"), equalTo(1511897796092L));
    assertThat(row.get("ORDERID"), equalTo(1L));
    assertThat(row.get("ITEMID"), equalTo("item_1"));
    assertThat(row.get("ORDERUNITS"), equalTo(10.0));
    assertThat(row.get("ARRAYCOL"), is(nullValue()));
    assertThat(row.get("MAPCOL"), is(nullValue()));
  }

  private static Struct serializeDeserializeAvroRecord(
      final Schema schema,
      final String topicName,
      final SchemaRegistryClient schemaRegistryClient,
      final GenericRecord avroRecord
  ) {
    final Map<String, Object> map = new HashMap<>();
    map.put(AbstractKafkaAvroSerDeConfig.AUTO_REGISTER_SCHEMAS, true);
    map.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "");
    final KafkaAvroSerializer kafkaAvroSerializer = new KafkaAvroSerializer(schemaRegistryClient, map);

    final byte[] bytes = kafkaAvroSerializer.serialize(topicName, avroRecord);

    final Deserializer<Object> deserializer =
        new KsqlAvroSerdeFactory(KsqlConstants.DEFAULT_AVRO_SCHEMA_FULL_NAME).createSerde(
            PersistenceSchema.of((ConnectSchema) schema),
            KSQL_CONFIG,
            () -> schemaRegistryClient,
            "loggerName",
            ProcessingLogContext.create()).deserializer();

    return (Struct) deserializer.deserialize(topicName, bytes);
  }

  private Struct serializeDeserializeRow(
      final Schema schema,
      final org.apache.avro.Schema rowAvroSchema,
      final Struct row
  ) {
    final GenericRecord avroRecord = new GenericData.Record(rowAvroSchema);

    final Iterator<Field> ksqlIt = row.schema().fields().iterator();

    for (final org.apache.avro.Schema.Field avroField : rowAvroSchema.getFields()) {
      final Field ksqlField = ksqlIt.next();
      avroRecord.put(avroField.name(), row.get(ksqlField));
    }

    return serializeDeserializeAvroRecord(schema, "t1", schemaRegistryClient, avroRecord);
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
        ksqlRecordSchema, "test-topic", schemaRegistryClient, avroRecord);

    assertThat(row.schema(), is(ksqlRecordSchema));
    assertThat(row.get("field0"), equalTo(ksqlValue));
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

    final Deserializer<Object> deserializer = deserializer(ksqlRecordSchema);

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

  private Deserializer<Object> deserializer(final Schema schema) {
    final Deserializer<Object> deserializer =
        new KsqlAvroSerdeFactory(KsqlConstants.DEFAULT_AVRO_SCHEMA_FULL_NAME)
            .createSerde(
                PersistenceSchema.of((ConnectSchema) schema),
                KSQL_CONFIG,
                () -> schemaRegistryClient,
                "loggerName",
                ProcessingLogContext.create())
            .deserializer();

    deserializer
        .configure(Collections.emptyMap(), false);

    return deserializer;
  }
}
