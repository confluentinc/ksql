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
import static org.junit.Assert.assertThat;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.confluent.connect.avro.AvroData;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlConstants;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.util.Utf8;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

@SuppressWarnings("unchecked")
public class KsqlGenericRowAvroSerializerTest {
  private final SchemaRegistryClient schemaRegistryClient = new MockSchemaRegistryClient();

  private Schema schema = SchemaBuilder.struct()
        .field("ordertime".toUpperCase(), Schema.OPTIONAL_INT64_SCHEMA)
        .field("orderid".toUpperCase(), Schema.OPTIONAL_INT64_SCHEMA)
        .field("itemid".toUpperCase(), Schema.OPTIONAL_STRING_SCHEMA)
        .field("orderunits".toUpperCase(), Schema.OPTIONAL_FLOAT64_SCHEMA)
        .field(
            "arraycol".toUpperCase(),
            SchemaBuilder.array(Schema.OPTIONAL_FLOAT64_SCHEMA).optional().build())
        .field(
            "mapcol".toUpperCase(),
            SchemaBuilder.map(
                Schema.STRING_SCHEMA,
                Schema.OPTIONAL_FLOAT64_SCHEMA).optional().build())
        .optional()
        .build();
  private KsqlConfig ksqlConfig = new KsqlConfig(
      ImmutableMap.of(KsqlConfig.KSQL_USE_NAMED_AVRO_MAPS, true)
  );
  private Serializer<GenericRow> serializer;
  private Deserializer<GenericRow> deserializer;

  @Before
  public void setup() {
    resetSerde();
  }

  private void resetSerde() {
    final Serde<GenericRow> serde =
        new KsqlAvroTopicSerDe().getGenericRowSerde(
            schema,
            ksqlConfig,
            false,
            () -> schemaRegistryClient
        );
    serializer = serde.serializer();
    deserializer = serde.deserializer();
  }

  @Test
  public void shouldSerializeRowCorrectly() {
    final List columns = Arrays.asList(
        1511897796092L, 1L, "item_1", 10.0, Arrays.asList(100.0),
        Collections.singletonMap("key1", 100.0));

    final GenericRow genericRow = new GenericRow(columns);
    final byte[] serializedRow = serializer.serialize("t1", genericRow);
    final KafkaAvroDeserializer kafkaAvroDeserializer = new KafkaAvroDeserializer(schemaRegistryClient);
    final GenericRecord genericRecord =
        (GenericRecord) kafkaAvroDeserializer.deserialize("t1", serializedRow);
    Assert.assertNotNull(genericRecord);
    assertThat("Incorrect serialization.", genericRecord.get("ordertime".toUpperCase()), equalTo
        (1511897796092L));
    assertThat("Incorrect serialization.", genericRecord.get("orderid".toUpperCase()), equalTo
        (1L));
    assertThat("Incorrect serialization.", genericRecord.get("itemid".toUpperCase()).toString(), equalTo("item_1"));
    assertThat("Incorrect serialization.", genericRecord.get("orderunits".toUpperCase()), equalTo
        (10.0));

    final GenericData.Array array = (GenericData.Array) genericRecord.get("arraycol".toUpperCase());
    final Map map = (Map) genericRecord.get("mapcol".toUpperCase());

    assertThat("Incorrect serialization.", array.size(), equalTo(1));
    assertThat("Incorrect serialization.", array.get(0), equalTo(100.0));
    assertThat("Incorrect serialization.", map.size(), equalTo(1));
    assertThat("Incorrect serialization.", map.get(new Utf8("key1")), equalTo(100.0));

  }

  @Test
  public void shouldSerializeRowWithNullCorrectly() {
    final List columns = Arrays.asList(
        1511897796092L, 1L, null, 10.0, Arrays.asList(100.0),
        Collections.singletonMap("key1", 100.0));

    final GenericRow genericRow = new GenericRow(columns);
    final byte[] serializedRow = serializer.serialize("t1", genericRow);
    final KafkaAvroDeserializer kafkaAvroDeserializer = new KafkaAvroDeserializer(schemaRegistryClient);
    final GenericRecord genericRecord =
        (GenericRecord) kafkaAvroDeserializer.deserialize("t1", serializedRow);
    Assert.assertNotNull(genericRecord);
    assertThat("Incorrect serialization.", genericRecord.get("ordertime".toUpperCase()), equalTo
        (1511897796092L));
    assertThat("Incorrect serialization.", genericRecord.get("orderid".toUpperCase()), equalTo
        (1L));
    assertThat("Incorrect serialization.", genericRecord.get("itemid".toUpperCase()), equalTo
        (null));
    assertThat("Incorrect serialization.", genericRecord.get("orderunits".toUpperCase()), equalTo
        (10.0));

    final GenericData.Array array = (GenericData.Array) genericRecord.get("arraycol".toUpperCase());
    final Map map = (Map) genericRecord.get("mapcol".toUpperCase());

    assertThat("Incorrect serialization.", array.size(), equalTo(1));
    assertThat("Incorrect serialization.", array.get(0), equalTo(100.0));
    assertThat("Incorrect serialization.", map,
               equalTo(Collections.singletonMap(new Utf8("key1"), 100.0)));

  }

  @Test
  @SuppressWarnings("unchecked")
  public void shouldSerializeRowWithNullValues() {
    final List columns = Arrays.asList(1511897796092L, 1L, "item_1", 10.0, null, null);

    final GenericRow genericRow = new GenericRow(columns);
    serializer.serialize("t1", genericRow);
  }

  @Test
  public void shouldFailForIncompatibleType() {
    final List columns = Arrays.asList(
        1511897796092L, 1L, "item_1", "10.0", Arrays.asList((Double)100.0),
        Collections.singletonMap("key1", 100.0));

    final GenericRow genericRow = new GenericRow(columns);
    try {
      serializer.serialize("t1", genericRow);
      Assert.fail("Did not fail for incompatible types.");
    } catch (final DataException e) {
    }
  }

  private void shouldSerializeTypeCorrectly(final Schema ksqlSchema,
                                            final Object ksqlValue,
                                            final org.apache.avro.Schema avroSchema) {
    shouldSerializeTypeCorrectly(ksqlSchema, ksqlValue, avroSchema, ksqlValue);
  }

  private void shouldSerializeTypeCorrectly(final Schema ksqlSchema,
                                            final Object ksqlValue,
                                            final org.apache.avro.Schema avroSchema,
                                            final Object avroValue) {
    // Given:
    schema = SchemaBuilder.struct()
        .field("field0", ksqlSchema)
        .build();
    resetSerde();
    final GenericRow ksqlRecord = new GenericRow(ImmutableList.of(ksqlValue));

    // When:
    final byte[] bytes = serializer.serialize("topic", ksqlRecord);

    // Then:
    final KafkaAvroDeserializer deserializer = new KafkaAvroDeserializer(schemaRegistryClient);
    final GenericRecord avroRecord = (GenericRecord) deserializer.deserialize("topic", bytes);
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
    final GenericRow deserializedKsqlRecord = this.deserializer.deserialize("topic", bytes);
    assertThat(deserializedKsqlRecord, equalTo(ksqlRecord));
  }

  @Test
  public void shouldSerializeInteger() {
    shouldSerializeTypeCorrectly(
        Schema.OPTIONAL_INT32_SCHEMA,
        123,
        org.apache.avro.SchemaBuilder.builder().intType());
  }

  @Test
  public void shouldSerializeBigint() {
    shouldSerializeTypeCorrectly(
        Schema.OPTIONAL_INT64_SCHEMA,
        123L,
        org.apache.avro.SchemaBuilder.builder().longType());
  }

  @Test
  public void shouldSerializeBoolean() {
    shouldSerializeTypeCorrectly(
        Schema.OPTIONAL_BOOLEAN_SCHEMA,
        false,
        org.apache.avro.SchemaBuilder.builder().booleanType());
    shouldSerializeTypeCorrectly(
        Schema.OPTIONAL_BOOLEAN_SCHEMA,
        true,
        org.apache.avro.SchemaBuilder.builder().booleanType());
  }

  @Test
  public void shouldSerializeString() {
    shouldSerializeTypeCorrectly(
        Schema.OPTIONAL_STRING_SCHEMA,
        "foobar",
        org.apache.avro.SchemaBuilder.builder().stringType(),
        new Utf8("foobar"));
  }

  @Test
  public void shouldSerializeDouble() {
    shouldSerializeTypeCorrectly(
        Schema.OPTIONAL_FLOAT64_SCHEMA,
        1.23456789012345,
        org.apache.avro.SchemaBuilder.builder().doubleType());
  }

  @Test
  public void shouldSerializeArray() {
    shouldSerializeTypeCorrectly(
        SchemaBuilder.array(Schema.OPTIONAL_INT32_SCHEMA).optional().build(),
        ImmutableList.of(1, 2, 3),
        org.apache.avro.SchemaBuilder.array().items(
            org.apache.avro.SchemaBuilder.builder()
                .unionOf().nullType().and().intType().endUnion())
    );
  }

  private org.apache.avro.Schema mapEntrySchema(
      final String name,
      final org.apache.avro.Schema valueSchema) {
    return org.apache.avro.SchemaBuilder.record(name)
        .namespace("io.confluent.ksql.avro_schemas")
        .prop("connect.internal.type", "MapEntry")
        .fields()
        .name("key")
        .type().unionOf().nullType().and().stringType().endUnion()
        .nullDefault()
        .name("value")
        .type(valueSchema).withDefault(null)
        .endRecord();
  }

  private org.apache.avro.Schema mapEntrySchema(final String name) {
    return mapEntrySchema(
        name,
        org.apache.avro.SchemaBuilder.unionOf().nullType().and().longType().endUnion());
  }

  private org.apache.avro.Schema mapSchema(final org.apache.avro.Schema entrySchema) {
    return org.apache.avro.SchemaBuilder.array().items(entrySchema);
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
    shouldSerializeTypeCorrectly(
        SchemaBuilder.map(Schema.OPTIONAL_STRING_SCHEMA, Schema.OPTIONAL_INT64_SCHEMA)
            .optional()
            .build(),
        value,
        avroSchema,
        avroValue);
  }

  @Test
  public void shouldSerializeMapWithName() {
    final org.apache.avro.Schema avroSchema = mapSchema(
        mapEntrySchema(KsqlConstants.AVRO_SCHEMA_NAMESPACE + ".KsqlDataSourceSchema_field0"));
    shouldSerializeMap(avroSchema);
  }

  private org.apache.avro.Schema legacyMapEntrySchema() {
    final String name = AvroData.NAMESPACE + "." + AvroData.MAP_ENTRY_TYPE_NAME;
    return org.apache.avro.SchemaBuilder.record(name)
        .fields()
        .name("key")
        .type().unionOf().nullType().and().stringType().endUnion()
        .nullDefault()
        .name("value")
        .type().unionOf().nullType().and().longType().endUnion()
        .nullDefault()
        .endRecord();
  }

  @Test
  public void shouldSerializeMapWithoutNameIfDisabled() {
    ksqlConfig = new KsqlConfig(
        Collections.singletonMap(KsqlConfig.KSQL_USE_NAMED_AVRO_MAPS, false));
    final org.apache.avro.Schema avroSchema = mapSchema(legacyMapEntrySchema());
    shouldSerializeMap(avroSchema);
  }

  @Test
  public void shouldSerializeMultipleMaps() {
    final org.apache.avro.Schema avroInnerSchema0
        = mapEntrySchema(
            KsqlConstants.AVRO_SCHEMA_NAMESPACE + ".KsqlDataSourceSchema_field0_inner0");
    final org.apache.avro.Schema avroInnerSchema1 =
        mapEntrySchema(
            KsqlConstants.AVRO_SCHEMA_NAMESPACE + ".KsqlDataSourceSchema_field0_inner1",
            org.apache.avro.SchemaBuilder.unionOf().nullType().and().stringType().endUnion());
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

    shouldSerializeTypeCorrectly(ksqlSchema, value, avroSchema, avroValue);
  }

  @Test
  public void shouldSerializeStruct() {
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
    shouldSerializeTypeCorrectly(ksqlSchema, value, avroSchema, avroValue);
  }

  @Test
  public void shouldRemoveSourceName() {
    final Schema ksqlRecordSchema = SchemaBuilder.struct()
        .field("source.field0", Schema.OPTIONAL_INT32_SCHEMA)
        .build();

    final GenericRow ksqlRecord = new GenericRow(ImmutableList.of(123));

    final Serde<GenericRow> serde =
        new KsqlAvroTopicSerDe().getGenericRowSerde(
            ksqlRecordSchema,
            new KsqlConfig(Collections.emptyMap()),
            false,
            () -> schemaRegistryClient
        );

    final byte[] bytes = serde.serializer().serialize("topic", ksqlRecord);

    final KafkaAvroDeserializer deserializer = new KafkaAvroDeserializer(schemaRegistryClient);
    final GenericRecord avroRecord = (GenericRecord) deserializer.deserialize("topic", bytes);

    assertThat(avroRecord.getSchema().getFields().size(), equalTo(1));
    assertThat(avroRecord.get("field0"), equalTo(123));

    final GenericRow deserializedKsqlRecord = serde.deserializer().deserialize("topic", bytes);
    assertThat(deserializedKsqlRecord, equalTo(ksqlRecord));
  }

  @Test
  public void shouldTransformSourceNameDelimiterForInternal() {
    final Schema ksqlRecordSchema = SchemaBuilder.struct()
        .field("source.field0", Schema.OPTIONAL_INT32_SCHEMA)
        .build();

    final GenericRow ksqlRecord = new GenericRow(ImmutableList.of(123));

    final Serde<GenericRow> serde =
        new KsqlAvroTopicSerDe().getGenericRowSerde(
            ksqlRecordSchema,
            new KsqlConfig(Collections.emptyMap()),
            true,
            () -> schemaRegistryClient
        );

    final byte[] bytes = serde.serializer().serialize("topic", ksqlRecord);

    final KafkaAvroDeserializer deserializer = new KafkaAvroDeserializer(schemaRegistryClient);
    final GenericRecord avroRecord = (GenericRecord) deserializer.deserialize("topic", bytes);

    assertThat(avroRecord.getSchema().getFields().size(), equalTo(1));
    assertThat(avroRecord.getSchema().getFields().get(0).name(), equalTo("source_field0"));
    assertThat(avroRecord.get("source_field0"), equalTo(123));

    final GenericRow deserializedKsqlRecord = serde.deserializer().deserialize("topic", bytes);
    assertThat(deserializedKsqlRecord, equalTo(ksqlRecord));
  }
}
