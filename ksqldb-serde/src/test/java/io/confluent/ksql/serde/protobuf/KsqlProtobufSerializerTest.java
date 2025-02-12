/*
 * Copyright 2021 Confluent Inc.
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

package io.confluent.ksql.serde.protobuf;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.google.protobuf.Timestamp;
import com.google.type.Date;
import com.google.type.TimeOfDay;
import io.confluent.ksql.serde.connect.ConnectProperties;
import java.nio.ByteBuffer;
import java.util.Optional;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import java.math.BigDecimal;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer;
import io.confluent.ksql.util.DecimalUtil;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.protobuf.type.Decimal;
import io.confluent.protobuf.type.utils.DecimalUtils;

import static io.confluent.ksql.util.KsqlConstants.getSRSubject;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.kafka.connect.data.Schema.OPTIONAL_STRING_SCHEMA;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;

@SuppressWarnings({"SameParameterValue", "rawtypes", "unchecked"})
@RunWith(MockitoJUnitRunner.class)
public class KsqlProtobufSerializerTest {

  private static final ParsedSchema DECIMAL_SCHEMA =
          parseProtobufSchema(
                  "syntax = \"proto3\";\n" +
                          "\n" +
                          "import \"confluent/meta.proto\";\n" +
                          "import \"confluent/type/decimal.proto\";\n" +
                          "\n" +
                          "message DecimalValue {\n" +
                          "  confluent.type.Decimal field0 = 1 [(confluent.field_meta) = { " +
                          "params: [\n" +
                          "    { key: \"precision\", value: \"4\" },\n" +
                          "    { key: \"scale\", value: \"2\" }\n" +
                          "  ]}];\n" +
                          "}\n");
  private static final ParsedSchema TIME_SCHEMA =
      parseProtobufSchema(
          "syntax = \"proto3\";\n" +
              "\n" +
              "import \"google/type/TimeOfDay.proto\";\n" +
              "\n" +
              "message ConnectDefault1 {google.type.TimeOfDay F1 = 1;}\n");
  private static final ParsedSchema DATE_SCHEMA =
      parseProtobufSchema(
          "syntax = \"proto3\";\n" +
              "\n" +
              "import \"google/type/Date.proto\";\n" +
              "\n" +
              "message ConnectDefault1 {google.type.Date F1 = 1;}\n");
  private static final ParsedSchema TIMESTAMP_SCHEMA =
      parseProtobufSchema(
          "syntax = \"proto3\";\n" +
              "\n" +
              "import \"google/protobuf/timestamp.proto\";\n" +
              "\n" +
              "message ConnectDefault1 {google.protobuf.Timestamp F1 = 1;}\n");
  private static final ParsedSchema PROTOBUF_STRUCT_SCHEMA =
      parseProtobufSchema(
          "syntax = \"proto3\";\n" +
              "\n" +
              "message StructName {\n int64 id = 1;\n" +
              "int32 age = 2;\n" +
              "string name = 3;\n}\n"
      );

  private static final ParsedSchema PROTOBUF_STRUCT_SCHEMA_WITH_EXTRA_FIELD =
      parseProtobufSchema(
          "syntax = \"proto3\";\n" +
              "\n" +
              "message StructName {\n int64 id = 1;\n" +
              "int32 age = 2;\n" +
              "string name = 3;\n" +
              "string address = 4;\n}\n"
      );

  private static final ParsedSchema PROTOBUF_STRUCT_SCHEMA_WITH_MISSING_FIELD =
      parseProtobufSchema(
          "syntax = \"proto3\";\n" +
              "\n" +
              "message StructName {\n int64 id = 1;\n" +
              "string name = 2;\n}\n"
      );

  private static final Schema STRUCT_SCHEMA = SchemaBuilder.struct()
      .field("id", SchemaBuilder.OPTIONAL_INT64_SCHEMA)
      .field("age", SchemaBuilder.OPTIONAL_INT32_SCHEMA)
      .field("name", SchemaBuilder.OPTIONAL_STRING_SCHEMA)
      .name("StructName")
      .optional()
      .build();

  private static final Schema RANDOM_NAME_STRUCT_SCHEMA = SchemaBuilder.struct()
      .field("id", SchemaBuilder.OPTIONAL_INT64_SCHEMA)
      .field("age", SchemaBuilder.OPTIONAL_INT32_SCHEMA)
      .field("name", SchemaBuilder.OPTIONAL_STRING_SCHEMA)
      .name("RandomName")
      .optional()
      .build();

  private static final Struct STRUCT_RECORD = new Struct(STRUCT_SCHEMA)
      .put("id", 123L)
      .put("age", 20)
      .put("name", "bob");

  private static final Struct RANDOM_NAME_STRUCT_RECORD = new Struct(RANDOM_NAME_STRUCT_SCHEMA)
      .put("id", 123L)
      .put("age", 20)
      .put("name", "bob");

  private static final String SOME_TOPIC = "bob";

  private final SchemaRegistryClient schemaRegistryClient = new MockSchemaRegistryClient();

  private final KsqlConfig ksqlConfig = new KsqlConfig(ImmutableMap.of());

  private Deserializer<Object> deserializer;

  @Before
  public void setup() {
    final ImmutableMap<String, Object> configs = ImmutableMap.of(
        AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS, true,
        AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, ""
    );

    deserializer = new KafkaProtobufDeserializer(schemaRegistryClient, configs);
  }

  @Test
  public void shouldUseNestedStructNames() throws Exception {
    // Given:
    final String schemaName = "TestSchemaName1";
    final String schemaNamespace = "com.test.namespace";

    givenPhysicalSchema(getSRSubject(SOME_TOPIC, false),
        parseProtobufSchema(
            "syntax = \"proto3\";\n"
                + "\n"
                + "package com.test.namespace;\n"
                + "\n"
                + "message TestSchemaName1 {\n "
                + "  StructName1 field0 = 1;\n"
                + "  repeated StructName1 field1 = 2;\n"
                + "  repeated MapEntry field2 = 3;\n"
                + "\n"
                + "  message StructName1 {\n"
                + "     string field0_1 = 1;\n"
                + "  }\n"
                + " message MapEntry {\n"
                + "     string key = 1;\n"
                + "     StructName1 value = 2;\n"
                + " }\n"
                + "}\n"
        ));


    final Schema internalStructSchema = SchemaBuilder.struct()
        .field("field0_1", OPTIONAL_STRING_SCHEMA)
        .build();

    final Schema internalArraySchema = SchemaBuilder.array(internalStructSchema).build();

    final Schema internalMapSchema = SchemaBuilder.map(
        OPTIONAL_STRING_SCHEMA,
        internalStructSchema
    ).build();

    final Schema ksqlRecordSchema = SchemaBuilder.struct()
        .field("field0", internalStructSchema)
        .field("field1", internalArraySchema)
        .field("field2", internalMapSchema)
        .build();

    final Struct ksqlRecord = new Struct(ksqlRecordSchema)
        .put("field0", new Struct(internalStructSchema)
            .put("field0_1", "foobar"))
        .put("field1", ImmutableList.of(
            new Struct(internalStructSchema).put("field0_1", "arrValue1"),
            new Struct(internalStructSchema).put("field0_1", "arrValue2")))
        .put("field2", ImmutableMap.of(
            "key1",
            new Struct(internalStructSchema).put("field0_1", "mapValue1")
        ));

    final Serializer<Struct> serializer =
        new ProtobufSerdeFactory(
            ImmutableMap.of(
                ConnectProperties.FULL_SCHEMA_NAME, schemaNamespace + "." + schemaName,
                ConnectProperties.SUBJECT_NAME, SOME_TOPIC + "-value")
        ).createSerde(
            (ConnectSchema) ksqlRecordSchema,
            ksqlConfig,
            () -> schemaRegistryClient,
            Struct.class,
            false).serializer();

    // When:
    final byte[] bytes = serializer.serialize(SOME_TOPIC, ksqlRecord);

    // Then:
    final Message deserialized = deserialize(bytes);

    assertThat(deserialized.toString(), is("field0 {\n" +
        "  field0_1: \"foobar\"\n" +
        "}\n" +
        "field1 {\n" +
        "  field0_1: \"arrValue1\"\n" +
        "}\n" +
        "field1 {\n" +
        "  field0_1: \"arrValue2\"\n" +
        "}\n" +
        "field2 {\n" +
        "  key: \"key1\"\n" +
        "  value {\n" +
        "    field0_1: \"mapValue1\"\n" +
        "  }\n" +
        "}\n"));
  }

  @Test
  public void shouldUseSchemaNameFromPropertyIfExists() throws Exception {
    // Given:
    givenPhysicalSchema(getSRSubject(SOME_TOPIC, false),
        parseProtobufSchema(
            "syntax = \"proto3\";\n"
                + "\n"
                + "package com.test.namespace;\n"
                + "\n"
                + "message TestSchemaName1 {\n "
                + "  string field0 = 1;\n"
                + "}"
                + "\n"
        ));

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
        new ProtobufSerdeFactory(
            ImmutableMap.of(
                ConnectProperties.FULL_SCHEMA_NAME, schemaNamespace + "." + schemaName,
                ConnectProperties.SUBJECT_NAME, SOME_TOPIC + "-value")
        ).createSerde(
            (ConnectSchema) ksqlRecordSchema,
            ksqlConfig,
            () -> schemaRegistryClient,
            Struct.class,
            false).serializer();

    // When:
    final byte[] bytes = serializer.serialize(SOME_TOPIC, ksqlRecord);

    // Then:
    final Message deserialized = deserialize(bytes);

    assertThat(deserialized.getDescriptorForType().getFullName(),
        is(schemaNamespace + "." + schemaName));
    assertThat(deserialized.toString(), is("field0: \"foobar\"\n"));
  }

  @Test
  public void shouldSerializeDecimalField() {
    final BigDecimal decimal = new BigDecimal("12.34");
    final Decimal bytes = DecimalUtils.fromBigDecimal(decimal);
    shouldSerializeFieldTypeCorrectly(
        DecimalUtil.builder(4, 2).build(),
        decimal,
        DECIMAL_SCHEMA,
        bytes
    );
  }

  @Test
  public void shouldSerializeTimeField() {
    shouldSerializeFieldTypeCorrectly(
        org.apache.kafka.connect.data.Time.SCHEMA,
        new java.sql.Time(2000),
        TIME_SCHEMA,
        TimeOfDay.newBuilder().setSeconds(2).build()
    );
  }

  @Test
  public void shouldSerializeDateField() {
    shouldSerializeFieldTypeCorrectly(
        org.apache.kafka.connect.data.Date.SCHEMA,
        new java.sql.Date(864000000L),
        TIME_SCHEMA,
        Date.newBuilder().setMonth(1).setDay(11).setYear(1970).build()
    );
  }

  @Test
  public void shouldSerializeTimestampField() {
    shouldSerializeFieldTypeCorrectly(
        org.apache.kafka.connect.data.Timestamp.SCHEMA,
        new java.sql.Timestamp(2000),
        TIMESTAMP_SCHEMA,
        Timestamp.newBuilder().setSeconds(2).setNanos(0).build()
    );
  }

  @Test
  public void shouldSerializeBytesField() {
    final Message record = serializeValue(Schema.BYTES_SCHEMA, ByteBuffer.wrap("abc".getBytes(UTF_8)));
    assertThat(record.getAllFields().size(), equalTo(1));
    Descriptors.FieldDescriptor field = record.getDescriptorForType().findFieldByName("field0");
    assertThat(((ByteString) record.getField(field)).toByteArray(), equalTo("abc".getBytes(UTF_8)));
  }

  @Test
  public void shouldSerializeStructWithSchemaId() throws Exception {
    // Given:
    int schemaId = givenPhysicalSchema(getSRSubject(SOME_TOPIC, false), PROTOBUF_STRUCT_SCHEMA);
    final Serializer<Struct> serializer = givenSerializerForSchema(STRUCT_SCHEMA,
        Struct.class, Optional.of(schemaId), Optional.of("StructName"));

    // When:
    final byte[] bytes = serializer.serialize(SOME_TOPIC, STRUCT_RECORD);
    final Message deserialized = deserialize(bytes);

    // Then:
    assertThat(deserialized.getField(
        deserialized.getDescriptorForType().findFieldByName("id")), is(123L));
    assertThat(deserialized.getField(
        deserialized.getDescriptorForType().findFieldByName("age")), is(20));
    assertThat(deserialized.getField(
        deserialized.getDescriptorForType().findFieldByName("name")), is("bob"));
  }

  @Test
  public void shouldSerializeStructWithExtraFieldWithSchemaId() throws Exception {
    // Given:
    int schemaId = givenPhysicalSchema(getSRSubject(SOME_TOPIC, false),
        PROTOBUF_STRUCT_SCHEMA_WITH_EXTRA_FIELD);
    final Serializer<Struct> serializer = givenSerializerForSchema(STRUCT_SCHEMA,
        Struct.class, Optional.of(schemaId), Optional.of("StructName"));

    // When:
    final byte[] bytes = serializer.serialize(SOME_TOPIC, STRUCT_RECORD);
    final Message deserialized = deserialize(bytes);

    // Then:
    assertThat(deserialized.getField(
        deserialized.getDescriptorForType().findFieldByName("id")), is(123L));
    assertThat(deserialized.getField(
        deserialized.getDescriptorForType().findFieldByName("age")), is(20));
    assertThat(deserialized.getField(
        deserialized.getDescriptorForType().findFieldByName("name")), is("bob"));
    assertThat(deserialized.getField(
        deserialized.getDescriptorForType().findFieldByName("address")), is(""));
  }

  @Test
  public void shouldThrowIfSchemaMissesFieldNotCompatible() throws Exception {
    // Given:
    int schemaId = givenPhysicalSchema(getSRSubject(SOME_TOPIC, false),
        PROTOBUF_STRUCT_SCHEMA_WITH_MISSING_FIELD);
    final Serializer<Struct> serializer = givenSerializerForSchema(RANDOM_NAME_STRUCT_SCHEMA,
        Struct.class, Optional.of(schemaId), Optional.empty());

    // When:
    final Exception e = assertThrows(
        SerializationException.class,
        () -> serializer.serialize(SOME_TOPIC, RANDOM_NAME_STRUCT_RECORD)
    );

    // Then:
    assertThat(e.getMessage(),
        containsString("Schema from Schema Registry misses field with name: age"));
  }

  @SuppressWarnings("unchecked")
  private <T> T deserialize(final byte[] serializedRow) {
    return (T) deserializer.deserialize(SOME_TOPIC, serializedRow);
  }

  private void shouldSerializeFieldTypeCorrectly(
      final Schema ksqlSchema,
      final Object ksqlValue,
      final ParsedSchema parsedSchema,
      final Object protobufValue
  ) {
    final Message record = serializeValue(ksqlSchema, ksqlValue);
    assertThat(record.getAllFields().size(), equalTo(1));
    Descriptors.FieldDescriptor field = record.getDescriptorForType().findFieldByName("field0");
    assertThat(record.getField(field).toString(), equalTo(protobufValue.toString()));
  }

  private Message serializeValue(final Schema ksqlSchema, final Object ksqlValue) {
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
    return deserialize(bytes);
  }

  private int givenPhysicalSchema(
      final String subject,
      final ParsedSchema physicalSchema
  ) throws Exception {
    return schemaRegistryClient.register(subject, physicalSchema);
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
    schemaName.ifPresent(s -> builder.put(ConnectProperties.FULL_SCHEMA_NAME, s));
    schemaId.ifPresent(integer -> builder.put(ConnectProperties.SCHEMA_ID, String.valueOf(integer)));
    return new ProtobufSerdeFactory(builder.build())
        .createSerde(
            (ConnectSchema) schema,
            ksqlConfig,
            () -> schemaRegistryClient,
            targetType,
            false).serializer();
  }

  private static ParsedSchema parseProtobufSchema(final String protobufSchema) {
    return new ProtobufSchema(protobufSchema);
  }
}
