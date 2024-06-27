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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.kafka.connect.data.Schema.OPTIONAL_STRING_SCHEMA;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.google.protobuf.Timestamp;
import com.google.type.Date;
import com.google.type.TimeOfDay;
import io.confluent.connect.protobuf.ProtobufData;
import io.confluent.connect.protobuf.ProtobufDataConfig;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.ksql.util.DecimalUtil;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.protobuf.type.Decimal;
import io.confluent.protobuf.type.utils.DecimalUtils;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Collections;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@SuppressWarnings({"SameParameterValue", "unchecked", "checkstyle:ClassDataAbstractionCoupling"})
@RunWith(MockitoJUnitRunner.class)
public class KsqlProtobufNoSRSerializerTest {

  private static final ParsedSchema DECIMAL_SCHEMA =
          parseProtobufSchema(
                  "syntax = \"proto3\";\n"
                          + "\n"
                          + "import \"confluent/meta.proto\";\n"
                          + "import \"confluent/type/decimal.proto\";\n"
                          + "\n"
                          + "message DecimalValue {\n"
                          + "  confluent.type.Decimal field0 = 1 [(confluent.field_meta) = { "
                          + "params: [\n"
                          + "    { key: \"precision\", value: \"4\" },\n"
                          + "    { key: \"scale\", value: \"2\" }\n"
                          + "  ]}];\n"
                          + "}\n");
  private static final ParsedSchema TIME_SCHEMA =
      parseProtobufSchema(
          "syntax = \"proto3\";\n"
              + "\n"
              + "import \"google/type/TimeOfDay.proto\";\n"
              + "\n"
              + "message ConnectDefault1 {google.type.TimeOfDay F1 = 1;}\n");
  private static final ParsedSchema TIMESTAMP_SCHEMA =
      parseProtobufSchema(
          "syntax = \"proto3\";\n"
              + "\n"
              + "import \"google/protobuf/timestamp.proto\";\n"
              + "\n"
              + "message ConnectDefault1 {google.protobuf.Timestamp F1 = 1;}\n");

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

  private static final String SOME_TOPIC = "bob";

  @SuppressWarnings("checkstyle:Indentation")
  private static final ProtobufNoSRConverter.Deserializer deserializer =
      new ProtobufNoSRConverter.Deserializer();

  @Test
  public void shouldUseNestedStructNames() throws Exception {
    // Given:
    final String schemaName = "TestSchemaName1";
    final String schemaNamespace = "com.test.namespace";

    final Schema internalStructSchema = SchemaBuilder.struct()
        .field("field0_1", OPTIONAL_STRING_SCHEMA)
        .build();

    final Schema internalArraySchema = SchemaBuilder.array(internalStructSchema).build();

    final Schema internalMapSchema = SchemaBuilder.map(
        OPTIONAL_STRING_SCHEMA,
        internalStructSchema
    ).build();

    final ConnectSchema ksqlRecordSchema = (ConnectSchema) SchemaBuilder.struct()
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
        new ProtobufNoSRSerdeFactory(ImmutableMap.of()).createSerde(
            ksqlRecordSchema,
            new KsqlConfig(ImmutableMap.of()),
            () -> null,
            Struct.class,
            false).serializer();

    // When:
    final byte[] bytes = serializer.serialize(SOME_TOPIC, ksqlRecord);

    // Then:
    final Message deserialized = deserialize(bytes, ksqlRecordSchema);

    assertThat(deserialized.toString(), is("field0 {\n"
        + "  field0_1: \"foobar\"\n"
        + "}\n"
        + "field1 {\n"
        + "  field0_1: \"arrValue1\"\n"
        + "}\n"
        + "field1 {\n"
        + "  field0_1: \"arrValue2\"\n"
        + "}\n"
        + "field2 {\n"
        + "  key: \"key1\"\n"
        + "  value {\n"
        + "    field0_1: \"mapValue1\"\n"
        + "  }\n"
        + "}\n"));
  }

  @Test
  public void shouldUseSchemaNameFromPropertyIfExists() throws Exception {
    // Given:
    final String schemaName = "TestSchemaName1";
    final String schemaNamespace = "com.test.namespace";

    final Object ksqlValue = "foobar";

    final Schema ksqlRecordSchema = SchemaBuilder.struct()
        .field("field0", OPTIONAL_STRING_SCHEMA)
        .build();

    final Struct ksqlRecord = new Struct(ksqlRecordSchema)
        .put("field0", ksqlValue);

    final Serializer<Struct> serializer =
        new ProtobufNoSRSerdeFactory(ImmutableMap.of()).createSerde(
            ksqlRecordSchema,
            new KsqlConfig(ImmutableMap.of()),
            () -> null,
            Struct.class,
            false).serializer();

    // When:
    final byte[] bytes = serializer.serialize(SOME_TOPIC, ksqlRecord);

    // Then:
    final Message deserialized = deserialize(bytes, ksqlRecordSchema);
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
    final Message record = serializeValue(Schema.BYTES_SCHEMA,
        ByteBuffer.wrap("abc".getBytes(UTF_8)));
    assertThat(record.getAllFields().size(), equalTo(1));
    final Descriptors.FieldDescriptor field = record.getDescriptorForType()
        .findFieldByName("field0");
    assertThat(((ByteString) record.getField(field)).toByteArray(), equalTo("abc".getBytes(UTF_8)));
  }

  @SuppressWarnings("unchecked")
  private <T> Deserializer<T> givenDeserializerForSchema(
      final Schema schema,
      final Class<T> targetType
  ) {
    final Deserializer<T> deserializer = new ProtobufNoSRSerdeFactory(ImmutableMap.of())
        .createSerde(
            schema,
            new KsqlConfig(ImmutableMap.of()),
            () -> null,
            targetType,
            false).deserializer();

    deserializer.configure(Collections.emptyMap(), false);

    return deserializer;
  }

  private void shouldSerializeFieldTypeCorrectly(
      final Schema ksqlSchema,
      final Object ksqlValue,
      final ParsedSchema parsedSchema,
      final Object protobufValue
  ) {
    final Message record = serializeValue(ksqlSchema, ksqlValue);
    assertThat(record.getAllFields().size(), equalTo(1));
    final Descriptors.FieldDescriptor field = record
        .getDescriptorForType()
        .findFieldByName("field0");
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
    return deserialize(bytes, schema);
  }

  @SuppressWarnings("unchecked")
  private <T> T deserialize(final byte[] serializedRow, final Schema schema) {
    final ProtobufSchema protobufSchema = new ProtobufData(
        new ProtobufDataConfig(ImmutableMap.of())).fromConnectSchema(schema);
    return (T) deserializer.deserialize(serializedRow, protobufSchema);
  }

  private <T> Serializer<T> givenSerializerForSchema(
      final Schema schema,
      final Class<T> targetType
  ) {
    final Serializer<T> serializer = new ProtobufNoSRSerdeFactory(ImmutableMap.of())
        .createSerde(
            schema,
            new KsqlConfig(ImmutableMap.of()),
            () -> null,
            targetType,
            false).serializer();

    serializer.configure(Collections.emptyMap(), false);

    return serializer;
  }

  private static ParsedSchema parseProtobufSchema(final String protobufSchema) {
    return new ProtobufSchema(protobufSchema);
  }
}
