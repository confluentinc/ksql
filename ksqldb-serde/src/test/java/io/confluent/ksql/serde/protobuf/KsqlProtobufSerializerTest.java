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

import com.google.common.collect.ImmutableMap;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.google.protobuf.Timestamp;
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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

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
  private static final ParsedSchema TIMESTAMP_SCHEMA =
      parseProtobufSchema(
          "syntax = \"proto3\";\n" +
              "\n" +
              "import \"google/protobuf/timestamp.proto\";\n" +
              "\n" +
              "message ConnectDefault1 {google.protobuf.Timestamp F1 = 1;}\n");

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
  public void shouldSerializeTimestampField() {
    shouldSerializeFieldTypeCorrectly(
        org.apache.kafka.connect.data.Timestamp.SCHEMA,
        new java.sql.Timestamp(2000),
        TIMESTAMP_SCHEMA,
        Timestamp.newBuilder().setSeconds(2).setNanos(0).build()
    );
  }


  @SuppressWarnings("unchecked")
  private <T> T deserialize(final byte[] serializedRow) {
    return (T) deserializer.deserialize(SOME_TOPIC, serializedRow);
  }

  private void shouldSerializeFieldTypeCorrectly(
      final Schema ksqlSchema,
      final Object ksqlValue,
      final ParsedSchema parsedSchema,
      final Message protobufValue
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
    final Message record = deserialize(bytes);
    assertThat(record.getAllFields().size(), equalTo(1));
    Descriptors.FieldDescriptor field = record.getDescriptorForType().findFieldByName("field0");
    assertThat(record.getField(field).toString(), equalTo(protobufValue.toString()));
  }

  private <T> Serializer<T> givenSerializerForSchema(
      final Schema schema,
      final Class<T> targetType
  ) {
    return ProtobufSerdeFactory
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
