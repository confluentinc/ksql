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
import java.nio.ByteBuffer;
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

import java.math.BigDecimal;
import java.util.Collections;

import io.confluent.connect.protobuf.ProtobufConverter;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.ksql.util.DecimalUtil;
import io.confluent.ksql.util.KsqlConfig;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@SuppressWarnings("rawtypes")
@RunWith(MockitoJUnitRunner.class)
public class KsqlProtobufDeserializerTest {

  private static final String SOME_TOPIC = "bob";

  private static final KsqlConfig KSQL_CONFIG = new KsqlConfig(Collections.singletonMap(
      KsqlConfig.SCHEMA_REGISTRY_URL_PROPERTY, "fake-schema-registry-url"));

  private SchemaRegistryClient schemaRegistryClient;
  private ProtobufConverter converter;

  @Before
  public void setUp() {
    final ImmutableMap<String, Object> configs = ImmutableMap.of(
        AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS, true,
        AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, ""
    );

    schemaRegistryClient = new MockSchemaRegistryClient();

    converter = new ProtobufConverter(schemaRegistryClient);
    converter.configure(configs, false);
  }

  @Test
  public void shouldDeserializeDecimalField() {
    final ConnectSchema schema = (ConnectSchema) SchemaBuilder.struct()
            .field("f0", DecimalUtil.builder(10, 2))
            .build();

    // Given:
    final Deserializer<Struct> deserializer =
        givenDeserializerForSchema(schema,
            Struct.class);
    final Struct value = new Struct(schema).put("f0", new BigDecimal("12.34"));
    final byte[] bytes = givenConnectSerialized(value, schema);

    // When:
    final Object result = deserializer.deserialize(SOME_TOPIC, bytes);

    // Then:
    assertThat(result, is(value));
  }

  @Test
  public void shouldDeserializeTimeField() {
    final ConnectSchema schema = (ConnectSchema) SchemaBuilder.struct()
        .field("f0", Time.SCHEMA)
        .build();

    // Given:
    final Deserializer<Struct> deserializer =
        givenDeserializerForSchema(schema,
            Struct.class);
    final Struct value = new Struct(schema).put("f0", new java.sql.Time(2000));
    final byte[] bytes = givenConnectSerialized(value, schema);

    // When:
    final Object result = deserializer.deserialize(SOME_TOPIC, bytes);

    // Then:
    assertThat(result, is(value));
  }

  @Test
  public void shouldDeserializeDateField() {
    final ConnectSchema schema = (ConnectSchema) SchemaBuilder.struct()
        .field("f0", Date.SCHEMA)
        .build();

    // Given:
    final Deserializer<Struct> deserializer =
        givenDeserializerForSchema(schema,
            Struct.class);
    final Struct value = new Struct(schema).put("f0", new java.sql.Date(864000000L));
    final byte[] bytes = givenConnectSerialized(value, schema);

    // When:
    final Object result = deserializer.deserialize(SOME_TOPIC, bytes);

    // Then:
    assertThat(result, is(value));
  }

  @Test
  public void shouldDeserializeTimestampField() {
    final ConnectSchema schema = (ConnectSchema) SchemaBuilder.struct()
        .field("f0", Timestamp.SCHEMA)
        .build();

    // Given:
    final Deserializer<Struct> deserializer =
        givenDeserializerForSchema(schema,
            Struct.class);
    final Struct value = new Struct(schema).put("f0", new java.sql.Timestamp(2000));
    final byte[] bytes = givenConnectSerialized(value, schema);

    // When:
    final Object result = deserializer.deserialize(SOME_TOPIC, bytes);

    // Then:
    assertThat(result, is(value));
  }

  @Test
  public void shouldDeserializeBytesField() {
    final ConnectSchema schema = (ConnectSchema) SchemaBuilder.struct()
        .field("f0", Schema.BYTES_SCHEMA)
        .build();

    // Given:
    final Deserializer<Struct> deserializer =
        givenDeserializerForSchema(schema,
            Struct.class);
    final Struct value = new Struct(schema).put("f0", ByteBuffer.wrap(new byte[] {123}));
    final byte[] bytes = givenConnectSerialized(value, schema);

    // When:
    final Object result = deserializer.deserialize(SOME_TOPIC, bytes);

    // Then:
    assertThat(((Struct) result).getBytes("f0"), is(value.getBytes("f0")));
  }

  private byte[] givenConnectSerialized(
          final Object value,
          final Schema connectSchema
  ) {
    return serializeAsBinaryProtobuf(SOME_TOPIC, connectSchema, value);
  }

  private byte[] serializeAsBinaryProtobuf(
          final String topicName,
          final Schema schema,
          final Object value
  ) {
    return converter.fromConnectData(topicName, schema, value);
  }

  private <T> Deserializer<T> givenDeserializerForSchema(
      final ConnectSchema schema,
      final Class<T> targetType
  ) {
    final Deserializer<T> deserializer = new ProtobufSerdeFactory(new ProtobufProperties(Collections.emptyMap()))
        .createSerde(
            schema,
            KSQL_CONFIG,
            () -> schemaRegistryClient,
            targetType,
            false).deserializer();

    deserializer.configure(Collections.emptyMap(), false);

    return deserializer;
  }
}
