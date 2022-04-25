/*
 * Copyright 2022 Confluent Inc.
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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.util.DecimalUtil;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Collections;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@SuppressWarnings("checkstyle:ClassDataAbstractionCoupling")
@RunWith(MockitoJUnitRunner.class)
public class KsqlProtobufNoSRDeserializerTest {
  private static final String SOME_TOPIC = "bob";

  @Test
  public void shouldDeserializeDecimalField() {
    final ConnectSchema schema = (ConnectSchema) SchemaBuilder.struct()
        .field("f0", DecimalUtil.builder(10, 2))
        .build();
    final ProtobufNoSRConverter converter = new ProtobufNoSRConverter(schema);
    converter.configure(Collections.emptyMap(), false);

    // Given:
    final Deserializer<Struct> deserializer =
        givenDeserializerForSchema(schema,
            Struct.class);
    final Struct value = new Struct(schema).put("f0", new BigDecimal("12.34"));
    final byte[] bytes = givenConnectSerialized(converter, value, schema);

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
    final ProtobufNoSRConverter converter = new ProtobufNoSRConverter(schema);
    converter.configure(Collections.emptyMap(), false);

    // Given:
    final Deserializer<Struct> deserializer =
        givenDeserializerForSchema(schema,
            Struct.class);
    final Struct value = new Struct(schema).put("f0", new java.sql.Time(2000));
    final byte[] bytes = givenConnectSerialized(converter, value, schema);

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
    final ProtobufNoSRConverter converter = new ProtobufNoSRConverter(schema);
    converter.configure(Collections.emptyMap(), false);

    // Given:
    final Deserializer<Struct> deserializer =
        givenDeserializerForSchema(schema,
            Struct.class);
    final Struct value = new Struct(schema).put("f0", new java.sql.Date(864000000L));
    final byte[] bytes = givenConnectSerialized(converter, value, schema);

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
    final ProtobufNoSRConverter converter = new ProtobufNoSRConverter(schema);
    converter.configure(Collections.emptyMap(), false);

    // Given:
    final Deserializer<Struct> deserializer =
        givenDeserializerForSchema(schema,
            Struct.class);
    final Struct value = new Struct(schema).put("f0", new java.sql.Timestamp(2000));
    final byte[] bytes = givenConnectSerialized(converter, value, schema);

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
    final ProtobufNoSRConverter converter = new ProtobufNoSRConverter(schema);
    converter.configure(Collections.emptyMap(), false);

    // Given:
    final Deserializer<Struct> deserializer =
        givenDeserializerForSchema(schema,
            Struct.class);
    final Struct value = new Struct(schema).put("f0", ByteBuffer.wrap(new byte[] {123}));
    final byte[] bytes = givenConnectSerialized(converter, value, schema);

    // When:
    final Object result = deserializer.deserialize(SOME_TOPIC, bytes);

    // Then:
    assertThat(((Struct) result).getBytes("f0"), is(value.getBytes("f0")));
  }

  private byte[] givenConnectSerialized(
      final ProtobufNoSRConverter converter,
      final Object value,
      final Schema connectSchema
  ) {
    return converter.fromConnectData(SOME_TOPIC, connectSchema, value);
  }

  private <T> Deserializer<T> givenDeserializerForSchema(
      final ConnectSchema schema,
      final Class<T> targetType
  ) {
    final Deserializer<T> deserializer = new ProtobufNoSRSerdeFactory(ImmutableMap.of())
        .createSerde(
        schema,
        targetType,
        false).deserializer();

    deserializer.configure(Collections.emptyMap(), false);

    return deserializer;
  }
}
