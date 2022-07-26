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
import io.confluent.connect.protobuf.ProtobufConverter;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.ksql.util.KsqlConfig;
import java.util.Collections;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.storage.Converter;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@SuppressWarnings("rawtypes")
@RunWith(MockitoJUnitRunner.class)
public class KsqlProtobufDeserializerTest extends AbstractKsqlProtobufDeserializerTest {

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

  @Override
  Converter getConverter(final ConnectSchema schema) {
    return converter;
  }

  @Override
  byte[] givenConnectSerialized(
          final Converter converter,
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

  @Override
  <T> Deserializer<T> givenDeserializerForSchema(
      final ConnectSchema schema,
      final Class<T> targetType
  ) {
    final Deserializer<T> deserializer = new ProtobufSerdeFactory(ImmutableMap.of()).createSerde(
        schema,
        KSQL_CONFIG,
        () -> schemaRegistryClient,
        targetType,
        false).deserializer();

    deserializer.configure(Collections.emptyMap(), false);

    return deserializer;
  }
}
