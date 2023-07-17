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

package io.confluent.ksql.test.serde.protobuf;

import static io.confluent.connect.protobuf.ProtobufDataConfig.OPTIONAL_FOR_NULLABLES_CONFIG;
import static io.confluent.connect.protobuf.ProtobufDataConfig.WRAPPER_FOR_NULLABLES_CONFIG;

import com.google.common.collect.ImmutableMap;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.serde.connect.ConnectSchemas;
import io.confluent.ksql.serde.protobuf.ProtobufNoSRConverter;
import io.confluent.ksql.serde.protobuf.ProtobufNoSRProperties;
import io.confluent.ksql.test.serde.SerdeSupplier;
import io.confluent.ksql.test.serde.SpecToConnectConverter;
import java.util.Map;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;

public class ValueSpecProtobufNoSRSerdeSupplier implements SerdeSupplier<Object> {

  private final Schema keySchema;
  private final Schema valueSchema;
  private final ProtobufNoSRConverter keyConverter;
  private final ProtobufNoSRConverter valueConverter;

  public ValueSpecProtobufNoSRSerdeSupplier(final LogicalSchema schema,
      final ProtobufNoSRProperties properties) {
    this.keySchema = ConnectSchemas.columnsToConnectSchema(schema.key());
    this.valueSchema = ConnectSchemas.columnsToConnectSchema(schema.value());
    this.valueConverter = new ProtobufNoSRConverter(this.valueSchema);
    this.keyConverter = new ProtobufNoSRConverter(this.keySchema);

    final ImmutableMap<String, Boolean> converterConfig = ImmutableMap.of(
        OPTIONAL_FOR_NULLABLES_CONFIG, properties.isNullableAsOptional(),
        WRAPPER_FOR_NULLABLES_CONFIG, properties.isNullableAsWrapper()
    );

    keyConverter.configure(converterConfig, true);
    valueConverter.configure(converterConfig, false);
  }

  @Override
  public Serializer<Object> getSerializer(final SchemaRegistryClient schemaRegistryClient,
      final boolean isKey) {
    if (isKey) {
      return new ValueSpecProtobufNoSRSerializer(keyConverter, keySchema);
    }
    return new ValueSpecProtobufNoSRSerializer(valueConverter, valueSchema);
  }

  @Override
  public Deserializer<Object> getDeserializer(final SchemaRegistryClient schemaRegistryClient,
      final boolean isKey) {
    if (isKey) {
      return new ValueSpecProtobufNoSRDeserializer(keyConverter);
    }
    return new ValueSpecProtobufNoSRDeserializer(valueConverter);
  }

  private static final class ValueSpecProtobufNoSRSerializer implements Serializer<Object> {
    private final Schema schema;
    private final ProtobufNoSRConverter converter;

    ValueSpecProtobufNoSRSerializer(final ProtobufNoSRConverter converter,
        final Schema schema) {
      this.converter = converter;
      this.schema = schema;
    }

    @Override
    public void configure(final Map<String, ?> configs, final boolean isKey) {
    }

    @Override
    public byte[] serialize(final String topic, final Object o) {
      final byte[] bytes = converter.fromConnectData(topic,
          schema,
          SpecToConnectConverter.specToConnect(o, schema));
      return bytes;
    }
  }

  private static final class ValueSpecProtobufNoSRDeserializer implements Deserializer<Object> {
    private final ProtobufNoSRConverter converter;

    ValueSpecProtobufNoSRDeserializer(final ProtobufNoSRConverter converter) {
      this.converter = converter;
    }

    @Override
    public void configure(final Map<String, ?> configs, final boolean isKey) {
    }


    @Override
    public Object deserialize(final String s, final byte[] bytes) {
      final SchemaAndValue schemaAndValue = converter.toConnectData(s, bytes);
      return SpecToConnectConverter.connectToSpec(schemaAndValue.value(),
          schemaAndValue.schema(),
          false);
    }
  }

}
