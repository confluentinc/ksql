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

import com.google.errorprone.annotations.Immutable;
import io.confluent.connect.avro.AvroConverter;
import io.confluent.connect.avro.AvroDataConfig;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.ksql.serde.connect.KsqlConnectDeserializer;
import io.confluent.ksql.serde.connect.KsqlConnectSerializer;
import io.confluent.ksql.serde.tls.ThreadLocalDeserializer;
import io.confluent.ksql.serde.tls.ThreadLocalSerializer;
import io.confluent.ksql.util.KsqlConfig;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Schema;

@Immutable
class KsqlAvroSerdeFactory {

  private final String fullSchemaName;

  KsqlAvroSerdeFactory(final String fullSchemaName) {
    this.fullSchemaName = Objects.requireNonNull(fullSchemaName, "fullSchemaName").trim();
    if (this.fullSchemaName.isEmpty()) {
      throw new IllegalArgumentException("the schema name cannot be empty");
    }
  }

  <T> Serde<T> createSerde(
      final ConnectSchema schema,
      final KsqlConfig ksqlConfig,
      final Supplier<SchemaRegistryClient> srFactory,
      final Class<T> targetType,
      final boolean isKey
  ) {
    AvroUtil.throwOnInvalidSchema(schema);

    final Supplier<Serializer<T>> serializerSupplier = createConnectSerializer(
        schema,
        ksqlConfig,
        srFactory,
        targetType,
        isKey
    );

    final Supplier<Deserializer<T>> deserializerSupplier = createConnectDeserializer(
        schema,
        ksqlConfig,
        srFactory,
        targetType,
        isKey
    );

    // Sanity check:
    serializerSupplier.get();
    deserializerSupplier.get();

    return Serdes.serdeFrom(
        new ThreadLocalSerializer<>(serializerSupplier),
        new ThreadLocalDeserializer<>(deserializerSupplier)
    );
  }

  private <T> Supplier<Serializer<T>> createConnectSerializer(
      final ConnectSchema schema,
      final KsqlConfig ksqlConfig,
      final Supplier<SchemaRegistryClient> srFactory,
      final Class<T> targetType,
      final boolean isKey
  ) {
    return () -> {
      final AvroDataTranslator translator = createAvroTranslator(schema);

      final AvroConverter avroConverter =
          getAvroConverter(srFactory.get(), ksqlConfig, isKey);

      return new KsqlConnectSerializer<>(
          translator.getAvroCompatibleSchema(),
          translator,
          avroConverter,
          targetType
      );
    };
  }

  private <T> Supplier<Deserializer<T>> createConnectDeserializer(
      final ConnectSchema schema,
      final KsqlConfig ksqlConfig,
      final Supplier<SchemaRegistryClient> srFactory,
      final Class<T> targetType,
      final boolean isKey
  ) {
    return () -> {
      final AvroDataTranslator translator = createAvroTranslator(schema);

      final AvroConverter avroConverter =
          getAvroConverter(srFactory.get(), ksqlConfig, isKey);

      return new KsqlConnectDeserializer<>(avroConverter, translator, targetType);
    };
  }

  private AvroDataTranslator createAvroTranslator(final Schema schema) {
    return new AvroDataTranslator(schema, fullSchemaName);
  }

  private static AvroConverter getAvroConverter(
      final SchemaRegistryClient schemaRegistryClient,
      final KsqlConfig ksqlConfig,
      final boolean isKey
  ) {
    final AvroConverter avroConverter = new AvroConverter(schemaRegistryClient);

    final Map<String, Object> avroConfig = ksqlConfig
        .originalsWithPrefix(KsqlConfig.KSQL_SCHEMA_REGISTRY_PREFIX);

    avroConfig.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
        ksqlConfig.getString(KsqlConfig.SCHEMA_REGISTRY_URL_PROPERTY));

    avroConfig.put(AvroDataConfig.CONNECT_META_DATA_CONFIG, false);

    avroConverter.configure(avroConfig, isKey);
    return avroConverter;
  }
}
