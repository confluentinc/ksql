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

import com.google.common.collect.ImmutableMap;
import com.google.errorprone.annotations.Immutable;
import io.confluent.connect.avro.AvroConverter;
import io.confluent.connect.avro.AvroDataConfig;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.ksql.serde.SerdeFactory;
import io.confluent.ksql.serde.SerdeUtils;
import io.confluent.ksql.serde.connect.ConnectDataTranslator;
import io.confluent.ksql.serde.connect.DataTranslator;
import io.confluent.ksql.serde.connect.KsqlConnectDeserializer;
import io.confluent.ksql.serde.connect.KsqlConnectSerializer;
import io.confluent.ksql.serde.tls.ThreadLocalDeserializer;
import io.confluent.ksql.serde.tls.ThreadLocalSerializer;
import io.confluent.ksql.util.KsqlConfig;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.data.Schema;

@Immutable
@SuppressWarnings("checkstyle:ClassDataAbstractionCoupling")
class KsqlAvroSerdeFactory implements SerdeFactory {

  private final String fullSchemaName;
  private final AvroProperties properties;

  KsqlAvroSerdeFactory(final AvroProperties properties) {
    this.properties = Objects.requireNonNull(properties, "properties");
    this.fullSchemaName = Objects.requireNonNull(
        properties.getFullSchemaName(), "fullSchemaName").trim();
    if (this.fullSchemaName.isEmpty()) {
      throw new IllegalArgumentException("the schema name cannot be empty");
    }
  }

  KsqlAvroSerdeFactory(final ImmutableMap<String, String> properties) {
    this(new AvroProperties(properties));
  }

  @Override
  public <T> Serde<T> createSerde(
      final Schema schema,
      final KsqlConfig ksqlConfig,
      final Supplier<SchemaRegistryClient> srFactory,
      final Class<T> targetType,
      final boolean isKey
  ) {
    AvroUtil.throwOnInvalidSchema(schema);
    final Optional<Schema> physicalSchema = properties.getSchemaId().isPresent() ? Optional.of(
        SerdeUtils.getAndTranslateSchemaById(srFactory, properties.getSchemaId()
            .get(), new AvroSchemaTranslator(properties))) : Optional.empty();

    final Supplier<Serializer<T>> serializerSupplier = createConnectSerializer(
        schema,
        ksqlConfig,
        srFactory,
        targetType,
        physicalSchema,
        isKey
    );

    final Supplier<Deserializer<T>> deserializerSupplier = createConnectDeserializer(
        schema,
        ksqlConfig,
        srFactory,
        targetType,
        physicalSchema,
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
      final Schema schema,
      final KsqlConfig ksqlConfig,
      final Supplier<SchemaRegistryClient> srFactory,
      final Class<T> targetType,
      final Optional<Schema> physicalSchema,
      final boolean isKey
  ) {
    return () -> {
      final DataTranslator translator = createAvroTranslator(schema, physicalSchema, false);
      final Schema compatibleSchema = translator instanceof AvroDataTranslator
          ? ((AvroDataTranslator) translator).getAvroCompatibleSchema()
          : ((ConnectDataTranslator) translator).getSchema();

      final AvroConverter avroConverter =
          getAvroConverter(srFactory.get(), ksqlConfig, properties.getSchemaId(), isKey);

      return new KsqlConnectSerializer<>(
          compatibleSchema,
          translator,
          avroConverter,
          targetType
      );
    };
  }

  private <T> Supplier<Deserializer<T>> createConnectDeserializer(
      final Schema schema,
      final KsqlConfig ksqlConfig,
      final Supplier<SchemaRegistryClient> srFactory,
      final Class<T> targetType,
      final Optional<Schema> physicalSchema,
      final boolean isKey
  ) {
    return () -> {
      final DataTranslator translator = createAvroTranslator(schema, physicalSchema, true);

      final AvroConverter avroConverter =
          getAvroConverter(srFactory.get(), ksqlConfig, Optional.empty(), isKey);

      return new KsqlConnectDeserializer<>(avroConverter, translator, targetType);
    };
  }

  private DataTranslator createAvroTranslator(final Schema schema,
      final Optional<Schema> physicalSchema, final boolean isDeserializer) {
    // If physical schema exists, we use physical schema to translate to connect data. During
    // deserialization, if physical schema exists, we use original schema to translate to ksql data.
    return physicalSchema.<DataTranslator>map(
            value -> isDeserializer ? new ConnectDataTranslator(schema)
                : new AvroSRSchemaDataTranslator(value))
        .orElseGet(() -> new AvroDataTranslator(schema, fullSchemaName));
  }

  private static AvroConverter getAvroConverter(
      final SchemaRegistryClient schemaRegistryClient,
      final KsqlConfig ksqlConfig,
      final Optional<Integer> schemaId,
      final boolean isKey
  ) {
    final AvroConverter avroConverter = new AvroConverter(schemaRegistryClient);

    final Map<String, Object> avroConfig = ksqlConfig
        .originalsWithPrefix(KsqlConfig.KSQL_SCHEMA_REGISTRY_PREFIX);

    avroConfig.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
        ksqlConfig.getString(KsqlConfig.SCHEMA_REGISTRY_URL_PROPERTY));

    avroConfig.put(AvroDataConfig.CONNECT_META_DATA_CONFIG, false);

    if (schemaId.isPresent()) {
      // Disable auto registering schema if schema id is used
      avroConfig.put(AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS, false);
      avroConfig.put(AbstractKafkaSchemaSerDeConfig.USE_SCHEMA_ID, schemaId.get());
    }

    avroConverter.configure(avroConfig, isKey);
    return avroConverter;
  }
}
