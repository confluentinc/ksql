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

package io.confluent.ksql.serde.json;

import com.google.common.collect.ImmutableMap;
import com.google.errorprone.annotations.Immutable;
import io.confluent.connect.json.JsonSchemaConverter;
import io.confluent.connect.json.JsonSchemaConverterConfig;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.ksql.serde.SerdeUtils;
import io.confluent.ksql.serde.connect.ConnectDataTranslator;
import io.confluent.ksql.serde.connect.ConnectSRSchemaDataTranslator;
import io.confluent.ksql.serde.connect.KsqlConnectSerializer;
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
import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.json.DecimalFormat;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.json.JsonConverterConfig;
import org.apache.kafka.connect.storage.Converter;

@SuppressWarnings("checkstyle:ClassDataAbstractionCoupling")
@Immutable
class KsqlJsonSerdeFactory {

  private final boolean useSchemaRegistryFormat;
  private final JsonSchemaProperties properties;

  KsqlJsonSerdeFactory() {
    useSchemaRegistryFormat = false;
    properties = null;
  }

  /**
   * @param properties JsonSchemaFormat properties
   */
  KsqlJsonSerdeFactory(final JsonSchemaProperties properties) {
    this.useSchemaRegistryFormat = true;
    this.properties = Objects.requireNonNull(properties, "properties");
  }

  <T> Serde<T> createSerde(
      final ConnectSchema schema,
      final KsqlConfig ksqlConfig,
      final Supplier<SchemaRegistryClient> srFactory,
      final Class<T> targetType,
      final boolean isKey
  ) {
    final Optional<Schema> physicalSchema;
    if (useSchemaRegistryFormat) {
      physicalSchema = properties.getSchemaId().isPresent() ? Optional.of(
          SerdeUtils.getAndTranslateSchemaById(srFactory, properties.getSchemaId()
              .get(), new JsonSchemaTranslator())) : Optional.empty();
    } else {
      physicalSchema = Optional.empty();
    }

    final Supplier<Serializer<T>> serializer = () -> createSerializer(
        schema,
        ksqlConfig,
        srFactory,
        targetType,
        physicalSchema,
        isKey
    );

    final Deserializer<T> deserializer = createDeserializer(schema, targetType);

    // Sanity check:
    serializer.get();

    return Serdes.serdeFrom(
        new ThreadLocalSerializer<>(serializer),
        deserializer
    );
  }

  private <T> Serializer<T> createSerializer(
      final ConnectSchema schema,
      final KsqlConfig ksqlConfig,
      final Supplier<SchemaRegistryClient> srFactory,
      final Class<T> targetType,
      final Optional<Schema> physicalSchema,
      final boolean isKey
  ) {
    final Converter converter = useSchemaRegistryFormat
        ? getSchemaConverter(srFactory.get(), ksqlConfig, properties.getSchemaId(), isKey)
        : getConverter();

    final ConnectDataTranslator dataTranslator =
        physicalSchema.isPresent() ? new ConnectSRSchemaDataTranslator(physicalSchema.get())
            : new ConnectDataTranslator(schema);

    return new KsqlConnectSerializer<>(
        dataTranslator.getSchema(),
        dataTranslator,
        converter,
        targetType
    );
  }

  private <T> Deserializer<T> createDeserializer(
      final ConnectSchema schema,
      final Class<T> targetType
  ) {
    return new KsqlJsonDeserializer<>(
        schema,
        useSchemaRegistryFormat,
        targetType
    );
  }

  private static Converter getConverter() {
    final JsonConverter converter = new JsonConverter();
    converter.configure(ImmutableMap.of(
        JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, false,
        JsonConverterConfig.DECIMAL_FORMAT_CONFIG, DecimalFormat.NUMERIC.name()
    ), false);
    return converter;
  }

  private static Converter getSchemaConverter(
      final SchemaRegistryClient schemaRegistryClient,
      final KsqlConfig ksqlConfig,
      final Optional<Integer> schemaId,
      final boolean isKey
  ) {
    final Map<String, Object> config = ksqlConfig
        .originalsWithPrefix(KsqlConfig.KSQL_SCHEMA_REGISTRY_PREFIX);

    config.put(
        JsonSchemaConverterConfig.SCHEMA_REGISTRY_URL_CONFIG,
        ksqlConfig.getString(KsqlConfig.SCHEMA_REGISTRY_URL_PROPERTY)
    );
    if (schemaId.isPresent()) {
      // Disable auto registering schema if schema id is used
      config.put(AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS, false);
      config.put(AbstractKafkaSchemaSerDeConfig.USE_SCHEMA_ID, schemaId.get());
    }
    config.put(JsonConverterConfig.DECIMAL_FORMAT_CONFIG, DecimalFormat.NUMERIC.name());

    final Converter converter = new JsonSchemaConverter(schemaRegistryClient);
    converter.configure(config, isKey);

    return converter;
  }
}
