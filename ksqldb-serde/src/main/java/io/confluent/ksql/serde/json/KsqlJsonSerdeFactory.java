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
import io.confluent.ksql.serde.connect.ConnectDataTranslator;
import io.confluent.ksql.serde.connect.KsqlConnectSerializer;
import io.confluent.ksql.serde.tls.ThreadLocalSerializer;
import io.confluent.ksql.util.KsqlConfig;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.json.DecimalFormat;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.json.JsonConverterConfig;
import org.apache.kafka.connect.storage.Converter;

@Immutable
class KsqlJsonSerdeFactory {

  private final boolean useSchemaRegistryFormat;

  /**
   * @param useSchemaRegistryFormat whether or not to require the magic byte and
   *                                schemaID as part of the JSON message
   */
  KsqlJsonSerdeFactory(final boolean useSchemaRegistryFormat) {
    this.useSchemaRegistryFormat = useSchemaRegistryFormat;
  }

  <T> Serde<T> createSerde(
      final ConnectSchema schema,
      final KsqlConfig ksqlConfig,
      final Supplier<SchemaRegistryClient> srFactory,
      final Class<T> targetType,
      final boolean isKey
  ) {
    final Supplier<Serializer<T>> serializer = () -> createSerializer(
        schema,
        ksqlConfig,
        srFactory,
        targetType,
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
      final boolean isKey
  ) {
    final Converter converter = useSchemaRegistryFormat
        ? getSchemaConverter(srFactory.get(), ksqlConfig, isKey)
        : getConverter();

    return new KsqlConnectSerializer<>(
        schema,
        new ConnectDataTranslator(schema),
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
      final boolean isKey
  ) {
    final Map<String, Object> config = ksqlConfig
        .originalsWithPrefix(KsqlConfig.KSQL_SCHEMA_REGISTRY_PREFIX);

    config.put(
        JsonSchemaConverterConfig.SCHEMA_REGISTRY_URL_CONFIG,
        ksqlConfig.getString(KsqlConfig.SCHEMA_REGISTRY_URL_PROPERTY)
    );
    config.put(JsonConverterConfig.DECIMAL_FORMAT_CONFIG, DecimalFormat.NUMERIC.name());

    final Converter converter = new JsonSchemaConverter(schemaRegistryClient);
    converter.configure(config, isKey);

    return converter;
  }
}
