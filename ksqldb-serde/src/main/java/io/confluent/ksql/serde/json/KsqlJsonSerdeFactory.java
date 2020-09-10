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
import io.confluent.ksql.schema.ksql.PersistenceSchema;
import io.confluent.ksql.serde.KsqlSerdeFactory;
import io.confluent.ksql.serde.connect.ConnectDataTranslator;
import io.confluent.ksql.serde.connect.KsqlConnectSerializer;
import io.confluent.ksql.serde.tls.ThreadLocalSerializer;
import io.confluent.ksql.util.KsqlConfig;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.json.DecimalFormat;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.json.JsonConverterConfig;
import org.apache.kafka.connect.storage.Converter;

@Immutable
public class KsqlJsonSerdeFactory implements KsqlSerdeFactory {

  private final boolean useSchemaRegistryFormat;

  /**
   * @param useSchemaRegistryFormat whether or not to require the magic byte and
   *                                schemaID as part of the JSON message
   */
  public KsqlJsonSerdeFactory(final boolean useSchemaRegistryFormat) {
    this.useSchemaRegistryFormat = useSchemaRegistryFormat;
  }

  @Override
  public void validate(final PersistenceSchema schema) {
    JsonSerdeUtils.validateSchema(schema);
  }

  @Override
  public Serde<Object> createSerde(
      final PersistenceSchema schema,
      final KsqlConfig ksqlConfig,
      final Supplier<SchemaRegistryClient> schemaRegistryClientFactory
  ) {
    final Supplier<Serializer<Object>> serializer = () -> createSerializer(
        schema,
        ksqlConfig,
        schemaRegistryClientFactory
    );

    // Sanity check:
    serializer.get();

    return Serdes.serdeFrom(
        new ThreadLocalSerializer<>(serializer),
        new KsqlJsonDeserializer(schema, useSchemaRegistryFormat)
    );
  }

  private KsqlConnectSerializer createSerializer(
      final PersistenceSchema schema,
      final KsqlConfig ksqlConfig,
      final Supplier<SchemaRegistryClient> schemaRegistryClientFactory
  ) {
    final Converter converter = useSchemaRegistryFormat
        ? getSchemaConverter(schemaRegistryClientFactory.get(), ksqlConfig)
        : getConverter();

    return new KsqlConnectSerializer(
        schema.serializedSchema(),
        new ConnectDataTranslator(schema.serializedSchema()),
        converter
    );
  }

  private Converter getConverter() {
    final JsonConverter converter = new JsonConverter();
    converter.configure(ImmutableMap.of(
        JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, false,
        JsonConverterConfig.DECIMAL_FORMAT_CONFIG, DecimalFormat.NUMERIC.name()
    ), false);
    return converter;
  }

  private Converter getSchemaConverter(
      final SchemaRegistryClient schemaRegistryClient,
      final KsqlConfig ksqlConfig
  ) {
    final Map<String, Object> config = ksqlConfig
        .originalsWithPrefix(KsqlConfig.KSQL_SCHEMA_REGISTRY_PREFIX);

    config.put(
        JsonSchemaConverterConfig.SCHEMA_REGISTRY_URL_CONFIG,
        ksqlConfig.getString(KsqlConfig.SCHEMA_REGISTRY_URL_PROPERTY)
    );
    config.put(JsonConverterConfig.DECIMAL_FORMAT_CONFIG, DecimalFormat.NUMERIC.name());

    final Converter converter = new JsonSchemaConverter(schemaRegistryClient);
    converter.configure(config, false);

    return converter;
  }
}
