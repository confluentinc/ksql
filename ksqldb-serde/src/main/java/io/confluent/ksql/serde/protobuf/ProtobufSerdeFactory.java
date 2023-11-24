/*
 * Copyright 2020 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"; you may not use
 * this file except in compliance with the License. You may obtain a copy of the
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
import io.confluent.connect.protobuf.ProtobufConverterConfig;
import io.confluent.connect.protobuf.ProtobufDataConfig;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.ksql.schema.connect.SchemaWalker;
import io.confluent.ksql.serde.SerdeUtils;
import io.confluent.ksql.serde.connect.ConnectDataTranslator;
import io.confluent.ksql.serde.connect.ConnectSRSchemaDataTranslator;
import io.confluent.ksql.serde.connect.KsqlConnectDeserializer;
import io.confluent.ksql.serde.connect.KsqlConnectSerializer;
import io.confluent.ksql.serde.tls.ThreadLocalDeserializer;
import io.confluent.ksql.serde.tls.ThreadLocalSerializer;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
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
import org.apache.kafka.connect.data.Schema.Type;

@SuppressWarnings("checkstyle:ClassDataAbstractionCoupling")
final class ProtobufSerdeFactory {

  final ProtobufProperties properties;

  ProtobufSerdeFactory(final ProtobufProperties properties) {
    this.properties = Objects.requireNonNull(properties, "properties");
  }

  ProtobufSerdeFactory(final ImmutableMap<String, String> formatProperties) {
    this(new ProtobufProperties(formatProperties));
  }

  <T> Serde<T> createSerde(
      final ConnectSchema schema,
      final KsqlConfig ksqlConfig,
      final Supplier<SchemaRegistryClient> srFactory,
      final Class<T> targetType,
      final boolean isKey) {
    validate(schema);

    final Optional<Schema> physicalSchema = properties.getSchemaId().isPresent() ? Optional.of(
        SerdeUtils.getAndTranslateSchema(srFactory, properties.getSchemaId()
            .get(), new ProtobufSchemaTranslator(properties))) : Optional.empty();

    final Supplier<Serializer<T>> serializer = () -> createSerializer(
        schema,
        ksqlConfig,
        srFactory,
        targetType,
        physicalSchema,
        isKey
    );
    final Supplier<Deserializer<T>> deserializer = () -> createDeserializer(
        schema,
        ksqlConfig,
        srFactory,
        targetType,
        physicalSchema,
        isKey
    );

    // Sanity check:
    serializer.get();
    deserializer.get();

    return Serdes.serdeFrom(
        new ThreadLocalSerializer<>(serializer),
        new ThreadLocalDeserializer<>(deserializer)
    );
  }

  private static void validate(final Schema schema) {
    SchemaWalker.visit(schema, new SchemaValidator());
  }

  private <T> KsqlConnectSerializer<T> createSerializer(
      final ConnectSchema schema,
      final KsqlConfig ksqlConfig,
      final Supplier<SchemaRegistryClient> srFactory,
      final Class<T> targetType,
      final Optional<Schema> physicalSchema,
      final boolean isKey
  ) {
    final ProtobufConverter converter = getConverter(srFactory.get(), ksqlConfig,
        properties.getSchemaId(), isKey);
    final ConnectDataTranslator translator = getDataTranslator(schema, physicalSchema, false);
    return new KsqlConnectSerializer<>(
        translator.getSchema(),
        translator,
        converter,
        targetType
    );
  }

  private <T> KsqlConnectDeserializer<T> createDeserializer(
      final ConnectSchema schema,
      final KsqlConfig ksqlConfig,
      final Supplier<SchemaRegistryClient> srFactory,
      final Class<T> targetType,
      final Optional<Schema> physicalSchema,
      final boolean isKey
  ) {
    final ProtobufConverter converter = getConverter(srFactory.get(), ksqlConfig, Optional.empty(),
        isKey);

    return new KsqlConnectDeserializer<>(
        converter,
        getDataTranslator(schema, physicalSchema, true),
        targetType
    );
  }

  private static ConnectDataTranslator getDataTranslator(final Schema schema,
      final Optional<Schema> physicalSchema, final boolean isDeserializer) {
    // If physical schema exists, we use physical schema to translate to connect data. During
    // deserialization, if physical schema exists, we use original schema to translate to ksql data.
    if (!physicalSchema.isPresent() || isDeserializer) {
      return new ConnectDataTranslator(schema);
    }
    return new ConnectSRSchemaDataTranslator(physicalSchema.get());
  }

  private ProtobufConverter getConverter(
      final SchemaRegistryClient schemaRegistryClient,
      final KsqlConfig ksqlConfig,
      final Optional<Integer> schemaId,
      final boolean isKey
  ) {
    final Map<String, Object> protobufConfig = ksqlConfig
        .originalsWithPrefix(KsqlConfig.KSQL_SCHEMA_REGISTRY_PREFIX);

    protobufConfig.put(
        ProtobufConverterConfig.SCHEMA_REGISTRY_URL_CONFIG,
        ksqlConfig.getString(KsqlConfig.SCHEMA_REGISTRY_URL_PROPERTY)
    );

    if (schemaId.isPresent()) {
      // Disable auto registering schema if schema id is used
      protobufConfig.put(AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS, false);
      protobufConfig.put(AbstractKafkaSchemaSerDeConfig.USE_SCHEMA_ID, schemaId.get());
    }

    protobufConfig.put(
        ProtobufDataConfig.WRAPPER_FOR_RAW_PRIMITIVES_CONFIG,
        properties.getUnwrapPrimitives()
    );

    final ProtobufConverter converter = new ProtobufConverter(schemaRegistryClient);
    converter.configure(protobufConfig, isKey);

    return converter;
  }

  private static class SchemaValidator implements SchemaWalker.Visitor<Void, Void> {

    @Override
    public Void visitMap(final Schema schema, final Void key, final Void value) {
      if (schema.keySchema().type() != Type.STRING) {
        throw new KsqlException("PROTOBUF format only supports MAP types with STRING keys. "
            + "See https://github.com/confluentinc/ksql/issues/6177.");
      }
      return null;
    }

    @Override
    public Void visitSchema(final Schema schema) {
      return null;
    }
  }
}