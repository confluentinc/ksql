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

import io.confluent.connect.protobuf.ProtobufConverter;
import io.confluent.connect.protobuf.ProtobufConverterConfig;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.schema.connect.SchemaWalker;
import io.confluent.ksql.serde.connect.ConnectDataTranslator;
import io.confluent.ksql.serde.connect.KsqlConnectDeserializer;
import io.confluent.ksql.serde.connect.KsqlConnectSerializer;
import io.confluent.ksql.serde.tls.ThreadLocalDeserializer;
import io.confluent.ksql.serde.tls.ThreadLocalSerializer;
import io.confluent.ksql.util.DecimalUtil;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Schema.Type;

final class ProtobufSerdeFactory {

  private ProtobufSerdeFactory() {
  }

  static <T> Serde<T> createSerde(
      final ConnectSchema schema,
      final KsqlConfig ksqlConfig,
      final Supplier<SchemaRegistryClient> srFactory,
      final Class<T> targetType
  ) {
    validate(schema);

    final Supplier<Serializer<T>> serializer = () -> createSerializer(
        schema,
        ksqlConfig,
        srFactory,
        targetType
    );
    final Supplier<Deserializer<T>> deserializer = () -> createDeserializer(
        schema,
        ksqlConfig,
        srFactory,
        targetType
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

  private static <T> KsqlConnectSerializer<T> createSerializer(
      final ConnectSchema schema,
      final KsqlConfig ksqlConfig,
      final Supplier<SchemaRegistryClient> srFactory,
      final Class<T> targetType
  ) {
    final ProtobufConverter converter = getConverter(srFactory.get(), ksqlConfig);

    return new KsqlConnectSerializer<>(
        schema,
        new ConnectDataTranslator(schema),
        converter,
        targetType
    );
  }

  private static <T> KsqlConnectDeserializer<T> createDeserializer(
      final ConnectSchema schema,
      final KsqlConfig ksqlConfig,
      final Supplier<SchemaRegistryClient> srFactory,
      final Class<T> targetType
  ) {
    final ProtobufConverter converter = getConverter(srFactory.get(), ksqlConfig);

    return new KsqlConnectDeserializer<>(
        converter,
        new ConnectDataTranslator(schema),
        targetType
    );
  }

  private static ProtobufConverter getConverter(
      final SchemaRegistryClient schemaRegistryClient,
      final KsqlConfig ksqlConfig
  ) {
    final Map<String, Object> protobufConfig = ksqlConfig
        .originalsWithPrefix(KsqlConfig.KSQL_SCHEMA_REGISTRY_PREFIX);

    protobufConfig.put(
        ProtobufConverterConfig.SCHEMA_REGISTRY_URL_CONFIG,
        ksqlConfig.getString(KsqlConfig.SCHEMA_REGISTRY_URL_PROPERTY)
    );

    final ProtobufConverter converter = new ProtobufConverter(schemaRegistryClient);
    converter.configure(protobufConfig, false);

    return converter;
  }

  private static class SchemaValidator implements SchemaWalker.Visitor<Void, Void> {

    public Void visitBytes(final Schema schema) {
      if (DecimalUtil.isDecimal(schema)) {
        throw new KsqlException("The '" + ProtobufFormat.NAME + "' format does not support type "
            + "'DECIMAL'. See https://github.com/confluentinc/ksql/issues/5762.");
      }
      return null;
    }

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