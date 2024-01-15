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
import io.confluent.connect.protobuf.ProtobufDataConfig;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.schema.connect.SchemaWalker;
import io.confluent.ksql.serde.SerdeFactory;
import io.confluent.ksql.serde.connect.ConnectDataTranslator;
import io.confluent.ksql.serde.connect.DataTranslator;
import io.confluent.ksql.serde.connect.KsqlConnectDeserializer;
import io.confluent.ksql.serde.connect.KsqlConnectSerializer;
import io.confluent.ksql.serde.tls.ThreadLocalDeserializer;
import io.confluent.ksql.serde.tls.ThreadLocalSerializer;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Schema.Type;

@SuppressWarnings("checkstyle:ClassDataAbstractionCoupling")
public final class ProtobufNoSRSerdeFactory implements SerdeFactory {

  private final ProtobufNoSRProperties properties;

  ProtobufNoSRSerdeFactory(final ProtobufNoSRProperties properties) {
    this.properties = Objects.requireNonNull(properties, "properties");
  }

  public ProtobufNoSRSerdeFactory(final ImmutableMap<String, String> formatProperties) {
    this(new ProtobufNoSRProperties(formatProperties));
  }

  @Override
  public <T> Serde<T> createSerde(
      final Schema schema,
      final KsqlConfig ksqlConfig,
      final Supplier<SchemaRegistryClient> srFactory,
      final Class<T> targetType,
      final boolean isKey) {
    validate(schema);

    final Supplier<Serializer<T>> serializer = () -> createSerializer(
        schema,
        targetType,
        isKey
    );
    final Supplier<Deserializer<T>> deserializer = () -> createDeserializer(
        schema,
        targetType,
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

  public <T> KsqlConnectSerializer<T> createSerializer(
      final Schema schema,
      final Class<T> targetType,
      final boolean isKey
  ) {
    final ProtobufNoSRConverter converter = getConverter(schema, isKey);
    final DataTranslator translator = getDataTranslator(schema);
    final Schema compatibleSchema = translator instanceof ProtobufDataTranslator
        ? ((ProtobufDataTranslator) translator).getSchema()
        : ((ConnectDataTranslator) translator).getSchema();

    return new KsqlConnectSerializer<>(
        compatibleSchema,
        translator,
        converter,
        targetType
    );
  }

  private <T> KsqlConnectDeserializer<T> createDeserializer(
      final Schema schema,
      final Class<T> targetType,
      final boolean isKey
  ) {
    return new KsqlConnectDeserializer<>(
        getConverter(schema, isKey),
        getDataTranslator(schema),
        targetType
    );
  }

  private DataTranslator getDataTranslator(final Schema schema) {
    // maybe switch to ProtobufSchemaTranslator or a variation
    return new ConnectDataTranslator(schema);
  }

  private ProtobufNoSRConverter getConverter(
      final Schema schema,
      final boolean isKey
  ) {
    final Map<String, Object> protobufConfig = new HashMap<>();

    protobufConfig.putAll(ImmutableMap.of(
        ProtobufDataConfig.WRAPPER_FOR_RAW_PRIMITIVES_CONFIG, properties.getUnwrapPrimitives(),
        ProtobufDataConfig.OPTIONAL_FOR_NULLABLES_CONFIG, properties.isNullableAsOptional(),
        ProtobufDataConfig.WRAPPER_FOR_NULLABLES_CONFIG, properties.isNullableAsWrapper()
    ));

    final ProtobufNoSRConverter converter = new ProtobufNoSRConverter(schema);
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