/*
 * Copyright 2019 Confluent Inc.
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

package io.confluent.ksql.test.utils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.NullNode;
import com.google.common.collect.ImmutableMap;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.ksql.model.WindowType;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.serde.Format;
import io.confluent.ksql.serde.FormatFactory;
import io.confluent.ksql.serde.FormatInfo;
import io.confluent.ksql.serde.KeyFormat;
import io.confluent.ksql.serde.avro.AvroFormat;
import io.confluent.ksql.serde.delimited.DelimitedFormat;
import io.confluent.ksql.serde.json.JsonFormat;
import io.confluent.ksql.serde.json.JsonSchemaFormat;
import io.confluent.ksql.serde.kafka.KafkaFormat;
import io.confluent.ksql.serde.none.NoneFormat;
import io.confluent.ksql.serde.protobuf.ProtobufFormat;
import io.confluent.ksql.serde.protobuf.ProtobufProperties;
import io.confluent.ksql.test.serde.SerdeSupplier;
import io.confluent.ksql.test.serde.avro.ValueSpecAvroSerdeSupplier;
import io.confluent.ksql.test.serde.json.ValueSpecJsonSerdeSupplier;
import io.confluent.ksql.test.serde.kafka.KafkaSerdeSupplier;
import io.confluent.ksql.test.serde.none.NoneSerdeSupplier;
import io.confluent.ksql.test.serde.protobuf.ValueSpecProtobufSerdeSupplier;
import io.confluent.ksql.test.serde.string.StringSerdeSupplier;
import io.confluent.ksql.test.tools.exceptions.InvalidFieldException;
import java.util.Map;
import java.util.Optional;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.kstream.SessionWindowedDeserializer;
import org.apache.kafka.streams.kstream.SessionWindowedSerializer;
import org.apache.kafka.streams.kstream.TimeWindowedDeserializer;
import org.apache.kafka.streams.kstream.TimeWindowedSerializer;
import org.apache.kafka.streams.kstream.Windowed;

// CHECKSTYLE_RULES.OFF: ClassDataAbstractionCoupling
public final class SerdeUtil {
  // CHECKSTYLE_RULES.ON: ClassDataAbstractionCoupling

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private SerdeUtil() {
  }

  public static SerdeSupplier<?> getSerdeSupplier(
      final FormatInfo formatInfo,
      final LogicalSchema schema,
      final Map<String, Object> properties
  ) {
    final Format format = FormatFactory.of(formatInfo);
    switch (format.name()) {
      case AvroFormat.NAME:       return new ValueSpecAvroSerdeSupplier();
      case ProtobufFormat.NAME:
        return new ValueSpecProtobufSerdeSupplier(
            new ProtobufProperties(formatInfo.getProperties()));
      case JsonFormat.NAME:       return new ValueSpecJsonSerdeSupplier(false, properties);
      case JsonSchemaFormat.NAME: return new ValueSpecJsonSerdeSupplier(true, properties);
      case DelimitedFormat.NAME:  return new StringSerdeSupplier();
      case KafkaFormat.NAME:      return new KafkaSerdeSupplier(schema);
      case NoneFormat.NAME:       return new NoneSerdeSupplier();
      default:
        throw new InvalidFieldException("format", "unsupported value: " + format);
    }
  }

  public static Optional<ParsedSchema> buildSchema(final JsonNode schema, final String format) {
    if (schema == null || schema instanceof NullNode) {
      return Optional.empty();
    }

    try {
      // format == null is the legacy case
      if (format == null || format.equalsIgnoreCase(AvroFormat.NAME)) {
        final String schemaString = OBJECT_MAPPER.writeValueAsString(schema);
        final org.apache.avro.Schema avroSchema =
            new org.apache.avro.Schema.Parser().parse(schemaString);
        return Optional.of(new AvroSchema(avroSchema));
      } else if (format.equalsIgnoreCase(JsonFormat.NAME)
          || format.equalsIgnoreCase(JsonSchemaFormat.NAME)) {
        final String schemaString = OBJECT_MAPPER.writeValueAsString(schema);
        return Optional.of(new JsonSchema(schemaString));
      } else if (format.equalsIgnoreCase(ProtobufFormat.NAME)) {
        // since Protobuf schemas are not valid JSON, the schema JsonNode in
        // this case is just a string.
        final String schemaString = schema.textValue();
        return Optional.of(new ProtobufSchema(schemaString));
      }
    } catch (final Exception e) {
      throw new InvalidFieldException("schema", "failed to parse", e);
    }

    throw new InvalidFieldException("schema", "not supported with format: " + format);
  }

  @SuppressWarnings("unchecked")
  public static <T> SerdeSupplier<?> getKeySerdeSupplier(
      final KeyFormat keyFormat,
      final LogicalSchema schema,
      final Map<String, Object> properties) {
    final SerdeSupplier<T> inner = (SerdeSupplier<T>) getSerdeSupplier(
        keyFormat.getFormatInfo(),
        schema,
        properties);

    if (!keyFormat.getWindowType().isPresent()) {
      return inner;
    }

    final WindowType windowType = keyFormat.getWindowType().get();
    if (windowType == WindowType.SESSION) {
      return new SerdeSupplier<Windowed<T>>() {
        @Override
        public Serializer<Windowed<T>> getSerializer(
            final SchemaRegistryClient srClient, final boolean isKey
        ) {
          final Serializer<T> serializer = inner.getSerializer(srClient, isKey);
          serializer.configure(ImmutableMap.of(
              AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "something"
          ), true);
          return new SessionWindowedSerializer<>(serializer);
        }

        @Override
        public Deserializer<Windowed<T>> getDeserializer(
            final SchemaRegistryClient srClient,
            final boolean isKey
        ) {
          final Deserializer<T> deserializer = inner.getDeserializer(srClient, isKey);
          deserializer.configure(ImmutableMap.of(), true);
          return new SessionWindowedDeserializer<>(deserializer);
        }
      };
    }

    return new SerdeSupplier<Windowed<T>>() {
      @Override
      public Serializer<Windowed<T>> getSerializer(
          final SchemaRegistryClient srClient,
          final boolean isKey
      ) {
        final Serializer<T> serializer = inner.getSerializer(srClient, isKey);
        serializer.configure(ImmutableMap.of(
            AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "something"
        ), true);
        return new TimeWindowedSerializer<>(serializer);
      }

      @SuppressWarnings("OptionalGetWithoutIsPresent")
      @Override
      public Deserializer<Windowed<T>> getDeserializer(
          final SchemaRegistryClient srClient,
          final boolean isKey
      ) {
        final Deserializer<T> deserializer = inner.getDeserializer(srClient, isKey);
        deserializer.configure(ImmutableMap.of(), true);
        return new TimeWindowedDeserializer<>(
            deserializer,
            keyFormat.getWindowSize().get().toMillis()
        );
      }
    };
  }
}
