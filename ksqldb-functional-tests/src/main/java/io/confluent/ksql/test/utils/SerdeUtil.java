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
import io.confluent.ksql.serde.SerdeFeatures;
import io.confluent.ksql.serde.avro.AvroFormat;
import io.confluent.ksql.serde.delimited.DelimitedFormat;
import io.confluent.ksql.serde.json.JsonFormat;
import io.confluent.ksql.serde.json.JsonSchemaFormat;
import io.confluent.ksql.serde.kafka.KafkaFormat;
import io.confluent.ksql.serde.none.NoneFormat;
import io.confluent.ksql.serde.protobuf.ProtobufFormat;
import io.confluent.ksql.serde.protobuf.ProtobufNoSRFormat;
import io.confluent.ksql.serde.protobuf.ProtobufNoSRProperties;
import io.confluent.ksql.serde.protobuf.ProtobufProperties;
import io.confluent.ksql.test.serde.SerdeSupplier;
import io.confluent.ksql.test.serde.avro.ValueSpecAvroSerdeSupplier;
import io.confluent.ksql.test.serde.json.ValueSpecJsonSchemaSerdeSupplier;
import io.confluent.ksql.test.serde.json.ValueSpecJsonSerdeSupplier;
import io.confluent.ksql.test.serde.kafka.KafkaSerdeSupplier;
import io.confluent.ksql.test.serde.none.NoneSerdeSupplier;
import io.confluent.ksql.test.serde.protobuf.ValueSpecProtobufNoSRSerdeSupplier;
import io.confluent.ksql.test.serde.protobuf.ValueSpecProtobufSerdeSupplier;
import io.confluent.ksql.test.serde.string.StringSerdeSupplier;
import io.confluent.ksql.test.tools.exceptions.InvalidFieldException;
import io.confluent.ksql.tools.test.model.SchemaReference;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
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
      final Map<String, Object> properties,
      final SerdeFeatures serdeFeatures
  ) {
    final Format format = FormatFactory.of(formatInfo);
    switch (format.name()) {
      case AvroFormat.NAME:       return new ValueSpecAvroSerdeSupplier();
      case ProtobufFormat.NAME:
        return new ValueSpecProtobufSerdeSupplier(
            new ProtobufProperties(formatInfo.getProperties()));
      case ProtobufNoSRFormat.NAME:
        return new ValueSpecProtobufNoSRSerdeSupplier(schema,
            new ProtobufNoSRProperties(formatInfo.getProperties()));
      case JsonFormat.NAME:       return new ValueSpecJsonSerdeSupplier(properties);
      case JsonSchemaFormat.NAME: return new ValueSpecJsonSchemaSerdeSupplier();
      case DelimitedFormat.NAME:  return new StringSerdeSupplier();
      case KafkaFormat.NAME:      return new KafkaSerdeSupplier(schema);
      case NoneFormat.NAME:       return new NoneSerdeSupplier();
      default:
        throw new InvalidFieldException("format", "unsupported value: " + format);
    }
  }

  public static Optional<ParsedSchema> buildSchema(
      final JsonNode schema,
      final String format
  ) {
    if (schema == null || schema instanceof NullNode) {
      return Optional.empty();
    }

    // format == null is the legacy case
    final String useFormat = (format == null) ? AvroFormat.NAME : format.toUpperCase();
    final String schemaString;

    switch (useFormat) {
      case ProtobufFormat.NAME:
        // Protobuf schema is already a JSON string
        schemaString = schema.textValue();
        break;
      case AvroFormat.NAME:
      case JsonSchemaFormat.NAME:
      case JsonFormat.NAME:
        try {
          schemaString = OBJECT_MAPPER.writeValueAsString(schema);
        } catch (final Exception e) {
          throw new InvalidFieldException("schema", "failed to parse", e);
        }
        break;
      default:
        throw new InvalidFieldException("schema", "not supported with format: " + format);
    }

    return Optional.of(buildSchema(schemaString, useFormat));
  }

  public static ParsedSchema buildSchema(
      final String schemaString,
      final String format
  ) {
    switch (format.toUpperCase()) {
      case AvroFormat.NAME:
        final org.apache.avro.Schema avroSchema =
            new org.apache.avro.Schema.Parser().parse(schemaString);
        return new AvroSchema(avroSchema);
      case ProtobufFormat.NAME:
        return new ProtobufSchema(schemaString);
      case JsonSchemaFormat.NAME:
      case JsonFormat.NAME:
        return new JsonSchema(schemaString);
      default:
        throw new InvalidFieldException("schema", "not supported with format: " + format);
    }
  }

  public static ParsedSchema withSchemaReferences(
      final ParsedSchema schema,
      final List<SchemaReference> references
  ) {
    if (references.isEmpty()) {
      return schema;
    }

    /* QTT does not support subjects versions yet */
    final int firstVersion = 1;

    final Map<String, String> resolvedReferences = references.stream()
        .collect(Collectors.toMap(
            ref -> ref.getName(),
            ref -> ref.getSchema().canonicalString()
        ));

    final List<io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference>
        schemaReferences = references.stream()
        .map(qttSchemaRef ->
            new io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference(
                qttSchemaRef.getName(),
                qttSchemaRef.getName(),
                firstVersion))
        .collect(Collectors.toList());

    switch (schema.schemaType()) {
      case ProtobufFormat.NAME:
        return new ProtobufSchema(
            schema.canonicalString(),
            schemaReferences,
            resolvedReferences,
            firstVersion,
            schema.name());
      case AvroFormat.NAME:
        return new AvroSchema(
            schema.canonicalString(),
            schemaReferences,
            resolvedReferences,
            firstVersion
        );
      case JsonSchemaFormat.NAME:
        return new JsonSchema(
            schema.canonicalString(),
            schemaReferences,
            resolvedReferences,
            firstVersion
        );
      default:
        return schema;
    }
  }

  @SuppressWarnings("unchecked")
  public static <T> SerdeSupplier<?> getKeySerdeSupplier(
      final KeyFormat keyFormat,
      final LogicalSchema schema,
      final Map<String, Object> properties) {
    final SerdeSupplier<T> inner = (SerdeSupplier<T>) getSerdeSupplier(
        keyFormat.getFormatInfo(),
        schema,
        properties,
        keyFormat.getFeatures()
    );

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
