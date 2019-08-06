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

import com.google.common.collect.ImmutableMap;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import io.confluent.ksql.model.WindowType;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.serde.Format;
import io.confluent.ksql.serde.KeyFormat;
import io.confluent.ksql.test.serde.SerdeSupplier;
import io.confluent.ksql.test.serde.avro.ValueSpecAvroSerdeSupplier;
import io.confluent.ksql.test.serde.json.ValueSpecJsonSerdeSupplier;
import io.confluent.ksql.test.serde.kafka.KafkaSerdeSupplier;
import io.confluent.ksql.test.serde.string.StringSerdeSupplier;
import io.confluent.ksql.test.tools.exceptions.InvalidFieldException;
import java.util.function.Supplier;
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

  private SerdeUtil() {
  }

  public static SerdeSupplier<?> getSerdeSupplier(
      final Format format,
      final Supplier<LogicalSchema> schemaSupplier
  ) {
    switch (format) {
      case AVRO:
        return new ValueSpecAvroSerdeSupplier();
      case JSON:
        return new ValueSpecJsonSerdeSupplier();
      case DELIMITED:
        return new StringSerdeSupplier();
      case KAFKA:
        return new KafkaSerdeSupplier(schemaSupplier);
      default:
        throw new InvalidFieldException("format", "unsupported value: " + format);
    }
  }

  @SuppressWarnings("unchecked")
  public static <T> SerdeSupplier<?> getKeySerdeSupplier(
      final KeyFormat keyFormat,
      final Supplier<LogicalSchema> logicalSchemaSupplier
  ) {
    final SerdeSupplier<T> inner = (SerdeSupplier<T>) SerdeUtil.getSerdeSupplier(
        keyFormat.getFormat(),
        logicalSchemaSupplier
    );

    if (!keyFormat.getWindowType().isPresent()) {
      return inner;
    }

    final WindowType windowType = keyFormat.getWindowType().get();
    if (windowType == WindowType.SESSION) {
      return new SerdeSupplier<Windowed<T>>() {
        @Override
        public Serializer<Windowed<T>> getSerializer(final SchemaRegistryClient srClient) {
          final Serializer<T> serializer = inner.getSerializer(srClient);
          serializer.configure(ImmutableMap.of(
              KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "something"
          ), true);
          return new SessionWindowedSerializer<>(serializer);
        }

        @Override
        public Deserializer<Windowed<T>> getDeserializer(final SchemaRegistryClient srClient) {
          final Deserializer<T> deserializer = inner.getDeserializer(srClient);
          deserializer.configure(ImmutableMap.of(), true);
          return new SessionWindowedDeserializer<>(deserializer);
        }
      };
    }

    return new SerdeSupplier<Windowed<T>>() {
      @Override
      public Serializer<Windowed<T>> getSerializer(final SchemaRegistryClient srClient) {
        final Serializer<T> serializer = inner.getSerializer(srClient);
        serializer.configure(ImmutableMap.of(
            KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "something"
        ), true);
        return new TimeWindowedSerializer<>(serializer);
      }

      @Override
      public Deserializer<Windowed<T>> getDeserializer(final SchemaRegistryClient srClient) {
        final Deserializer<T> deserializer = inner.getDeserializer(srClient);
        deserializer.configure(ImmutableMap.of(), true);
        return new TimeWindowedDeserializer<>(
            deserializer,
            keyFormat.getWindowSize().get().toMillis()
        );
      }
    };
  }
}
