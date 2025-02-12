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

package io.confluent.ksql.serde.kafka;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.schema.ksql.PersistenceSchema;
import io.confluent.ksql.schema.ksql.SchemaConverters;
import io.confluent.ksql.schema.ksql.SimpleColumn;
import io.confluent.ksql.schema.ksql.types.SqlBaseType;
import io.confluent.ksql.serde.FormatFactory;
import io.confluent.ksql.serde.SerdeUtils;
import io.confluent.ksql.serde.voids.KsqlVoidSerde;
import io.confluent.ksql.util.KsqlException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

public final class KafkaSerdeFactory {

  // Note: If supporting new types here, add new type of PRINT TOPIC support too
  private static final ImmutableMap<Class<?>, Serde<?>> SERDE = ImmutableMap.of(
      Integer.class, Serdes.Integer(),
      Long.class, Serdes.Long(),
      Double.class, Serdes.Double(),
      String.class, Serdes.String(),
      ByteBuffer.class, Serdes.ByteBuffer()
  );

  private KafkaSerdeFactory() {
  }

  public static boolean containsSerde(final Class<?> javaType) {
    return SERDE.containsKey(javaType);
  }

  static Serde<List<?>> createSerde(final PersistenceSchema schema) {
    final List<SimpleColumn> columns = schema.columns();
    if (columns.isEmpty()) {
      // No columns:
      return new KsqlVoidSerde<>();
    }

    if (columns.size() != 1) {
      throw new KsqlException("The '" + FormatFactory.KAFKA.name()
          + "' format only supports a single field. Got: " + columns);
    }

    final SimpleColumn singleColumn = columns.get(0);

    final Class<?> javaType = SchemaConverters.sqlToJavaConverter()
        .toJavaType(singleColumn.type());

    return createSerde(singleColumn, javaType);
  }

  private static <T> Serde<List<?>> createSerde(
      final SimpleColumn singleColumn,
      final Class<T> javaType
  ) {
    final Serde<T> primitiveSerde = getPrimitiveSerde(singleColumn.type().baseType(), javaType);

    final Serializer<List<?>> serializer = new RowSerializer<>(
        primitiveSerde.serializer(),
        javaType
    );

    final Deserializer<List<?>> deserializer = new RowDeserializer(
        primitiveSerde.deserializer()
    );

    return Serdes.serdeFrom(serializer, deserializer);
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  @VisibleForTesting
  public static <T> Serde<T> getPrimitiveSerde(final SqlBaseType sqlType, final Class<T> javaType) {
    final Serde<?> serde = SERDE.get(javaType);
    if (serde == null) {
      throw new KsqlException("The '" + FormatFactory.KAFKA.name()
          + "' format does not support type '" + sqlType + "'");
    }

    return (Serde) serde;
  }

  private static final class RowSerializer<T> implements Serializer<List<?>> {

    private final Serializer<T> delegate;
    private final Class<T> javaType;

    RowSerializer(final Serializer<T> delegate, final Class<T> javaType) {
      this.delegate = Objects.requireNonNull(delegate, "delegate");
      this.javaType = Objects.requireNonNull(javaType, "javaType");
    }

    @Override
    public byte[] serialize(final String topic, final List<?> values) {
      if (values == null) {
        return null;
      }

      SerdeUtils.throwOnColumnCountMismatch(1, values.size(), true, topic);

      final T value = SerdeUtils.safeCast(values.get(0), javaType);
      return delegate.serialize(topic, value);
    }
  }

  private static final class RowDeserializer implements Deserializer<List<?>> {

    private final Deserializer<?> delegate;

    RowDeserializer(final Deserializer<?> delegate) {
      this.delegate = Objects.requireNonNull(delegate, "delegate");
    }

    @Override
    public List<?> deserialize(final String topic, final byte[] bytes) {
      try {
        final Object primitive = delegate.deserialize(topic, bytes);
        if (primitive == null) {
          return null;
        }

        return Collections.singletonList(primitive);
      } catch (final Exception e) {
        throw new SerializationException(
            "Error deserializing KAFKA message from topic: " + topic, e);
      }
    }
  }
}
