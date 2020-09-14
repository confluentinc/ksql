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
import io.confluent.ksql.schema.ksql.SimpleColumn;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.serde.FormatFactory;
import io.confluent.ksql.serde.connect.ConnectSchemas;
import io.confluent.ksql.serde.voids.KsqlVoidSerde;
import io.confluent.ksql.util.KsqlException;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;

public final class KafkaSerdeFactory {

  // Note: If supporting new types here, add new type of PRINT TOPIC support too
  private static final ImmutableMap<SqlType, Serde<?>> SERDE = ImmutableMap.of(
      SqlTypes.INTEGER, Serdes.Integer(),
      SqlTypes.BIGINT, Serdes.Long(),
      SqlTypes.DOUBLE, Serdes.Double(),
      SqlTypes.STRING, Serdes.String()
  );

  private KafkaSerdeFactory() {
  }

  static Serde<Struct> createSerde(final PersistenceSchema schema) {
    final Serde<Object> primitiveSerde = getPrimitiveSerde(schema);

    final ConnectSchema connectSchema = ConnectSchemas.columnsToConnectSchema(schema.columns());

    final Serializer<Struct> serializer = new RowSerializer(
        primitiveSerde.serializer(),
        connectSchema
    );

    final Deserializer<Struct> deserializer = new RowDeserializer(
        primitiveSerde.deserializer(),
        connectSchema
    );

    return Serdes.serdeFrom(serializer, deserializer);
  }

  @VisibleForTesting
  @SuppressWarnings({"unchecked", "rawtypes"})
  public static Serde<Object> getPrimitiveSerde(final PersistenceSchema schema) {

    final List<SimpleColumn> columns = schema.columns();
    if (columns.isEmpty()) {
      // No columns:
      return (Serde) new KsqlVoidSerde();
    }

    if (columns.size() != 1) {
      throw new KsqlException("The '" + FormatFactory.KAFKA.name()
          + "' format only supports a single field. Got: " + schema.columns());
    }

    final SqlType type = columns.get(0).type();
    final Serde<?> serde = SERDE.get(type);
    if (serde == null) {
      throw new KsqlException("The '" + FormatFactory.KAFKA.name()
          + "' format does not support type '" + type.baseType() + "'");
    }

    return (Serde) serde;
  }

  private static final class RowSerializer implements Serializer<Struct> {

    private final Serializer<Object> delegate;
    private final Optional<Field> field;

    RowSerializer(final Serializer<Object> delegate, final ConnectSchema schema) {
      this.delegate = Objects.requireNonNull(delegate, "delegate");
      this.field = schema.fields().isEmpty()
          ? Optional.empty()
          : Optional.of(schema.fields().get(0));
    }

    @Override
    public byte[] serialize(final String topic, final Struct struct) {
      final Object value = struct == null || !field.isPresent()
          ? null
          : struct.get(field.get());

      return delegate.serialize(topic, value);
    }
  }

  private static final class RowDeserializer implements Deserializer<Struct> {

    private final Deserializer<Object> delegate;
    private final ConnectSchema schema;
    private final Optional<Field> field;

    RowDeserializer(
        final Deserializer<Object> delegate,
        final ConnectSchema schema
    ) {
      this.delegate = Objects.requireNonNull(delegate, "delegate");
      this.schema = Objects.requireNonNull(schema, "schema");
      this.field = schema.fields().isEmpty()
          ? Optional.empty()
          : Optional.of(schema.fields().get(0));
    }

    @Override
    public Struct deserialize(final String topic, final byte[] bytes) {
      try {
        final Object primitive = delegate.deserialize(topic, bytes);
        if (primitive == null) {
          return null;
        }

        final Struct struct = new Struct(schema);
        struct.put(field.orElseThrow(IllegalStateException::new), primitive);
        return struct;
      } catch (final Exception e) {
        throw new SerializationException(
            "Error deserializing KAFKA message from topic: " + topic, e);
      }
    }
  }
}
