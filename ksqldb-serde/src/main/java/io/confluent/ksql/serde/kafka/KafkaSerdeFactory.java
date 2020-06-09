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
import com.google.errorprone.annotations.Immutable;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.schema.connect.SqlSchemaFormatter;
import io.confluent.ksql.schema.connect.SqlSchemaFormatter.Option;
import io.confluent.ksql.schema.ksql.PersistenceSchema;
import io.confluent.ksql.serde.FormatFactory;
import io.confluent.ksql.serde.KsqlSerdeFactory;
import io.confluent.ksql.serde.voids.KsqlVoidSerde;
import io.confluent.ksql.util.DecimalUtil;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema.Type;
import org.apache.kafka.connect.data.Struct;

@Immutable
public class KafkaSerdeFactory implements KsqlSerdeFactory {

  // Note: If supporting new types here, add new type of PRINT TOPIC support too
  private static final ImmutableMap<Type, Serde<?>> SERDE = ImmutableMap.of(
      Type.INT32, Serdes.Integer(),
      Type.INT64, Serdes.Long(),
      Type.FLOAT64, Serdes.Double(),
      Type.STRING, Serdes.String()
  );

  @Override
  public void validate(final PersistenceSchema schema) {
    getPrimitiveSerde(schema.serializedSchema());
  }

  @Override
  public Serde<Object> createSerde(
      final PersistenceSchema schema,
      final KsqlConfig ksqlConfig,
      final Supplier<SchemaRegistryClient> schemaRegistryClientFactory
  ) {
    final Serde<Object> primitiveSerde = getPrimitiveSerde(schema.serializedSchema());

    final Serializer<Object> serializer = new RowSerializer(
        primitiveSerde.serializer(),
        schema.serializedSchema()
    );

    final Deserializer<Object> deserializer = new RowDeserializer(
        primitiveSerde.deserializer(),
        schema.serializedSchema()
    );

    return Serdes.serdeFrom(serializer, deserializer);
  }

  @VisibleForTesting
  @SuppressWarnings({"unchecked", "rawtypes"})
  public static Serde<Object> getPrimitiveSerde(final ConnectSchema schema) {
    if (schema.type() != Type.STRUCT) {
      throw new IllegalArgumentException("KAFKA format does not support unwrapping");
    }

    final List<Field> fields = schema.fields();
    if (fields.isEmpty()) {
      // No columns:
      return (Serde) new KsqlVoidSerde();
    }

    if (fields.size() != 1) {
      final String got = new SqlSchemaFormatter(w -> false, Option.AS_COLUMN_LIST).format(schema);
      throw new KsqlException("The '" + FormatFactory.KAFKA.name()
          + "' format only supports a single field. Got: " + got);
    }

    final Type type = fields.get(0).schema().type();
    final Serde<?> serde = SERDE.get(type);
    if (serde == null) {
      final String typeString = DecimalUtil.isDecimal(fields.get(0).schema())
          ? "DECIMAL"
          : type.toString();

      throw new KsqlException("The '" + FormatFactory.KAFKA.name()
          + "' format does not support type '" + typeString + "'");
    }

    return (Serde) serde;
  }

  private static final class RowSerializer implements Serializer<Object> {

    private final Serializer<Object> delegate;
    private final Optional<Field> field;

    RowSerializer(final Serializer<Object> delegate, final ConnectSchema schema) {
      this.delegate = Objects.requireNonNull(delegate, "delegate");
      this.field = schema.fields().isEmpty()
          ? Optional.empty()
          : Optional.of(schema.fields().get(0));
    }

    @Override
    public byte[] serialize(final String topic, final Object struct) {
      final Object value = struct == null || !field.isPresent()
          ? null
          : ((Struct) struct).get(field.get());

      return delegate.serialize(topic, value);
    }
  }

  private static final class RowDeserializer implements Deserializer<Object> {

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
            "Error deserializing DELIMITED message from topic: " + topic, e);
      }
    }
  }
}
