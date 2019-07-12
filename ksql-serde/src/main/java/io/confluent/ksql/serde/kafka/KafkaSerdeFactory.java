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

import com.google.common.collect.ImmutableMap;
import com.google.errorprone.annotations.Immutable;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.logging.processing.ProcessingLogger;
import io.confluent.ksql.schema.connect.SqlSchemaFormatter;
import io.confluent.ksql.schema.connect.SqlSchemaFormatter.Option;
import io.confluent.ksql.schema.ksql.PersistenceSchema;
import io.confluent.ksql.serde.Format;
import io.confluent.ksql.serde.KsqlSerdeFactory;
import io.confluent.ksql.serde.util.SerdeProcessingLogMessageFactory;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
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
public class KafkaSerdeFactory extends KsqlSerdeFactory {

  private static final Map<Type, Serde<?>> SERDE = ImmutableMap.of(
      Type.STRING, Serdes.String(),
      Type.INT32, Serdes.Integer(),
      Type.INT64, Serdes.Long(),
      Type.FLOAT64, Serdes.Double()
  );

  public KafkaSerdeFactory() {
    super(Format.KAFKA);
  }

  @Override
  public void validate(final ConnectSchema schema) {
    getPrimitiveSerde(schema);
  }

  @Override
  protected Serializer<Object> createSerializer(
      final PersistenceSchema schema,
      final KsqlConfig ksqlConfig,
      final Supplier<SchemaRegistryClient> schemaRegistryClientFactory
  ) {
    final Serializer<Object> primitiveSerializer = getPrimitiveSerde(schema.getConnectSchema())
        .serializer();

    primitiveSerializer.configure(Collections.emptyMap(), false);

    return new RowSerializer(primitiveSerializer, schema.getConnectSchema());
  }

  @Override
  protected Deserializer<Object> createDeserializer(
      final PersistenceSchema schema,
      final KsqlConfig ksqlConfig,
      final Supplier<SchemaRegistryClient> schemaRegistryClientFactory,
      final ProcessingLogger processingLogger
  ) {
    final Deserializer<Object> primitiveDeserializer = getPrimitiveSerde(schema.getConnectSchema())
        .deserializer();

    primitiveDeserializer.configure(Collections.emptyMap(), false);

    return new RowDeserializer(primitiveDeserializer, schema.getConnectSchema(), processingLogger);
  }

  @SuppressWarnings("unchecked")
  private static Serde<Object> getPrimitiveSerde(final ConnectSchema schema) {
    if (schema.type() != Type.STRUCT) {
      throw new IllegalArgumentException("KAFKA format does not support unwrapping");
    }

    final List<Field> fields = schema.fields();
    if (fields.size() != 1) {
      final String got = new SqlSchemaFormatter(w -> false, Option.AS_COLUMN_LIST).format(schema);
      throw new KsqlException("The '" + Format.KAFKA
          + "' format only supports a single field. Got: " + got);
    }

    final Type type = fields.get(0).schema().type();
    final Serde<?> serde = SERDE.get(type);
    if (serde == null) {
      final String typeString = type == Type.BYTES ? "DECIMAL" : type.toString();
      throw new KsqlException("The '" + Format.KAFKA
          + "' format does not support type '" + typeString + "'");
    }

    return (Serde) serde;
  }

  private static final class RowSerializer implements Serializer<Object> {

    private final Serializer<Object> delegate;
    private final Field field;

    RowSerializer(final Serializer<Object> delegate, final ConnectSchema schema) {
      this.delegate = Objects.requireNonNull(delegate, "delegate");
      this.field = schema.fields().get(0);
    }

    @Override
    public byte[] serialize(final String topic, final Object struct) {
      final Object value = ((Struct) struct).get(field);
      return delegate.serialize(topic, value);
    }
  }

  private static final class RowDeserializer implements Deserializer<Object> {

    private final Deserializer<Object> delegate;
    private final Struct struct;
    private final Field field;
    private final ProcessingLogger processingLogger;

    RowDeserializer(
        final Deserializer<Object> delegate,
        final ConnectSchema schema,
        final ProcessingLogger processingLogger
    ) {
      this.delegate = Objects.requireNonNull(delegate, "delegate");
      this.struct = new Struct(schema);
      this.field = schema.fields().get(0);
      this.processingLogger = Objects.requireNonNull(processingLogger, "processingLogger");
    }

    @Override
    public Struct deserialize(final String topic, final byte[] bytes) {
      try {
        final Object primitive = delegate.deserialize(topic, bytes);
        struct.put(field, primitive);
        return struct;
      } catch (final Exception e) {
        processingLogger.error(
            SerdeProcessingLogMessageFactory.deserializationErrorMsg(
                e,
                Optional.ofNullable(bytes))
        );
        throw new SerializationException(
            "Error deserializing DELIMITED message from topic: " + topic, e);
      }
    }
  }
}
