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

package io.confluent.ksql.serde;

import static io.confluent.ksql.logging.processing.ProcessingLoggerUtil.join;
import static java.util.Objects.requireNonNull;

import com.google.common.annotations.VisibleForTesting;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.SchemaNotSupportedException;
import io.confluent.ksql.logging.processing.LoggingDeserializer;
import io.confluent.ksql.logging.processing.LoggingSerializer;
import io.confluent.ksql.logging.processing.ProcessingLogContext;
import io.confluent.ksql.logging.processing.ProcessingLogger;
import io.confluent.ksql.schema.ksql.PersistenceSchema;
import io.confluent.ksql.schema.ksql.SchemaConverters;
import io.confluent.ksql.schema.ksql.SystemColumns;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;

public final class GenericRowSerDe implements ValueSerdeFactory {

  /**
   * Additional capacity added to each created `GenericRow` in an attempt to avoid later resizes,
   * and associated array copies, when the row has additional elements appended to the end during
   * processing, e.g. to match columns added by
   * {@link io.confluent.ksql.schema.ksql.LogicalSchema#withPseudoAndKeyColsInValue(boolean)}
   *
   * <p>The number is optimised for a single key column, as this is the most common case.
   *
   * <p>Count covers the following additional columns:
   * <ol>
   *   <li>{@link SystemColumns#ROWTIME_NAME}</li>
   *   <li>A single key column. (Which is the most common case)</li>
   *   <li>{@link SystemColumns#WINDOWSTART_NAME}</li>
   *   <li>{@link SystemColumns#WINDOWEND_NAME}</li>
   * </ol>
   *
   */
  private static final int ADDITIONAL_CAPACITY = 4;

  private final SerdeFactories serdeFactories;

  public GenericRowSerDe() {
    this(new KsqlSerdeFactories());
  }

  @VisibleForTesting
  GenericRowSerDe(final SerdeFactories serdeFactories) {
    this.serdeFactories = Objects.requireNonNull(serdeFactories, "serdeFactories");
  }

  @Override
  public Serde<GenericRow> create(
      final FormatInfo format,
      final PersistenceSchema schema,
      final KsqlConfig ksqlConfig,
      final Supplier<SchemaRegistryClient> schemaRegistryClientFactory,
      final String loggerNamePrefix,
      final ProcessingLogContext processingLogContext
  ) {
    return from(
        format,
        schema,
        ksqlConfig,
        schemaRegistryClientFactory,
        loggerNamePrefix,
        processingLogContext,
        getTargetType(schema)
    );
  }

  public static Serde<GenericRow> from(
      final FormatInfo format,
      final PersistenceSchema schema,
      final KsqlConfig ksqlConfig,
      final Supplier<SchemaRegistryClient> schemaRegistryClientFactory,
      final String loggerNamePrefix,
      final ProcessingLogContext processingLogContext
  ) {
    return new GenericRowSerDe().create(
        format,
        schema,
        ksqlConfig,
        schemaRegistryClientFactory,
        loggerNamePrefix,
        processingLogContext
    );
  }

  private <T> Serde<GenericRow> from(
      final FormatInfo format,
      final PersistenceSchema schema,
      final KsqlConfig ksqlConfig,
      final Supplier<SchemaRegistryClient> schemaRegistryClientFactory,
      final String loggerNamePrefix,
      final ProcessingLogContext processingLogContext,
      final Class<T> targetType
  ) {
    try {
      serdeFactories.validate(format, schema);
    } catch (final Exception e) {
      throw new SchemaNotSupportedException("Value format does not support value schema."
          + System.lineSeparator()
          + "format: " + format.getFormat()
          + System.lineSeparator()
          + "schema: " + schema
          + System.lineSeparator()
          + "reason: " + e.getMessage(),
          e
      );
    }

    final Serde<T> serde = serdeFactories
        .create(format, schema, ksqlConfig, schemaRegistryClientFactory, targetType);

    final ProcessingLogger serializerProcessingLogger = processingLogContext.getLoggerFactory()
        .getLogger(join(loggerNamePrefix, GenericKeySerDe.SERIALIZER_LOGGER_NAME));
    final ProcessingLogger deserializerProcessingLogger = processingLogContext.getLoggerFactory()
        .getLogger(join(loggerNamePrefix, GenericKeySerDe.DESERIALIZER_LOGGER_NAME));

    final Serde<GenericRow> genericRowSerde = schema.isUnwrapped()
          ? unwrapped(serde)
          : wrapped(serde, schema, targetType);

    final Serde<GenericRow> result = Serdes.serdeFrom(
        new LoggingSerializer<>(genericRowSerde.serializer(), serializerProcessingLogger),
        new LoggingDeserializer<>(genericRowSerde.deserializer(), deserializerProcessingLogger)
    );

    result.configure(Collections.emptyMap(), false);

    return result;
  }

  private static Class<?> getTargetType(final PersistenceSchema schema) {
    return SchemaConverters.sqlToJavaConverter().toJavaType(
        SchemaConverters.connectToSqlConverter().toSqlType(schema.serializedSchema())
    );
  }

  private static <K> Serde<GenericRow> unwrapped(final Serde<K> innerSerde) {
    final Serializer<GenericRow> serializer =
        new UnwrappedGenericRowSerializer<>(innerSerde.serializer());

    final Deserializer<GenericRow> deserializer =
        new UnwrappedGenericRowDeserializer<>(innerSerde.deserializer());

    return Serdes.serdeFrom(serializer, deserializer);
  }

  private static <T> Serde<GenericRow> wrapped(
      final Serde<T> innerSerde,
      final PersistenceSchema schema,
      final Class<T> type
  ) {
    if (type != Struct.class) {
      throw new IllegalArgumentException("Unwrapped must be of type Struct");
    }

    @SuppressWarnings("unchecked") final Serde<Struct> structSerde = (Serde<Struct>) innerSerde;

    final Serializer<GenericRow> serializer =
        new GenericRowSerializer(structSerde.serializer(), schema);

    final Deserializer<GenericRow> deserializer =
        new GenericRowDeserializer(structSerde.deserializer());

    return Serdes.serdeFrom(serializer, deserializer);
  }

  private static class UnwrappedGenericRowSerializer<K> implements Serializer<GenericRow> {

    private final Serializer<K> inner;

    UnwrappedGenericRowSerializer(final Serializer<K> inner) {
      this.inner = requireNonNull(inner, "inner");
    }

    @Override
    public void configure(final Map<String, ?> configs, final boolean isKey) {
      inner.configure(configs, isKey);
    }

    @SuppressWarnings("unchecked")
    @Override
    public byte[] serialize(final String topic, final GenericRow data) {
      if (data == null) {
        return inner.serialize(topic, null);
      }

      if (data.size() != 1) {
        throw new SerializationException("Expected single-field value. "
            + "got: " + data.size());
      }

      final Object singleField = data.get(0);
      return inner.serialize(topic, (K) singleField);
    }
  }

  private static class UnwrappedGenericRowDeserializer<K> implements Deserializer<GenericRow> {

    private final Deserializer<K> inner;

    UnwrappedGenericRowDeserializer(final Deserializer<K> inner) {
      this.inner = requireNonNull(inner, "inner");
    }

    @Override
    public void configure(final Map<String, ?> configs, final boolean isKey) {
      inner.configure(configs, isKey);
    }

    @Override
    public GenericRow deserialize(final String topic, final byte[] data) {
      final K value = inner.deserialize(topic, data);
      if (value == null) {
        return null;
      }

      final GenericRow row = new GenericRow(1 + ADDITIONAL_CAPACITY);
      row.append(value);
      return row;
    }
  }

  private static class GenericRowSerializer implements Serializer<GenericRow> {

    private final Serializer<Struct> inner;
    private final ConnectSchema schema;

    GenericRowSerializer(final Serializer<Struct> inner, final PersistenceSchema schema) {
      this.inner = requireNonNull(inner, "inner");
      this.schema = requireNonNull(schema, "schema").ksqlSchema();
    }

    @Override
    public void configure(final Map<String, ?> configs, final boolean isKey) {
      inner.configure(configs, isKey);
    }

    @Override
    public byte[] serialize(final String topic, final GenericRow data) {
      if (data == null) {
        return inner.serialize(topic, null);
      }

      if (data.size() != schema.fields().size()) {
        throw new SerializationException("Field count mismatch."
            + " topic: " + topic
            + ", expected: " + schema.fields().size()
            + ", got: " + data.size()
        );
      }

      final Struct struct = new Struct(schema);
      for (int i = 0; i < data.size(); i++) {
        putField(struct, schema.fields().get(i), data.get(i));
      }

      return inner.serialize(topic, struct);
    }

    private void putField(final Struct struct, final Field field, final Object value) {
      try {
        struct.put(field, value);
      } catch (DataException e) {
        // Add more info to error message in case of Struct to call out struct schemas
        // with non-optional fields from incorrectly-written UDFs as a potential cause:
        // https://github.com/confluentinc/ksql/issues/5364
        if (!(value instanceof Struct)) {
          throw e;
        } else {
          throw new KsqlException(
              "Failed to prepare Struct value field '" + field.name() + "' for serialization. "
                  + "This could happen if the value was produced by a user-defined function "
                  + "where the schema has non-optional return types. ksqlDB requires all "
                  + "schemas to be optional at all levels of the Struct: the Struct itself, "
                  + "schemas for all fields within the Struct, and so on.",
              e);
        }
      }
    }
  }

  private static class GenericRowDeserializer implements Deserializer<GenericRow> {

    private final Deserializer<Struct> inner;

    GenericRowDeserializer(final Deserializer<Struct> inner) {
      this.inner = requireNonNull(inner, "inner");
    }

    @Override
    public void configure(final Map<String, ?> configs, final boolean isKey) {
      inner.configure(configs, isKey);
    }

    @Override
    public GenericRow deserialize(final String topic, final byte[] data) {
      final Struct struct = inner.deserialize(topic, data);
      if (struct == null) {
        return null;
      }

      final List<Field> fields = struct.schema().fields();

      final GenericRow row = new GenericRow(fields.size() + ADDITIONAL_CAPACITY);

      for (final Field field : fields) {
        final Object columnVal = struct.get(field);
        row.append(columnVal);
      }

      return row;
    }
  }
}
