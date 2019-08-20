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
import io.confluent.ksql.logging.processing.LoggingDeserializer;
import io.confluent.ksql.logging.processing.ProcessingLogContext;
import io.confluent.ksql.logging.processing.ProcessingLogger;
import io.confluent.ksql.schema.ksql.PersistenceSchema;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.SchemaUtil;
import java.util.ArrayList;
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

public final class GenericRowSerDe implements ValueSerdeFactory {

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
    final Serde<T> serde = serdeFactories
        .create(format, schema, ksqlConfig, schemaRegistryClientFactory, targetType);

    final ProcessingLogger processingLogger = processingLogContext.getLoggerFactory()
        .getLogger(join(loggerNamePrefix, GenericKeySerDe.DESERIALIZER_LOGGER_NAME));

    final Serde<GenericRow> genericRowSerde = schema.isUnwrapped()
          ? unwrapped(serde)
          : wrapped(serde, schema, targetType);

    final Serde<GenericRow> result = Serdes.serdeFrom(
        genericRowSerde.serializer(),
        new LoggingDeserializer<>(genericRowSerde.deserializer(), processingLogger)
    );

    result.configure(Collections.emptyMap(), false);

    return result;
  }

  private static Class<?> getTargetType(final PersistenceSchema schema) {
    return SchemaUtil.getJavaType(schema.serializedSchema());
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

      if (data.getColumns().size() != 1) {
        throw new SerializationException("Expected single-field value. "
            + "got: " + data.getColumns().size());
      }

      final Object singleField = data.getColumns().get(0);
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

      final GenericRow row = new GenericRow();
      row.getColumns().add(value);
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

      if (data.getColumns().size() != schema.fields().size()) {
        throw new SerializationException("Field count mismatch."
            + " expected: " + schema.fields().size()
            + ", got: " + data.getColumns().size()
        );
      }

      final Struct struct = new Struct(schema);
      for (int i = 0; i < data.getColumns().size(); i++) {
        struct.put(schema.fields().get(i), data.getColumns().get(i));
      }

      return inner.serialize(topic, struct);
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
      final List<Object> columns = new ArrayList<>(fields.size());

      for (final Field field : fields) {
        final Object columnVal = struct.get(field);
        columns.add(columnVal);
      }

      return new GenericRow(columns);
    }
  }
}
