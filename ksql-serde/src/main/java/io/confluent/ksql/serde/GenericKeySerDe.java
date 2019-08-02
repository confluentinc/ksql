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
import io.confluent.ksql.logging.processing.LoggingDeserializer;
import io.confluent.ksql.logging.processing.ProcessingLogContext;
import io.confluent.ksql.logging.processing.ProcessingLogger;
import io.confluent.ksql.schema.ksql.PersistenceSchema;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.SchemaUtil;
import java.util.Collections;
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
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.WindowedSerdes.SessionWindowedSerde;
import org.apache.kafka.streams.kstream.WindowedSerdes.TimeWindowedSerde;

public final class GenericKeySerDe implements KeySerdeFactory {

  static final String DESERIALIZER_LOGGER_NAME = "deserializer";

  private final SerdeFactories serdeFactories;

  public GenericKeySerDe() {
    this(new KsqlSerdeFactories());
  }

  @VisibleForTesting
  GenericKeySerDe(final SerdeFactories serdeFactories) {
    this.serdeFactories = Objects.requireNonNull(serdeFactories, "serdeFactories");
  }

  @Override
  public KeySerde<Struct> create(
      final FormatInfo format,
      final PersistenceSchema schema,
      final KsqlConfig ksqlConfig,
      final Supplier<SchemaRegistryClient> schemaRegistryClientFactory,
      final String loggerNamePrefix,
      final ProcessingLogContext processingLogContext
  ) {
    final Serde<Struct> inner = createInner(
        format,
        schema,
        ksqlConfig,
        schemaRegistryClientFactory,
        loggerNamePrefix,
        processingLogContext,
        getTargetType(schema)
    );

    return createKeySerde(
        inner,
        false,
        format,
        schema,
        ksqlConfig,
        schemaRegistryClientFactory,
        loggerNamePrefix,
        processingLogContext
    );
  }

  @SuppressWarnings("OptionalGetWithoutIsPresent")
  @Override
  public KeySerde<Windowed<Struct>> create(
      final FormatInfo format,
      final WindowInfo window,
      final PersistenceSchema schema,
      final KsqlConfig ksqlConfig,
      final Supplier<SchemaRegistryClient> schemaRegistryClientFactory,
      final String loggerNamePrefix,
      final ProcessingLogContext processingLogContext
  ) {
    final Serde<Struct> inner = createInner(
        format,
        schema,
        ksqlConfig,
        schemaRegistryClientFactory,
        loggerNamePrefix,
        processingLogContext,
        getTargetType(schema)
    );

    final Serde<Windowed<Struct>> windowedSerde = window.getType().requiresWindowSize()
        ? new TimeWindowedSerde<>(inner, window.getSize().get().toMillis())
        : new SessionWindowedSerde<>(inner);

    return createKeySerde(
        windowedSerde,
        true,
        format,
        schema,
        ksqlConfig,
        schemaRegistryClientFactory,
        loggerNamePrefix,
        processingLogContext
    );
  }

  private <T> KeySerde<T> createKeySerde(
      final Serde<T> serde,
      final boolean windowed,
      final FormatInfo format,
      final PersistenceSchema schema,
      final KsqlConfig ksqlConfig,
      final Supplier<SchemaRegistryClient> schemaRegistryClientFactory,
      final String loggerNamePrefix,
      final ProcessingLogContext processingLogContext
  ) {
    return new KeySerde<T>() {

      @Override
      public void configure(final Map<String, ?> configs, final boolean isKey) {
        serde.configure(configs, isKey);
      }

      @Override
      public void close() {
        serde.close();
      }

      @Override
      public Serializer<T> serializer() {
        return serde.serializer();
      }

      @Override
      public Deserializer<T> deserializer() {
        return serde.deserializer();
      }

      @Override
      public KeySerde<Struct> rebind(final PersistenceSchema newKeySchema) {
        return create(
            format,
            newKeySchema,
            ksqlConfig,
            schemaRegistryClientFactory,
            loggerNamePrefix,
            processingLogContext
        );
      }

      @Override
      public KeySerde<Windowed<Struct>> rebind(final WindowInfo window) {
        return create(
            format,
            window,
            schema,
            ksqlConfig,
            schemaRegistryClientFactory,
            loggerNamePrefix,
            processingLogContext
        );
      }

      @Override
      public boolean isWindowed() {
        return windowed;
      }
    };
  }

  private <T> Serde<Struct> createInner(
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
        .getLogger(join(loggerNamePrefix, DESERIALIZER_LOGGER_NAME));

    final Serde<Struct> inner = schema.isUnwrapped()
        ? unwrapped(serde, schema)
        : wrapped(serde, targetType);

    final Serde<Struct> result = Serdes.serdeFrom(
        inner.serializer(),
        new LoggingDeserializer<>(inner.deserializer(), processingLogger)
    );

    result.configure(Collections.emptyMap(), true);

    return result;
  }

  private static Class<?> getTargetType(final PersistenceSchema schema) {
    return SchemaUtil.getJavaType(schema.serializedSchema());
  }

  private static <K> Serde<Struct> unwrapped(
      final Serde<K> innerSerde,
      final PersistenceSchema schema
  ) {
    final Serializer<Struct> serializer =
        new UnwrappedKeySerializer<>(innerSerde.serializer(), schema);

    final Deserializer<Struct> deserializer =
        new UnwrappedKeyDeserializer<>(innerSerde.deserializer(), schema);

    return Serdes.serdeFrom(serializer, deserializer);
  }

  @SuppressWarnings("unchecked")
  private static <T> Serde<Struct> wrapped(
      final Serde<T> innerSerde,
      final Class<T> type
  ) {
    if (type != Struct.class) {
      throw new IllegalArgumentException("Unwrapped must be of type Struct");
    }

    return (Serde) innerSerde;
  }

  static class UnwrappedKeySerializer<K> implements Serializer<Struct> {

    private final Serializer<K> inner;
    private final Field singleField;
    private final ConnectSchema schema;

    UnwrappedKeySerializer(final Serializer<K> inner, final PersistenceSchema schema) {
      this.inner = requireNonNull(inner, "inner");
      this.schema = requireNonNull(schema, "schema").ksqlSchema();
      this.singleField = this.schema.fields().get(0);
      if (this.schema.fields().size() != 1) {
        throw new IllegalArgumentException("Serializer only supports single field");
      }
    }

    @Override
    public void configure(final Map<String, ?> configs, final boolean isKey) {
      inner.configure(configs, isKey);
    }

    @SuppressWarnings("unchecked")
    @Override
    public byte[] serialize(final String topic, final Struct data) {
      if (data == null) {
        return inner.serialize(topic, null);
      }

      if (data.schema() != schema) {
        throw new SerializationException("Schema mismatch."
            + " expect: " + schema
            + " got: " + data.schema()
        );
      }

      final Object value = data.get(singleField);
      return inner.serialize(topic, (K) value);
    }

    @Override
    public void close() {
      inner.close();
    }
  }

  private static class UnwrappedKeyDeserializer<K> implements Deserializer<Struct> {

    private final Deserializer<K> inner;
    private final Field singleField;
    private final ConnectSchema schema;

    UnwrappedKeyDeserializer(final Deserializer<K> inner, final PersistenceSchema schema) {
      this.inner = requireNonNull(inner, "inner");
      this.schema = requireNonNull(schema, "schema").ksqlSchema();
      this.singleField = this.schema.fields().get(0);
      if (this.schema.fields().size() != 1) {
        throw new IllegalArgumentException("Serializer only supports single field");
      }
    }

    @Override
    public void configure(final Map<String, ?> configs, final boolean isKey) {
      inner.configure(configs, isKey);
    }

    @Override
    public Struct deserialize(final String topic, final byte[] data) {
      final K value = inner.deserialize(topic, data);
      if (value == null) {
        return null;
      }

      final Struct struct = new Struct(schema);
      struct.put(singleField, value);
      return struct;
    }

    @Override
    public void close() {
      inner.close();
    }
  }
}
