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

import static java.util.Objects.requireNonNull;

import com.google.common.annotations.VisibleForTesting;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.logging.processing.ProcessingLogContext;
import io.confluent.ksql.schema.ksql.PersistenceSchema;
import io.confluent.ksql.schema.ksql.SimpleColumn;
import io.confluent.ksql.schema.ksql.types.SqlDecimal;
import io.confluent.ksql.schema.ksql.types.SqlPrimitiveType;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.serde.connect.ConnectSchemas;
import io.confluent.ksql.serde.tracked.TrackedCallback;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;
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

  private final GenericSerdeFactory innerFactory;

  public GenericKeySerDe() {
    this(new GenericSerdeFactory());
  }

  @VisibleForTesting
  GenericKeySerDe(final GenericSerdeFactory innerFactory) {
    this.innerFactory = Objects.requireNonNull(innerFactory, "innerFactory");
  }

  @Override
  public Serde<Struct> create(
      final FormatInfo format,
      final PersistenceSchema schema,
      final KsqlConfig ksqlConfig,
      final Supplier<SchemaRegistryClient> schemaRegistryClientFactory,
      final String loggerNamePrefix,
      final ProcessingLogContext processingLogContext,
      final Optional<TrackedCallback> tracker
  ) {
    return createInner(
        format,
        schema,
        ksqlConfig,
        schemaRegistryClientFactory,
        loggerNamePrefix,
        processingLogContext,
        tracker
    );
  }

  @SuppressWarnings("OptionalGetWithoutIsPresent")
  @Override
  public Serde<Windowed<Struct>> create(
      final FormatInfo format,
      final WindowInfo window,
      final PersistenceSchema schema,
      final KsqlConfig ksqlConfig,
      final Supplier<SchemaRegistryClient> schemaRegistryClientFactory,
      final String loggerNamePrefix,
      final ProcessingLogContext processingLogContext,
      final Optional<TrackedCallback> tracker
  ) {
    final Serde<Struct> inner = createInner(
        format,
        schema,
        ksqlConfig,
        schemaRegistryClientFactory,
        loggerNamePrefix,
        processingLogContext,
        tracker
    );

    return window.getType().requiresWindowSize()
        ? new TimeWindowedSerde<>(inner, window.getSize().get().toMillis())
        : new SessionWindowedSerde<>(inner);
  }

  private Serde<Struct> createInner(
      final FormatInfo format,
      final PersistenceSchema schema,
      final KsqlConfig ksqlConfig,
      final Supplier<SchemaRegistryClient> schemaRegistryClientFactory,
      final String loggerNamePrefix,
      final ProcessingLogContext processingLogContext,
      final Optional<TrackedCallback> tracker
  ) {
    if (unsupportedSchema(schema.columns(), ksqlConfig)) {
      throw new KsqlException("Unsupported key schema: " + schema.columns());
    }

    final Serde<List<?>> formatSerde = innerFactory
        .createFormatSerde("Key", format, schema, ksqlConfig, schemaRegistryClientFactory, true);

    final Serde<Struct> structSerde = toStructSerde(formatSerde, schema);

    final Serde<Struct> loggingSerde = innerFactory
        .wrapInLoggingSerde(structSerde, loggerNamePrefix, processingLogContext);

    final Serde<Struct> serde = tracker
        .map(callback -> innerFactory.wrapInTrackingSerde(loggingSerde, callback))
        .orElse(loggingSerde);

    serde.configure(Collections.emptyMap(), true);

    return serde;
  }

  private static boolean unsupportedSchema(
      final List<SimpleColumn> columns,
      final KsqlConfig config
  ) {
    if (config.getBoolean(KsqlConfig.KSQL_KEY_FORMAT_ENABLED)) {
      return columns.size() > 1;
    } else {
      if (columns.isEmpty()) {
        return false;
      }

      if (columns.size() > 1) {
        return true;
      }

      final SqlType sqlType = columns.get(0).type();
      return !(sqlType instanceof SqlPrimitiveType || sqlType instanceof SqlDecimal);
    }
  }

  private static Serde<Struct> toStructSerde(
      final Serde<List<?>> inner,
      final PersistenceSchema schema
  ) {
    final ConnectSchema connectSchema = ConnectSchemas.columnsToConnectSchema(schema.columns());
    return Serdes.serdeFrom(
        new GenericKeySerializer(inner.serializer(), connectSchema.fields().size()),
        new GenericKeyDeserializer(inner.deserializer(), connectSchema)
    );
  }

  @VisibleForTesting
  static class GenericKeySerializer implements Serializer<Struct> {

    private final Serializer<List<?>> inner;
    private final int numColumns;

    GenericKeySerializer(final Serializer<List<?>> inner, final int numColumns) {
      this.inner = requireNonNull(inner, "inner");
      this.numColumns = numColumns;
    }

    @Override
    public void configure(final Map<String, ?> configs, final boolean isKey) {
      inner.configure(configs, isKey);
    }

    @Override
    public byte[] serialize(final String topic, final Struct data) {
      if (data == null) {
        return inner.serialize(topic, null);
      }

      final List<Field> fields = data.schema().fields();

      SerdeUtils.throwOnColumnCountMismatch(numColumns, fields.size(), true, topic);

      final ArrayList<Object> values = new ArrayList<>(numColumns);
      for (final Field field : fields) {
        values.add(data.get(field));
      }

      return inner.serialize(topic, values);
    }

    @Override
    public void close() {
      inner.close();
    }
  }

  @VisibleForTesting
  static class GenericKeyDeserializer implements Deserializer<Struct> {

    private final Deserializer<List<?>> inner;
    private final ConnectSchema connectSchema;

    GenericKeyDeserializer(final Deserializer<List<?>> inner, final ConnectSchema connectSchema) {
      this.inner = requireNonNull(inner, "inner");
      this.connectSchema = requireNonNull(connectSchema, "connectSchema");
    }

    @Override
    public void configure(final Map<String, ?> configs, final boolean isKey) {
      inner.configure(configs, isKey);
    }

    @Override
    public void close() {
      inner.close();
    }

    @Override
    public Struct deserialize(final String topic, final byte[] data) {
      final List<?> values = inner.deserialize(topic, data);
      if (values == null) {
        return null;
      }

      final List<Field> fields = connectSchema.fields();

      SerdeUtils.throwOnColumnCountMismatch(fields.size(), values.size(), false, topic);

      final Struct row = new Struct(connectSchema);

      final Iterator<Field> fIt = fields.iterator();
      final Iterator<?> vIt = values.iterator();
      while (fIt.hasNext()) {
        row.put(fIt.next(), vIt.next());
      }

      return row;
    }
  }
}
