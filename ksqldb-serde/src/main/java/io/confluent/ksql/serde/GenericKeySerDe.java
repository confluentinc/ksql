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
import io.confluent.ksql.schema.ksql.types.SqlArray;
import io.confluent.ksql.schema.ksql.types.SqlDecimal;
import io.confluent.ksql.schema.ksql.types.SqlMap;
import io.confluent.ksql.schema.ksql.types.SqlPrimitiveType;
import io.confluent.ksql.schema.ksql.types.SqlStruct;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.schema.utils.FormatOptions;
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
    checkUnsupportedSchema(schema.columns(), ksqlConfig);

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

  private static void checkUnsupportedSchema(
      final List<SimpleColumn> columns,
      final KsqlConfig config
  ) {
    if (columns.isEmpty()) {
      return;
    }

    if (config.getBoolean(KsqlConfig.KSQL_KEY_FORMAT_ENABLED)) {
      for (final SimpleColumn column : columns) {
        if (containsMapType(column.type())) {
          throw new KsqlException("Map keys, including types that contain maps, are not supported "
              + "as they may lead to unexpected behavior due to inconsistent serialization. "
              + "Key column name: " + column.name() + ". "
              + "Column type: " + column.type().toString(FormatOptions.none()));
        }
      }
      return;
    }

    if (columns.size() > 1) {
      throw new KsqlException(
          "Only single KEY column supported. Multiple KEY columns found: " + columns);
    }

    final SqlType sqlType = columns.get(0).type();
    if (!(sqlType instanceof SqlPrimitiveType || sqlType instanceof SqlDecimal)) {
      throw new KsqlException("Unsupported key schema: " + columns);
    }
  }

  private static boolean containsMapType(final SqlType type) {
    if (type instanceof SqlMap) {
      return true;
    }

    if (type instanceof SqlPrimitiveType || type instanceof SqlDecimal) {
      return false;
    }

    if (type instanceof SqlArray) {
      return containsMapType(((SqlArray) type).getItemType());
    }

    if (type instanceof SqlStruct) {
      return ((SqlStruct) type).fields().stream()
          .map(io.confluent.ksql.schema.ksql.types.Field::type)
          .anyMatch(GenericKeySerDe::containsMapType);
    }

    throw new IllegalStateException("Unexpected type: " + type);
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
