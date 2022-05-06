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

import com.google.common.annotations.VisibleForTesting;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.GenericKey;
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
import io.confluent.ksql.serde.tracked.TrackedCallback;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.WindowedSerdes.SessionWindowedSerde;
import org.apache.kafka.streams.kstream.WindowedSerdes.TimeWindowedSerde;

public final class GenericKeySerDe implements KeySerdeFactory {

  private final GenericSerdeFactory innerFactory;
  private Optional<String> queryId;

  public GenericKeySerDe() {
    this(new GenericSerdeFactory(), Optional.empty());
  }

  public GenericKeySerDe(final String queryId) {
    this(
        new GenericSerdeFactory(),
        Optional.of(queryId)
    );
  }

  @VisibleForTesting
  GenericKeySerDe(
      final GenericSerdeFactory innerFactory,
      final Optional<String> queryId
  ) {
    this.innerFactory = Objects.requireNonNull(innerFactory, "innerFactory");
    this.queryId = queryId;
  }

  @Override
  public Serde<GenericKey> create(
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
  public Serde<Windowed<GenericKey>> create(
      final FormatInfo format,
      final WindowInfo window,
      final PersistenceSchema schema,
      final KsqlConfig ksqlConfig,
      final Supplier<SchemaRegistryClient> schemaRegistryClientFactory,
      final String loggerNamePrefix,
      final ProcessingLogContext processingLogContext,
      final Optional<TrackedCallback> tracker
  ) {
    final Serde<GenericKey> inner = createInner(
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

  private Serde<GenericKey> createInner(
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

    final Serde<GenericKey> genericKeySerde = toGenericKeySerde(formatSerde, schema);

    final Serde<GenericKey> loggingSerde = innerFactory.wrapInLoggingSerde(
        genericKeySerde,
        loggerNamePrefix,
        processingLogContext,
        queryId);

    final Serde<GenericKey> serde = tracker
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

    for (final SimpleColumn column : columns) {
      if (containsMapType(column.type())) {
        throw new KsqlException("Map keys, including types that contain maps, are not supported "
            + "as they may lead to unexpected behavior due to inconsistent serialization. "
            + "Key column name: " + column.name() + ". "
            + "Column type: " + column.type().toString(FormatOptions.none()) + ". "
            + "See https://github.com/confluentinc/ksql/issues/6621 for more.");
      }
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
          .map(SqlStruct.Field::type)
          .anyMatch(GenericKeySerDe::containsMapType);
    }

    throw new IllegalStateException("Unexpected type: " + type);
  }

  private static Serde<GenericKey> toGenericKeySerde(
      final Serde<List<?>> innerSerde,
      final PersistenceSchema schema
  ) {
    final Serializer<GenericKey> serializer = new GenericSerializer<>(
        GenericKey::values,
        innerSerde.serializer(),
        schema.columns().size()
    );

    final Deserializer<GenericKey> deserializer = new GenericDeserializer<>(
        GenericKey::fromList,
        innerSerde.deserializer(),
        schema.columns().size()
    );

    return Serdes.serdeFrom(serializer, deserializer);
  }
}
