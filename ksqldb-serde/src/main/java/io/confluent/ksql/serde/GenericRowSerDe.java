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
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.logging.processing.ProcessingLogContext;
import io.confluent.ksql.schema.ksql.PersistenceSchema;
import io.confluent.ksql.schema.ksql.SystemColumns;
import io.confluent.ksql.serde.tracked.TrackedCallback;
import io.confluent.ksql.util.KsqlConfig;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

public final class GenericRowSerDe implements ValueSerdeFactory {

  /**
   * Additional capacity added to each created `GenericRow` in an attempt to avoid later resizes,
   * and associated array copies, when the row has additional elements appended to the end during
   * processing, e.g. to match columns added by
   * {@link io.confluent.ksql.schema.ksql.LogicalSchema#withPseudoAndKeyColsInValue(boolean, int)}
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

  private final GenericSerdeFactory innerFactory;
  private final Optional<String> queryId;

  public GenericRowSerDe() {
    this(new GenericSerdeFactory(), Optional.empty());
  }

  public GenericRowSerDe(final String queryId) {
    this(
        new GenericSerdeFactory(),
        Optional.of(queryId)
    );
  }

  @VisibleForTesting
  GenericRowSerDe(
      final GenericSerdeFactory innerFactory,
      final Optional<String> queryId
  ) {
    this.innerFactory = Objects.requireNonNull(innerFactory, "innerFactory");
    this.queryId = queryId;
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
        processingLogContext,
        Optional.empty());
  }

  @Override
  public Serde<GenericRow> create(
      final FormatInfo format,
      final PersistenceSchema schema,
      final KsqlConfig ksqlConfig,
      final Supplier<SchemaRegistryClient> srClientFactory,
      final String loggerNamePrefix,
      final ProcessingLogContext processingLogContext,
      final Optional<TrackedCallback> tracker
  ) {
    final Serde<List<?>> formatSerde =
        innerFactory.createFormatSerde("Value", format, schema, ksqlConfig, srClientFactory, false);

    final Serde<GenericRow> genericRowSerde = toGenericRowSerde(formatSerde, schema);

    final Serde<GenericRow> loggingSerde = innerFactory.wrapInLoggingSerde(
        genericRowSerde,
        loggerNamePrefix,
        processingLogContext,
        queryId);

    final Serde<GenericRow> serde = tracker
        .map(callback -> innerFactory.wrapInTrackingSerde(loggingSerde, callback))
        .orElse(loggingSerde);

    serde.configure(Collections.emptyMap(), false);

    return serde;
  }

  private static Serde<GenericRow> toGenericRowSerde(
      final Serde<List<?>> innerSerde,
      final PersistenceSchema schema
  ) {
    final Serializer<GenericRow> serializer = new GenericSerializer<>(
        GenericRow::values,
        innerSerde.serializer(),
        schema.columns().size()
    );

    final Deserializer<GenericRow> deserializer = new GenericDeserializer<>(
        GenericRowSerDe::createGenericRow,
        innerSerde.deserializer(),
        schema.columns().size()
    );

    return Serdes.serdeFrom(serializer, deserializer);
  }

  private static GenericRow createGenericRow(final List<?> values) {
    final GenericRow row = new GenericRow(values.size() + ADDITIONAL_CAPACITY);
    row.appendAll(values);
    return row;
  }
}
