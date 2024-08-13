/*
 * Copyright 2020 Confluent Inc.
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

import com.google.common.annotations.VisibleForTesting;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.SchemaNotSupportedException;
import io.confluent.ksql.logging.processing.LoggingDeserializer;
import io.confluent.ksql.logging.processing.LoggingSerializer;
import io.confluent.ksql.logging.processing.ProcessingLogContext;
import io.confluent.ksql.logging.processing.ProcessingLogger;
import io.confluent.ksql.schema.ksql.PersistenceSchema;
import io.confluent.ksql.serde.tracked.TrackedCallback;
import io.confluent.ksql.serde.tracked.TrackedSerde;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.MetricsTagsUtil;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

final class GenericSerdeFactory {

  private static final String SERIALIZER_LOGGER_NAME = "serializer";
  private static final String DESERIALIZER_LOGGER_NAME = "deserializer";

  private final Function<FormatInfo, Format> formatFactory;

  GenericSerdeFactory() {
    this(FormatFactory::of);
  }

  @VisibleForTesting
  GenericSerdeFactory(final Function<FormatInfo, Format> formatFactory) {
    this.formatFactory = Objects.requireNonNull(formatFactory, "formatFactory");
  }

  Serde<List<?>> createFormatSerde(
      final String target,
      final FormatInfo formatInfo,
      final PersistenceSchema schema,
      final KsqlConfig ksqlConfig,
      final Supplier<SchemaRegistryClient> schemaRegistryClientFactory,
      final boolean isKey
  ) {
    final Format format = formatFactory.apply(formatInfo);

    try {
      return format
          .getSerde(schema,
              formatInfo.getProperties(),
              ksqlConfig,
              schemaRegistryClientFactory,
              isKey
          );
    } catch (final Exception e) {
      throw new SchemaNotSupportedException(target + " format does not support schema."
          + System.lineSeparator()
          + "format: " + format.name()
          + System.lineSeparator()
          + "schema: " + schema
          + System.lineSeparator()
          + "reason: " + e.getMessage(),
          e
      );
    }
  }

  <T> Serde<T> wrapInLoggingSerde(
          final Serde<T> formatSerde,
          final String loggerNamePrefix,
          final ProcessingLogContext processingLogContext
  ) {
    return wrapInLoggingSerde(
        formatSerde,
        loggerNamePrefix,
        processingLogContext,
        Optional.empty()
    );
  }

  @SuppressWarnings("MethodMayBeStatic") // Part of injected API
  <T> Serde<T> wrapInLoggingSerde(
      final Serde<T> formatSerde,
      final String loggerNamePrefix,
      final ProcessingLogContext processingLogContext,
      final Optional<String> queryId
  ) {
    final ProcessingLogger serializerProcessingLogger;
    final ProcessingLogger deserializerProcessingLogger;
    if (queryId.isPresent()) {
      serializerProcessingLogger = processingLogContext.getLoggerFactory()
          .getLogger(
              join(loggerNamePrefix, SERIALIZER_LOGGER_NAME),
              MetricsTagsUtil.getMetricsTagsWithQueryId(queryId.get(), Collections.emptyMap()));
      deserializerProcessingLogger = processingLogContext.getLoggerFactory()
          .getLogger(
              join(loggerNamePrefix, DESERIALIZER_LOGGER_NAME),
              MetricsTagsUtil.getMetricsTagsWithQueryId(queryId.get(), Collections.emptyMap()));
    } else {
      serializerProcessingLogger = processingLogContext.getLoggerFactory()
          .getLogger(join(loggerNamePrefix, SERIALIZER_LOGGER_NAME));
      deserializerProcessingLogger = processingLogContext.getLoggerFactory()
          .getLogger(join(loggerNamePrefix, DESERIALIZER_LOGGER_NAME));
    }

    return Serdes.serdeFrom(
        new LoggingSerializer<>(formatSerde.serializer(), serializerProcessingLogger),
        new LoggingDeserializer<>(formatSerde.deserializer(), deserializerProcessingLogger)
    );
  }

  @SuppressWarnings("MethodMayBeStatic") // Part of injected API
  <T> Serde<T> wrapInTrackingSerde(
      final Serde<T> serde,
      final TrackedCallback callback
  ) {
    return TrackedSerde.from(serde, callback);
  }
}
