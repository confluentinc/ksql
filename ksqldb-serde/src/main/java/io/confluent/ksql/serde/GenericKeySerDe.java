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
import io.confluent.ksql.logging.processing.ProcessingLogContext;
import io.confluent.ksql.schema.ksql.PersistenceSchema;
import io.confluent.ksql.util.KsqlConfig;
import java.util.Collections;
import java.util.Objects;
import java.util.function.Supplier;
import org.apache.kafka.common.serialization.Serde;
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
      final ProcessingLogContext processingLogContext
  ) {
    return createInner(
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
  public Serde<Windowed<Struct>> create(
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
        processingLogContext
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
      final ProcessingLogContext processingLogContext
  ) {
    final Serde<Struct> formatSerde = innerFactory
        .createFormatSerde("Key", format, schema, ksqlConfig, schemaRegistryClientFactory);

    final Serde<Struct> serde = innerFactory
        .wrapInLoggingSerde(formatSerde, loggerNamePrefix, processingLogContext);

    serde.configure(Collections.emptyMap(), true);

    return serde;
  }
}
