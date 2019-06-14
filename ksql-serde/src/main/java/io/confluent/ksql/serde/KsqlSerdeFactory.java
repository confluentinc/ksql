/*
 * Copyright 2018 Confluent Inc.
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

import com.google.errorprone.annotations.Immutable;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.logging.processing.ProcessingLogContext;
import io.confluent.ksql.logging.processing.ProcessingLogger;
import io.confluent.ksql.schema.ksql.PersistenceSchema;
import io.confluent.ksql.util.KsqlConfig;
import java.util.Objects;
import java.util.function.Supplier;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

@Immutable
public abstract class KsqlSerdeFactory {

  private static final String DESERIALIZER_LOGGER_NAME = "deserializer";

  private final Format format;

  protected KsqlSerdeFactory(final Format format) {
    this.format = format;
  }

  public Format getFormat() {
    return format;
  }

  public Serde<Object> createSerde(
      final PersistenceSchema schema,
      final KsqlConfig ksqlConfig,
      final Supplier<SchemaRegistryClient> schemaRegistryClientFactory,
      final String loggerNamePrefix,
      final ProcessingLogContext processingLogContext
  ) {
    final ProcessingLogger processingLogger = processingLogContext.getLoggerFactory()
        .getLogger(join(loggerNamePrefix, DESERIALIZER_LOGGER_NAME));

    final Serializer<Object> serializer = createSerializer(
        schema,
        ksqlConfig,
        schemaRegistryClientFactory
    );

    final Deserializer<Object> deserializer = createDeserializer(
        schema,
        ksqlConfig,
        schemaRegistryClientFactory,
        processingLogger
    );

    return Serdes.serdeFrom(serializer, deserializer);
  }

  protected abstract Serializer<Object> createSerializer(
      PersistenceSchema schema,
      KsqlConfig ksqlConfig,
      Supplier<SchemaRegistryClient> schemaRegistryClientFactory
  );

  protected abstract Deserializer<Object> createDeserializer(
      PersistenceSchema schema,
      KsqlConfig ksqlConfig,
      Supplier<SchemaRegistryClient> schemaRegistryClientFactory,
      ProcessingLogger processingLogger
  );

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof KsqlSerdeFactory)) {
      return false;
    }
    final KsqlSerdeFactory that = (KsqlSerdeFactory) o;
    return format == that.format;
  }

  @Override
  public int hashCode() {
    return Objects.hash(format);
  }
}
