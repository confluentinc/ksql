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
import io.confluent.ksql.schema.ksql.PersistenceSchema;
import io.confluent.ksql.serde.avro.KsqlAvroSerdeFactory;
import io.confluent.ksql.serde.delimited.KsqlDelimitedSerdeFactory;
import io.confluent.ksql.serde.json.KsqlJsonSerdeFactory;
import io.confluent.ksql.serde.kafka.KafkaSerdeFactory;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlConstants;
import io.confluent.ksql.util.KsqlException;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.kafka.common.serialization.Serde;

final class KsqlSerdeFactories implements SerdeFactories {

  private final Function<FormatInfo, KsqlSerdeFactory> factoryMethod;

  KsqlSerdeFactories() {
    this(KsqlSerdeFactories::create);
  }

  @VisibleForTesting
  KsqlSerdeFactories(final Function<FormatInfo, KsqlSerdeFactory> factoryMethod) {
    this.factoryMethod = Objects.requireNonNull(factoryMethod, "factoryMethod");
  }

  @Override
  public <K> Serde<K> create(
      final FormatInfo format,
      final PersistenceSchema schema,
      final KsqlConfig ksqlConfig,
      final Supplier<SchemaRegistryClient> schemaRegistryClientFactory,
      final Class<K> type
  ) {
    final KsqlSerdeFactory ksqlSerdeFactory = factoryMethod.apply(format);

    ksqlSerdeFactory.validate(schema);

    return ksqlSerdeFactory.createSerde(schema, ksqlConfig, schemaRegistryClientFactory, type);
  }

  @VisibleForTesting
  static KsqlSerdeFactory create(final FormatInfo format) {
    switch (format.getFormat()) {
      case AVRO:
        final String schemaFullName = format.getAvroFullSchemaName()
            .orElse(KsqlConstants.DEFAULT_AVRO_SCHEMA_FULL_NAME);

        return new KsqlAvroSerdeFactory(schemaFullName);

      case JSON:
        return new KsqlJsonSerdeFactory();

      case DELIMITED:
        return new KsqlDelimitedSerdeFactory();

      case KAFKA:
        return new KafkaSerdeFactory();

      default:
        throw new KsqlException(
            String.format("Unsupported format: %s", format.getFormat()));
    }
  }
}
