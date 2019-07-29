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

import io.confluent.ksql.properties.with.CommonCreateConfigs;
import io.confluent.ksql.serde.avro.KsqlAvroSerdeFactory;
import io.confluent.ksql.serde.delimited.KsqlDelimitedSerdeFactory;
import io.confluent.ksql.serde.json.KsqlJsonSerdeFactory;
import io.confluent.ksql.serde.kafka.KafkaSerdeFactory;
import io.confluent.ksql.util.KsqlConstants;
import io.confluent.ksql.util.KsqlException;
import java.util.Optional;

public final class KsqlSerdeFactories implements SerdeFactories {

  @Override
  public KsqlSerdeFactory create(
      final Format format,
      final Optional<String> fullSchemaName
  ) {
    validateProps(format, fullSchemaName);

    switch (format) {
      case AVRO:
        final String schemaFullName = fullSchemaName
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
            String.format("Unsupported format: %s", format));
    }
  }

  private static void validateProps(final Format format, final Optional<String> avroSchema) {
    if (format != Format.AVRO && avroSchema.isPresent()) {
      throw new KsqlException(
          CommonCreateConfigs.VALUE_AVRO_SCHEMA_FULL_NAME + " is only valid for AVRO topics.");
    }
  }
}
