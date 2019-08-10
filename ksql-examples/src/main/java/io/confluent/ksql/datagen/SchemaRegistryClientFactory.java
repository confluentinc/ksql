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

package io.confluent.ksql.datagen;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.serde.Format;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import java.util.Optional;

final class SchemaRegistryClientFactory {

  private SchemaRegistryClientFactory() {
  }

  static Optional<SchemaRegistryClient> getSrClient(
      final Format keyFormat,
      final Format valueFormat,
      final KsqlConfig ksqlConfig
  ) {
    if (keyFormat != Format.AVRO && valueFormat != Format.AVRO) {
      return Optional.empty();
    }

    if (ksqlConfig.getString(KsqlConfig.SCHEMA_REGISTRY_URL_PROPERTY) == null) {
      throw new KsqlException("Schema registry url is not set.");
    }

    return Optional.of(new CachedSchemaRegistryClient(
        ksqlConfig.getString(KsqlConfig.SCHEMA_REGISTRY_URL_PROPERTY),
        100,
        ksqlConfig.originalsWithPrefix(KsqlConfig.KSQL_SCHEMA_REGISTRY_PREFIX)
    ));
  }

}
