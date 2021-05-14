/*
 * Copyright 2021 Confluent Inc.
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

import com.google.common.collect.ImmutableList;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaProvider;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.json.JsonSchemaProvider;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaProvider;
import io.confluent.ksql.serde.Format;
import io.confluent.ksql.serde.FormatFactory;
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
    // the ksql datagen at the moment only supports AVRO, not JSON/PROTOBUF
    if (keyFormat != FormatFactory.AVRO && valueFormat != FormatFactory.AVRO) {
      return Optional.empty();
    }

    if (ksqlConfig.getString(KsqlConfig.SCHEMA_REGISTRY_URL_PROPERTY) == null) {
      throw new KsqlException("Schema registry url is not set.");
    }

    return Optional.of(new CachedSchemaRegistryClient(
        ImmutableList.of(ksqlConfig.getString(KsqlConfig.SCHEMA_REGISTRY_URL_PROPERTY)),
        100,
        ImmutableList.of(
            new AvroSchemaProvider(), new ProtobufSchemaProvider(), new JsonSchemaProvider()),
        ksqlConfig.originalsWithPrefix(KsqlConfig.KSQL_SCHEMA_REGISTRY_PREFIX)
    ));
  }

}
