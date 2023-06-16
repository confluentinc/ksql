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

package io.confluent.ksql.services;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.schema.registry.KsqlSchemaRegistryClientFactory;
import io.confluent.ksql.util.KsqlConfig;
import java.util.Collections;
import java.util.Optional;
import java.util.function.Supplier;
import org.apache.kafka.streams.KafkaClientSupplier;
import org.apache.kafka.streams.processor.internals.DefaultKafkaClientSupplier;

public final class ServiceContextFactory {

  private ServiceContextFactory() {

  }

  public static ServiceContext create(
      final KsqlConfig ksqlConfig,
      final Supplier<SimpleKsqlClient> ksqlClientSupplier
  ) {
    return create(
        ksqlConfig,
        new DefaultKafkaClientSupplier(),
        new KsqlSchemaRegistryClientFactory(
            ksqlConfig,
            Collections.emptyMap())::get,
        () -> new DefaultConnectClientFactory(ksqlConfig).get(
            Optional.empty(),
            Collections.emptyList(),
            Optional.empty()),
        ksqlClientSupplier
    );
  }

  public static ServiceContext create(
      final KsqlConfig ksqlConfig,
      final KafkaClientSupplier kafkaClientSupplier,
      final Supplier<SchemaRegistryClient> srClientFactory,
      final Supplier<ConnectClient> connectClientSupplier,
      final Supplier<SimpleKsqlClient> ksqlClientSupplier
  ) {

    return new DefaultServiceContext(
        kafkaClientSupplier,
        () -> kafkaClientSupplier
            .getAdmin(ksqlConfig.getKsqlAdminClientConfigProps()),
        srClientFactory,
        connectClientSupplier,
        ksqlClientSupplier
    );
  }
}
