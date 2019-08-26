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
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.streams.KafkaClientSupplier;
import org.apache.kafka.streams.processor.internals.DefaultKafkaClientSupplier;

public final class ServiceContextFactory {
  private ServiceContextFactory() {}

  public static ServiceContext create(final KsqlConfig ksqlConfig) {
    // Default to SERVER_CONTEXT if no kafka/SR clients are provided which means they will run
    // under the KSQL kafka/SR credentials found in the KsqlConfig
    return create(
        ServiceContext.ContextType.SERVER_CONTEXT,
        Optional.empty(),
        ksqlConfig,
        new DefaultKafkaClientSupplier(),
        new KsqlSchemaRegistryClientFactory(ksqlConfig, Collections.emptyMap())::get
    );
  }

  public static ServiceContext create(
      final ServiceContext.ContextType contextType,
      final Optional<String> userName,
      final KsqlConfig ksqlConfig,
      final KafkaClientSupplier kafkaClientSupplier,
      final Supplier<SchemaRegistryClient> srClientFactory
  ) {
    final Admin adminClient = kafkaClientSupplier.getAdmin(
        ksqlConfig.getKsqlAdminClientConfigProps()
    );

    return new DefaultServiceContext(
        contextType,
        userName,
        kafkaClientSupplier,
        adminClient,
        new KafkaTopicClientImpl(adminClient),
        srClientFactory,
        new DefaultConnectClient(ksqlConfig.getString(KsqlConfig.CONNECT_URL_PROPERTY))
    );
  }
}
