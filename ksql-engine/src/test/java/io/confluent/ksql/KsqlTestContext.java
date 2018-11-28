/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.confluent.ksql;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.function.InternalFunctionRegistry;
import io.confluent.ksql.internal.KsqlEngineMetrics;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.MetaStoreImpl;
import io.confluent.ksql.util.KafkaTopicClient;
import io.confluent.ksql.util.KafkaTopicClientImpl;
import io.confluent.ksql.util.KsqlConfig;
import java.util.Objects;
import java.util.function.Supplier;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.streams.KafkaClientSupplier;
import org.apache.kafka.streams.processor.internals.DefaultKafkaClientSupplier;

public final class KsqlTestContext {

  private KsqlTestContext() {
  }

  public static KsqlContext create(
      final KsqlConfig ksqlConfig,
      final Supplier<SchemaRegistryClient> schemaRegistryClientFactory
  ) {
    Objects.requireNonNull(ksqlConfig, "ksqlConfig cannot be null.");
    Objects.requireNonNull(schemaRegistryClientFactory, "schemaRegistryClient cannot be null.");

    final KafkaClientSupplier clientSupplier = new DefaultKafkaClientSupplier();

    final AdminClient adminClient = clientSupplier
        .getAdminClient(ksqlConfig.getKsqlAdminClientConfigProps());

    final KafkaTopicClient kafkaTopicClient = new
        KafkaTopicClientImpl(adminClient);

    final MetaStore metaStore = new MetaStoreImpl(new InternalFunctionRegistry());

    final KsqlEngine engine = new KsqlEngine(
        kafkaTopicClient,
        schemaRegistryClientFactory,
        clientSupplier,
        metaStore,
        ksqlConfig.getString(KsqlConfig.KSQL_SERVICE_ID_CONFIG),
        adminClient,
        KsqlEngineMetrics::new
    );

    return new KsqlContext(ksqlConfig, engine);
  }
}