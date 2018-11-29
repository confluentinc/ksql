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
import io.confluent.ksql.internal.KsqlEngineMetrics;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.util.KafkaTopicClient;
import io.confluent.ksql.util.KsqlConfig;
import java.util.function.Supplier;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.streams.KafkaClientSupplier;

public final class KsqlEngineTestUtil {
  private KsqlEngineTestUtil() {
  }

  public static KsqlEngine createKsqlEngine(
      final KafkaTopicClient topicClient,
      final Supplier<SchemaRegistryClient> schemaRegistryClientFactory,
      final KafkaClientSupplier clientSupplier,
      final MetaStore metaStore,
      final KsqlConfig initializationKsqlConfig,
      final AdminClient adminClient
  ) {
    return new KsqlEngine(
        topicClient,
        schemaRegistryClientFactory,
        clientSupplier,
        metaStore,
        initializationKsqlConfig.getString(KsqlConfig.KSQL_SERVICE_ID_CONFIG),
        adminClient,
        KsqlEngineMetrics::new
    );
  }

  public static KsqlEngine createKsqlEngine(
      final KafkaTopicClient topicClient,
      final Supplier<SchemaRegistryClient> schemaRegistryClientFactory,
      final KafkaClientSupplier clientSupplier,
      final MetaStore metaStore,
      final KsqlConfig initializationKsqlConfig,
      final AdminClient adminClient,
      final KsqlEngineMetrics engineMetrics
  ) {
    return new KsqlEngine(
        topicClient,
        schemaRegistryClientFactory,
        clientSupplier,
        metaStore,
        initializationKsqlConfig.getString(KsqlConfig.KSQL_SERVICE_ID_CONFIG),
        adminClient,
        ignored -> engineMetrics
    );
  }
}
