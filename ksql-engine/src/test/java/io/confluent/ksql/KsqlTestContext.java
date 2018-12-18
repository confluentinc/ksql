/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.services.TestServiceContext;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.util.KafkaTopicClient;
import io.confluent.ksql.util.KafkaTopicClientImpl;
import io.confluent.ksql.util.KsqlConfig;
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
    final KafkaClientSupplier clientSupplier = new DefaultKafkaClientSupplier();

    final AdminClient adminClient = clientSupplier
        .getAdminClient(ksqlConfig.getKsqlAdminClientConfigProps());

    final KafkaTopicClient kafkaTopicClient = new KafkaTopicClientImpl(adminClient);

    final ServiceContext serviceContext = TestServiceContext.create(
        clientSupplier,
        adminClient,
        kafkaTopicClient,
        schemaRegistryClientFactory
    );

    final KsqlEngine engine = new KsqlEngine(
        serviceContext,
        ksqlConfig.getString(KsqlConfig.KSQL_SERVICE_ID_CONFIG)
    );

    return new KsqlContext(serviceContext, ksqlConfig, engine);
  }
}