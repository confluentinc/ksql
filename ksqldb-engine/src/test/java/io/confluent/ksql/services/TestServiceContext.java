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

import static io.confluent.ksql.util.KsqlConfig.CONNECT_REQUEST_TIMEOUT_DEFAULT;

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.util.FakeKafkaClientSupplier;
import io.confluent.ksql.util.KsqlConfig;
import java.util.Collections;
import java.util.Optional;
import java.util.function.Supplier;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.common.security.ssl.DefaultSslEngineFactory;
import org.apache.kafka.streams.KafkaClientSupplier;
import org.apache.kafka.streams.processor.internals.DefaultKafkaClientSupplier;

public final class TestServiceContext {

  private TestServiceContext() {
  }

  public static ServiceContext create() {
    return create(
        new FakeKafkaTopicClient(),
        new FakeKafkaConsumerGroupClient()
    );
  }

  public static ServiceContext create(
      final KafkaTopicClient topicClient,
      final Supplier<SchemaRegistryClient> srClientFactory
  ) {
    return create(
        topicClient,
        srClientFactory,
        new FakeKafkaConsumerGroupClient()
    );
  }

  public static ServiceContext create(
      final KafkaTopicClient topicClient
  ) {
    return create(
        topicClient,
        MockSchemaRegistryClient::new,
        new FakeKafkaConsumerGroupClient()
    );
  }

  public static ServiceContext create(
      final KafkaTopicClient topicClient,
      final KafkaConsumerGroupClient consumerGroupClient
  ) {
    return create(
        topicClient,
        MockSchemaRegistryClient::new,
        consumerGroupClient
    );
  }

  public static ServiceContext create(
      final Supplier<SchemaRegistryClient> srClientFactory
  ) {
    return create(
        new FakeKafkaTopicClient(),
        srClientFactory,
        new FakeKafkaConsumerGroupClient()
    );
  }

  public static ServiceContext create(
      final KafkaTopicClient topicClient,
      final Supplier<SchemaRegistryClient> srClientFactory,
      final KafkaConsumerGroupClient consumerGroupClient
  ) {
    return create(
        new FakeKafkaClientSupplier(),
        new FakeKafkaClientSupplier().getAdmin(Collections.emptyMap()),
        topicClient,
        srClientFactory,
        new DefaultConnectClient(
            "http://localhost:8083",
            Optional.empty(),
            Collections.emptyMap(),
            Optional.empty(),
            false,
            CONNECT_REQUEST_TIMEOUT_DEFAULT),
        consumerGroupClient
    );
  }

  public static ServiceContext create(
      final KsqlConfig ksqlConfig,
      final Supplier<SchemaRegistryClient> srClientFactory
  ) {
    final DefaultKafkaClientSupplier kafkaClientSupplier = new DefaultKafkaClientSupplier();
    final Admin adminClient = kafkaClientSupplier
        .getAdmin(ksqlConfig.getKsqlAdminClientConfigProps());

    return create(
        kafkaClientSupplier,
        adminClient,
        new KafkaTopicClientImpl(() -> adminClient),
        srClientFactory,
        new DefaultConnectClientFactory(ksqlConfig)
            .get(Optional.empty(), Collections.emptyList(), Optional.empty()),
        new KafkaConsumerGroupClientImpl(() -> adminClient)
    );
  }
  public static ServiceContext create(
      final KafkaClientSupplier kafkaClientSupplier,
      final Admin adminClient,
      final KafkaTopicClient topicClient,
      final Supplier<SchemaRegistryClient> srClientFactory,
      final ConnectClient connectClient
  ) {
    return create(
        kafkaClientSupplier,
        adminClient,
        topicClient,
        srClientFactory,
        connectClient,
        new FakeKafkaConsumerGroupClient());
  }

  public static ServiceContext create(
      final KafkaClientSupplier kafkaClientSupplier,
      final Admin adminClient,
      final KafkaTopicClient topicClient,
      final Supplier<SchemaRegistryClient> srClientFactory,
      final ConnectClient connectClient,
      final KafkaConsumerGroupClient consumerGroupClient
  ) {
    final DefaultServiceContext serviceContext = new DefaultServiceContext(
        kafkaClientSupplier,
        () -> adminClient,
        () -> adminClient,
        topicClient,
        srClientFactory,
        () -> connectClient,
        DisabledKsqlClient::instance,
        consumerGroupClient
    );

    // Ensure admin client is closed on serviceContext.close():
    serviceContext.getAdminClient();

    return serviceContext;
  }
}