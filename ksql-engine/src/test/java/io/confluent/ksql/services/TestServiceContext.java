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

package io.confluent.ksql.services;

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.util.FakeKafkaClientSupplier;
import io.confluent.ksql.util.FakeKafkaTopicClient;
import io.confluent.ksql.util.KafkaTopicClient;
import java.util.Collections;
import java.util.function.Supplier;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.streams.KafkaClientSupplier;

public final class TestServiceContext {

  private TestServiceContext() {
  }

  public static ServiceContext create() {
    return create(
        new FakeKafkaTopicClient()
    );
  }

  public static ServiceContext create(
      final KafkaTopicClient topicClient
  ) {
    return create(
        topicClient,
        MockSchemaRegistryClient::new
    );
  }

  public static ServiceContext create(
      final Supplier<SchemaRegistryClient> srClientFactory
  ) {
    return create(
        new FakeKafkaTopicClient(),
        srClientFactory
    );
  }

  public static ServiceContext create(
      final KafkaTopicClient topicClient,
      final Supplier<SchemaRegistryClient> srClientFactory
  ) {
    return create(
        new FakeKafkaClientSupplier(),
        new FakeKafkaClientSupplier().getAdminClient(Collections.emptyMap()),
        topicClient,
        srClientFactory
    );
  }

  public static ServiceContext create(
      final KafkaClientSupplier kafkaClientSupplier,
      final AdminClient adminClient,
      final KafkaTopicClient topicClient,
      final Supplier<SchemaRegistryClient> srClientFactory
  ) {
    return new ServiceContext(kafkaClientSupplier, adminClient, topicClient, srClientFactory);
  }
}