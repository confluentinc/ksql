/*
 * Copyright 2019 Confluent Inc.
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

package io.confluent.ksql.services;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.util.KafkaTopicClient;
import java.util.Objects;
import java.util.function.Supplier;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.streams.KafkaClientSupplier;

/**
 * A service context to use when trying out operations.
 *
 * <p>The service clients within will not make changes to the external services they connect to.
 */
public final class TryServiceContext implements ServiceContext {

  private final KafkaTopicClient topicClient;
  private final SchemaRegistryClient srClient;
  private final KafkaClientSupplier kafkaClientSupplier;

  public static TryServiceContext tryContext(final ServiceContext serviceContext) {
    if (serviceContext instanceof TryServiceContext) {
      return (TryServiceContext) serviceContext;
    }

    final KafkaClientSupplier kafkaClientSupplier = new TryKafkaClientSupplier();
    final KafkaTopicClient kafkaTopicClient = new TryKafkaTopicClient(
        serviceContext.getTopicClient());
    final SchemaRegistryClient schemaRegistryClient =
        new TrySchemaRegistryClient(serviceContext.getSchemaRegistryClient());

    return new TryServiceContext(
        kafkaClientSupplier,
        kafkaTopicClient,
        schemaRegistryClient);
  }

  private TryServiceContext(
      final KafkaClientSupplier kafkaClientSupplier,
      final KafkaTopicClient topicClient,
      final SchemaRegistryClient srClient
  ) {
    this.kafkaClientSupplier = Objects.requireNonNull(kafkaClientSupplier, "kafkaClientSupplier");
    this.topicClient = Objects.requireNonNull(topicClient, "topicClient");
    this.srClient = Objects.requireNonNull(srClient, "srClient");
  }

  @Override
  public AdminClient getAdminClient() {
    throw new UnsupportedOperationException();
  }

  @Override
  public KafkaTopicClient getTopicClient() {
    return topicClient;
  }

  @Override
  public KafkaClientSupplier getKafkaClientSupplier() {
    return kafkaClientSupplier;
  }

  @Override
  public SchemaRegistryClient getSchemaRegistryClient() {
    return srClient;
  }

  @Override
  public Supplier<SchemaRegistryClient> getSchemaRegistryClientFactory() {
    return () -> srClient;
  }

  @Override
  public void close() {
  }
}
