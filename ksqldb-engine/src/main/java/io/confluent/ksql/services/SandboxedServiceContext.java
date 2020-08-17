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
import io.confluent.ksql.util.Sandbox;
import java.util.Objects;
import java.util.function.Supplier;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.streams.KafkaClientSupplier;

/**
 * A sandboxed service context to use when trying out operations.
 *
 * <p>The service clients within will not make changes to the external services they connect to.
 */
@Sandbox
public final class SandboxedServiceContext implements ServiceContext {

  private final KafkaTopicClient topicClient;
  private final SchemaRegistryClient srClient;
  private final KafkaClientSupplier kafkaClientSupplier;
  private final ConnectClient connectClient;
  private final KafkaConsumerGroupClient consumerGroupClient;

  public static SandboxedServiceContext create(final ServiceContext serviceContext) {
    if (serviceContext instanceof SandboxedServiceContext) {
      return (SandboxedServiceContext) serviceContext;
    }

    final KafkaClientSupplier kafkaClientSupplier = new SandboxedKafkaClientSupplier();
    final KafkaTopicClient kafkaTopicClient = SandboxedKafkaTopicClient
        .createProxy(serviceContext.getTopicClient());
    final SchemaRegistryClient schemaRegistryClient =
        SandboxedSchemaRegistryClient.createProxy(serviceContext.getSchemaRegistryClient());
    final ConnectClient connectClient = SandboxConnectClient.createProxy();
    final KafkaConsumerGroupClient kafkaConsumerGroupClient = SandboxedKafkaConsumerGroupClient
        .createProxy(serviceContext.getConsumerGroupClient());

    return new SandboxedServiceContext(
        kafkaClientSupplier,
        kafkaTopicClient,
        schemaRegistryClient,
        connectClient,
        kafkaConsumerGroupClient);
  }

  private SandboxedServiceContext(
      final KafkaClientSupplier kafkaClientSupplier,
      final KafkaTopicClient topicClient,
      final SchemaRegistryClient srClient,
      final ConnectClient connectClient,
      final KafkaConsumerGroupClient consumerGroupClient
  ) {
    this.kafkaClientSupplier = Objects.requireNonNull(kafkaClientSupplier, "kafkaClientSupplier");
    this.topicClient = Objects.requireNonNull(topicClient, "topicClient");
    this.srClient = Objects.requireNonNull(srClient, "srClient");
    this.connectClient = Objects.requireNonNull(connectClient, "connectClient");
    this.consumerGroupClient = Objects.requireNonNull(consumerGroupClient, "consumerGroupClient");
  }

  @Override
  public Admin getAdminClient() {
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
  public ConnectClient getConnectClient() {
    return connectClient;
  }

  @Override
  public SimpleKsqlClient getKsqlClient() {
    throw new UnsupportedOperationException();
  }

  @Override
  public KafkaConsumerGroupClient getConsumerGroupClient() {
    return consumerGroupClient;
  }

  @Override
  public void close() {
    // No op.
  }
}
