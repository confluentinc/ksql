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
import io.confluent.ksql.util.KsqlConfig;
import java.util.Objects;
import java.util.function.Supplier;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.streams.KafkaClientSupplier;

/**
 * A real service context, initialized from a {@link KsqlConfig} instance.
 */
public class DefaultServiceContext implements ServiceContext, AutoCloseable {

  private final KafkaClientSupplier kafkaClientSupplier;
  private final Admin adminClient;
  private final KafkaTopicClient topicClient;
  private final Supplier<SchemaRegistryClient> srClientFactory;
  private final SchemaRegistryClient srClient;
  private final ConnectClient connectClient;

  DefaultServiceContext(
      final KafkaClientSupplier kafkaClientSupplier,
      final Admin adminClient,
      final KafkaTopicClient topicClient,
      final Supplier<SchemaRegistryClient> srClientFactory,
      final ConnectClient connectClient
  ) {
    this.kafkaClientSupplier = Objects.requireNonNull(kafkaClientSupplier, "kafkaClientSupplier");
    this.adminClient = Objects.requireNonNull(adminClient, "adminClient");
    this.topicClient = Objects.requireNonNull(topicClient, "topicClient");
    this.srClientFactory = Objects.requireNonNull(srClientFactory, "srClientFactory");
    this.srClient = Objects.requireNonNull(srClientFactory.get(), "srClient");
    this.connectClient = Objects.requireNonNull(connectClient, "connectClient");
  }

  @Override
  public Admin getAdminClient() {
    return adminClient;
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
    return srClientFactory;
  }

  @Override
  public ConnectClient getConnectClient() {
    return connectClient;
  }

  @Override
  public void close() {
    adminClient.close();
  }
}
