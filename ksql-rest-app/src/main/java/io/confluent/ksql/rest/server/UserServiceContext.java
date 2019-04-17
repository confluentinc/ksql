/*
 * Copyright 2019 Confluent Inc.
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

package io.confluent.ksql.rest.server;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.services.DefaultServiceContext;
import io.confluent.ksql.services.KafkaTopicClient;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.util.KsqlConfig;
import java.util.function.Supplier;
import javax.ws.rs.core.SecurityContext;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.streams.KafkaClientSupplier;


public final class UserServiceContext implements ServiceContext {
  public static UserServiceContext create(
      final SecurityContext securityContext,
      final KsqlConfig ksqlConfig
  ) {
    if (securityContext.getUserPrincipal() == null) {
      return new UserServiceContext(DefaultServiceContext.create(ksqlConfig));
    }

    // TODO: Create a UserServiceContext using user credentials
    return new UserServiceContext(DefaultServiceContext.create(ksqlConfig));
  }

  private final KafkaClientSupplier kafkaClientSupplier;
  private final AdminClient adminClient;
  private final KafkaTopicClient topicClient;
  private final Supplier<SchemaRegistryClient> srClientFactory;
  private final SchemaRegistryClient srClient;

  private UserServiceContext(final ServiceContext serviceContext) {
    this.kafkaClientSupplier = serviceContext.getKafkaClientSupplier();
    this.adminClient = serviceContext.getAdminClient();
    this.topicClient = serviceContext.getTopicClient();
    this.srClientFactory = serviceContext.getSchemaRegistryClientFactory();
    this.srClient = serviceContext.getSchemaRegistryClient();
  }

  @Override
  public AdminClient getAdminClient() {
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
  public void close() {
    adminClient.close();
  }
}
