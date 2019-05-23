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

package io.confluent.ksql.services;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import java.util.function.Supplier;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.streams.KafkaClientSupplier;

public class LazyServiceContext implements ServiceContext {
  private final Supplier<ServiceContext> serviceContextSupplier;
  private volatile ServiceContext serviceContext = null;

  public LazyServiceContext(final Supplier<ServiceContext> serviceContextSupplier) {
    this.serviceContextSupplier = serviceContextSupplier;
  }

  private ServiceContext getServiceContext() {
    if (this.serviceContext == null) {
      this.serviceContext = serviceContextSupplier.get();
    }
    return serviceContext;
  }

  @Override
  public AdminClient getAdminClient() {
    return getServiceContext().getAdminClient();
  }

  @Override
  public KafkaTopicClient getTopicClient() {
    return getServiceContext().getTopicClient();
  }

  @Override
  public KafkaClientSupplier getKafkaClientSupplier() {
    return getServiceContext().getKafkaClientSupplier();
  }

  @Override
  public SchemaRegistryClient getSchemaRegistryClient() {
    return getServiceContext().getSchemaRegistryClient();
  }

  @Override
  public Supplier<SchemaRegistryClient> getSchemaRegistryClientFactory() {
    return getServiceContext().getSchemaRegistryClientFactory();
  }

  @Override
  public void close() {
    getServiceContext().close();
  }
}
