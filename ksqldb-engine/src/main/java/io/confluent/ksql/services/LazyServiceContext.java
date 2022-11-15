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

import com.google.common.base.Suppliers;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import java.util.function.Supplier;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.streams.KafkaClientSupplier;

public class LazyServiceContext implements ServiceContext {
  private final Supplier<ServiceContext> serviceContextSupplier;

  public LazyServiceContext(final Supplier<ServiceContext> serviceContextSupplier) {
    this.serviceContextSupplier = Suppliers.memoize(serviceContextSupplier::get)::get;
  }

  @Override
  public Admin getAdminClient() {
    return serviceContextSupplier.get().getAdminClient();
  }

  @Override
  public KafkaTopicClient getTopicClient() {
    return serviceContextSupplier.get().getTopicClient();
  }

  @Override
  public KafkaClientSupplier getKafkaClientSupplier() {
    return serviceContextSupplier.get().getKafkaClientSupplier();
  }

  @Override
  public SchemaRegistryClient getSchemaRegistryClient() {
    return serviceContextSupplier.get().getSchemaRegistryClient();
  }

  @Override
  public Supplier<SchemaRegistryClient> getSchemaRegistryClientFactory() {
    return serviceContextSupplier.get().getSchemaRegistryClientFactory();
  }

  @Override
  public ConnectClient getConnectClient() {
    return serviceContextSupplier.get().getConnectClient();
  }

  @Override
  public SimpleKsqlClient getKsqlClient() {
    return serviceContextSupplier.get().getKsqlClient();
  }

  @Override
  public KafkaConsumerGroupClient getConsumerGroupClient() {
    return serviceContextSupplier.get().getConsumerGroupClient();
  }

  @Override
  public void close() {
    serviceContextSupplier.get().close();
  }
}
