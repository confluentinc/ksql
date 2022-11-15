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
import java.util.function.Supplier;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.streams.KafkaClientSupplier;

/**
 * Provides access to clients required to communicate with remote services.
 */
public interface ServiceContext extends AutoCloseable {

  /**
   * Get the shared {@link Admin} instance.
   *
   * <p>The default implementation is thread-safe and can be shared across threads.
   *
   * <p>The caller must <i>not</i> close the returned shared instance.
   *
   * @return a shared {@link Admin} instance.
   */
  Admin getAdminClient();

  /**
   * Get the shared {@link KafkaTopicClient} instance.
   *
   * <p>The default implementation is thread-safe and can be shared across threads.
   *
   * @return a shared {@link KafkaTopicClient} instance.
   */
  KafkaTopicClient getTopicClient();

  /**
   * Get the {@link KafkaClientSupplier} instance.
   *
   * <p>The default implementation is thread-safe and can be shared across threads to create
   * the standard Kafka clients, some of which are not thread-safe.
   *
   * <p>The caller is responsible for closing any clients created by the factory.
   *
   * @return a {@link KafkaClientSupplier}.
   */
  KafkaClientSupplier getKafkaClientSupplier();

  /**
   * Get the shared {@link SchemaRegistryClient} instance.
   *
   * <p>The default implementation is thread-safe, but makes heavy use of synchronization.
   * As such, this shared instance should <i>not</i> be used for performance sensitive operations.
   *
   * <p>A factory method for instances is available for use during performance sensitive operations,
   * see {@link #getSchemaRegistryClientFactory()}
   *
   * @return a shared {@link SchemaRegistryClient}.
   */
  SchemaRegistryClient getSchemaRegistryClient();

  /**
   * Get a supplier of {@link SchemaRegistryClient} instance.
   *
   * <p>The default implementation of the supplier is thread-safe. The returned clients are also,
   * by default, thread-safe. However, they make heavy use of synchronization, and hence should not
   * be shared across threads for performance sensitive operations.
   *
   * <p>A shared instance is available for use during non performance sensitive operations,
   * see {@link #getSchemaRegistryClient()}
   *
   * @return a shared {@link SchemaRegistryClient}.
   */
  Supplier<SchemaRegistryClient> getSchemaRegistryClientFactory();

  /**
   * Get the shared {@link ConnectClient} instance.
   *
   * <p>The default implementation is thread-safe and can be shared across threads.
   *
   * @return a shared {@link ConnectClient}
   */
  ConnectClient getConnectClient();

  /**
   * Get a shared {@link SimpleKsqlClient} instance.
   *
   * <p>The returned instance is thread-safe.
   *
   * @return a shared {@link SimpleKsqlClient}.
   */
  SimpleKsqlClient getKsqlClient();

  KafkaConsumerGroupClient getConsumerGroupClient();

  @Override
  void close();
}
