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

package io.confluent.ksql.services;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.schema.registry.KsqlSchemaRegistryClientFactory;
import io.confluent.ksql.util.KafkaTopicClient;
import io.confluent.ksql.util.KafkaTopicClientImpl;
import io.confluent.ksql.util.KsqlConfig;
import java.util.Objects;
import java.util.function.Supplier;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.streams.KafkaClientSupplier;
import org.apache.kafka.streams.processor.internals.DefaultKafkaClientSupplier;

/**
 * Provides access to clients required to communicate with remote services.
 */
public class ServiceContext implements AutoCloseable {

  private final KafkaClientSupplier kafkaClientSupplier;
  private final AdminClient adminClient;
  private final KafkaTopicClient topicClient;
  private final Supplier<SchemaRegistryClient> srClientFactory;
  private final SchemaRegistryClient srClient;

  public static ServiceContext create(final KsqlConfig ksqlConfig) {
    final DefaultKafkaClientSupplier kafkaClientSupplier = new DefaultKafkaClientSupplier();
    final AdminClient adminClient = kafkaClientSupplier
        .getAdminClient(ksqlConfig.getKsqlAdminClientConfigProps());

    return new ServiceContext(
        kafkaClientSupplier,
        adminClient,
        new KafkaTopicClientImpl(adminClient),
        new KsqlSchemaRegistryClientFactory(ksqlConfig)::get
    );
  }

  ServiceContext(
      final KafkaClientSupplier kafkaClientSupplier,
      final AdminClient adminClient,
      final KafkaTopicClient topicClient,
      final Supplier<SchemaRegistryClient> srClientFactory
  ) {
    this.kafkaClientSupplier = Objects.requireNonNull(kafkaClientSupplier, "kafkaClientSupplier");
    this.adminClient = Objects.requireNonNull(adminClient, "adminClient");
    this.topicClient = Objects.requireNonNull(topicClient, "topicClient");
    this.srClientFactory = Objects.requireNonNull(srClientFactory, "srClientFactory");
    this.srClient = Objects.requireNonNull(srClientFactory.get(), "srClient");
  }

  /**
   * Get the shared {@link AdminClient} instance.
   *
   * <p>The default implementation is thread-safe and can be shared across threads.
   *
   * <p>The caller must <i>not</i> close the returned shared instance.
   *
   * @return a shared {@link AdminClient} instance.
   */
  public AdminClient getAdminClient() {
    return adminClient;
  }

  /**
   * Get the shared {@link KafkaTopicClient} instance.
   *
   * <p>The default implementation is thread-safe and can be shared across threads.
   *
   * @return a shared {@link KafkaTopicClient} instance.
   */
  public KafkaTopicClient getTopicClient() {
    return topicClient;
  }

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
  public KafkaClientSupplier getKafkaClientSupplier() {
    return kafkaClientSupplier;
  }

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
  public SchemaRegistryClient getSchemaRegistryClient() {
    return srClient;
  }

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
  public Supplier<SchemaRegistryClient> getSchemaRegistryClientFactory() {
    return srClientFactory;
  }

  @Override
  public void close() {
    adminClient.close();
  }
}
