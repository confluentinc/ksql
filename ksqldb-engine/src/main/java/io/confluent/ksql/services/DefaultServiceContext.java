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

import static java.util.Objects.requireNonNull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Suppliers;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.util.KsqlConfig;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.streams.KafkaClientSupplier;

/**
 * A real service context, initialized from a {@link KsqlConfig} instance.
 */
public class DefaultServiceContext implements ServiceContext {

  private final KafkaClientSupplier kafkaClientSupplier;
  private final MemoizedSupplier<Admin> adminClientSupplier;
  private final MemoizedSupplier<Admin> topicAdminClientSupplier;
  private final MemoizedSupplier<KafkaTopicClient> topicClientSupplier;
  private final Supplier<SchemaRegistryClient> srClientFactorySupplier;
  private final MemoizedSupplier<SchemaRegistryClient> srClient;
  private final MemoizedSupplier<ConnectClient> connectClientSupplier;
  private final MemoizedSupplier<SimpleKsqlClient> ksqlClientSupplier;
  private final MemoizedSupplier<KafkaConsumerGroupClient> consumerGroupClientSupplier;

  public DefaultServiceContext(
      final KafkaClientSupplier kafkaClientSupplier,
      final Supplier<Admin> adminClientSupplier,
      final Supplier<Admin> topicAdminClientSupplier,
      final Supplier<SchemaRegistryClient> srClientSupplier,
      final Supplier<ConnectClient> connectClientSupplier,
      final Supplier<SimpleKsqlClient> ksqlClientSupplier
  ) {
    this(
        kafkaClientSupplier,
        adminClientSupplier,
        topicAdminClientSupplier,
        KafkaTopicClientImpl::new,
        srClientSupplier,
        connectClientSupplier,
        ksqlClientSupplier,
        KafkaConsumerGroupClientImpl::new
    );
  }

  @VisibleForTesting
  public DefaultServiceContext(
      final KafkaClientSupplier kafkaClientSupplier,
      final Supplier<Admin> adminClientSupplier,
      final Supplier<Admin> topicAdminClientSupplier,
      final KafkaTopicClient topicClient,
      final Supplier<SchemaRegistryClient> srClientSupplier,
      final Supplier<ConnectClient> connectClientSupplier,
      final Supplier<SimpleKsqlClient> ksqlClientSupplier,
      final KafkaConsumerGroupClient consumerGroupClient
  ) {
    this(
        kafkaClientSupplier,
        adminClientSupplier,
        topicAdminClientSupplier,
        adminSupplier -> topicClient,
        srClientSupplier,
        connectClientSupplier,
        ksqlClientSupplier,
        adminSupplier -> consumerGroupClient
    );
  }

  private DefaultServiceContext(
      final KafkaClientSupplier kafkaClientSupplier,
      final Supplier<Admin> adminClientSupplier,
      final Supplier<Admin> topicAdminClientSupplier,
      final Function<Supplier<Admin>, KafkaTopicClient> topicClientProvider,
      final Supplier<SchemaRegistryClient> srClientSupplier,
      final Supplier<ConnectClient> connectClientSupplier,
      final Supplier<SimpleKsqlClient> ksqlClientSupplier,
      final Function<Supplier<Admin>, KafkaConsumerGroupClient> consumerGroupClientProvider
  ) {
    requireNonNull(adminClientSupplier, "adminClientSupplier");
    this.adminClientSupplier = new MemoizedSupplier<>(adminClientSupplier);
    this.topicAdminClientSupplier = new MemoizedSupplier<>(topicAdminClientSupplier);

    this.srClientFactorySupplier = requireNonNull(srClientSupplier, "srClientSupplier");

    requireNonNull(connectClientSupplier, "connectClientSupplier");
    this.connectClientSupplier = new MemoizedSupplier<>(
        connectClientSupplier);

    requireNonNull(ksqlClientSupplier, "ksqlClientSupplier");
    this.ksqlClientSupplier = new MemoizedSupplier<>(ksqlClientSupplier);

    this.srClient = new MemoizedSupplier<>(srClientSupplier);

    this.kafkaClientSupplier = requireNonNull(kafkaClientSupplier, "kafkaClientSupplier");

    this.topicClientSupplier = new MemoizedSupplier<>(
        () -> topicClientProvider.apply(this.topicAdminClientSupplier));

    this.consumerGroupClientSupplier = new MemoizedSupplier<>(
        () -> consumerGroupClientProvider.apply(this.adminClientSupplier));
  }

  @Override
  public Admin getAdminClient() {
    return adminClientSupplier.get();
  }

  @Override
  public KafkaTopicClient getTopicClient() {
    return topicClientSupplier.get();
  }

  @Override
  public KafkaClientSupplier getKafkaClientSupplier() {
    return kafkaClientSupplier;
  }

  @Override
  public SchemaRegistryClient getSchemaRegistryClient() {
    return srClient.get();
  }

  @Override
  public Supplier<SchemaRegistryClient> getSchemaRegistryClientFactory() {
    return srClientFactorySupplier;
  }

  @Override
  public ConnectClient getConnectClient() {
    return connectClientSupplier.get();
  }

  @Override
  public SimpleKsqlClient getKsqlClient() {
    return ksqlClientSupplier.get();
  }

  @Override
  public KafkaConsumerGroupClient getConsumerGroupClient() {
    return consumerGroupClientSupplier.get();
  }

  @Override
  public void close() {
    if (adminClientSupplier.isInitialized()) {
      adminClientSupplier.get().close();
    }
    if (topicAdminClientSupplier.isInitialized()) {
      topicAdminClientSupplier.get().close();
    }
    if (ksqlClientSupplier.isInitialized()) {
      ksqlClientSupplier.get().close();
    }
  }

  static final class MemoizedSupplier<T> implements Supplier<T> {

    private final Supplier<T> supplier;
    private volatile boolean initialized = false;

    MemoizedSupplier(final Supplier<T> supplier) {
      this.supplier = Suppliers.memoize(supplier::get);
    }

    @Override
    public T get() {
      initialized = true;
      return supplier.get();
    }

    boolean isInitialized() {
      return initialized;
    }
  }
}
