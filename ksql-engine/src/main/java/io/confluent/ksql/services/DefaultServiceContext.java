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
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.util.KsqlConfig;
import java.util.function.Supplier;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.streams.KafkaClientSupplier;

/**
 * A real service context, initialized from a {@link KsqlConfig} instance.
 */
public class DefaultServiceContext implements ServiceContext {

  private final KafkaClientSupplier kafkaClientSupplier;
  private final Supplier<Admin> adminClientSupplier;
  private final Supplier<KafkaTopicClient>  topicClientSupplier;
  private final Supplier<SchemaRegistryClient> srClientSupplier;
  private final SchemaRegistryClient srClient;
  private final Supplier<ConnectClient> connectClientSupplier;
  private final Supplier<SimpleKsqlClient> ksqlClientSupplier;

  public DefaultServiceContext(
      final KafkaClientSupplier kafkaClientSupplier,
      final Supplier<Admin> adminClientSupplier,
      final Supplier<SchemaRegistryClient> srClientSupplier,
      final Supplier<ConnectClient> connectClientSupplier,
      final Supplier<SimpleKsqlClient> ksqlClientSupplier
  ) {

    requireNonNull(adminClientSupplier, "adminClientSupplier");
    this.adminClientSupplier = new NotThreadSafeMemoizedSupplier<Admin>(adminClientSupplier);

    requireNonNull(srClientSupplier, "srClientSupplier");
    this.srClientSupplier = new NotThreadSafeMemoizedSupplier<SchemaRegistryClient>(
        srClientSupplier);

    requireNonNull(connectClientSupplier, "connectClientSupplier");
    this.connectClientSupplier = new NotThreadSafeMemoizedSupplier<ConnectClient>(
        connectClientSupplier);

    requireNonNull(ksqlClientSupplier, "ksqlClientSupplier");
    this.ksqlClientSupplier = new NotThreadSafeMemoizedSupplier<SimpleKsqlClient>(
        ksqlClientSupplier);

    this.srClient = requireNonNull(srClientSupplier.get(), "srClient");

    this.kafkaClientSupplier = requireNonNull(kafkaClientSupplier, "kafkaClientSupplier");

    this.topicClientSupplier = new NotThreadSafeMemoizedSupplier<KafkaTopicClient>(
        () -> new KafkaTopicClientImpl(this.adminClientSupplier));
  }

  @VisibleForTesting
  public DefaultServiceContext(
      final KafkaClientSupplier kafkaClientSupplier,
      final Supplier<Admin> adminClientSupplier,
      final KafkaTopicClient topicClient,
      final Supplier<SchemaRegistryClient> srClientSupplier,
      final Supplier<ConnectClient> connectClientSupplier,
      final Supplier<SimpleKsqlClient> ksqlClientSupplier
  ) {

    requireNonNull(adminClientSupplier, "adminClientSupplier");
    this.adminClientSupplier = new NotThreadSafeMemoizedSupplier<Admin>(adminClientSupplier);

    requireNonNull(srClientSupplier, "srClientSupplier");
    this.srClientSupplier = new NotThreadSafeMemoizedSupplier<SchemaRegistryClient>(
        srClientSupplier);

    requireNonNull(connectClientSupplier, "connectClientSupplier");
    this.connectClientSupplier = new NotThreadSafeMemoizedSupplier<ConnectClient>(
        connectClientSupplier);

    requireNonNull(ksqlClientSupplier, "ksqlClientSupplier");
    this.ksqlClientSupplier = new NotThreadSafeMemoizedSupplier<SimpleKsqlClient>(
        ksqlClientSupplier);

    this.srClient = requireNonNull(srClientSupplier.get(), "srClient");

    this.kafkaClientSupplier = requireNonNull(kafkaClientSupplier, "kafkaClientSupplier");

    requireNonNull(topicClient, "topicClient");
    this.topicClientSupplier = new NotThreadSafeMemoizedSupplier<KafkaTopicClient>(
        () -> topicClient);
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
    return srClientSupplier.get();
  }

  @Override
  public Supplier<SchemaRegistryClient> getSchemaRegistryClientFactory() {
    return srClientSupplier;
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
  public void close() {
    if (((NotThreadSafeMemoizedSupplier)adminClientSupplier).isInitialized()) {
      adminClientSupplier.get().close();
    }
  }

  public static final class NotThreadSafeMemoizedSupplier<T> implements Supplier<T> {

    private Supplier<T> supplier;
    private T value;

    public NotThreadSafeMemoizedSupplier(Supplier<T> supplierToMemoize) {
      this.supplier = supplierToMemoize;
      this.value = null;
    }

    @Override
    public T get() {
      if (value == null) {
        value = supplier.get();
      }
      return value;
    }

    public boolean isInitialized() {
      return value != null;
    }

    public Supplier<T> getSupplier() {
      return supplier;
    }

  }
}
