/*
 * Copyright 2021 Confluent Inc.
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

import com.google.common.collect.ImmutableMap;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.streams.KafkaClientSupplier;

/**
 * {@link KafkaClientSupplier} implementation that injects extra configurations to the kafka
 * admin and consumer/producer clients.
 */
public class ConfiguredKafkaClientSupplier implements KafkaClientSupplier {
  private final KafkaClientSupplier defaultSupplier;
  private final ImmutableMap<String, Object> supplierProperties;

  public ConfiguredKafkaClientSupplier(
      final KafkaClientSupplier defaultSupplier,
      final Map<String, Object> supplierProperties
  ) {
    this.defaultSupplier = Objects.requireNonNull(defaultSupplier, "defaultSupplier");
    this.supplierProperties = ImmutableMap.copyOf(
        Objects.requireNonNull(supplierProperties, "supplierProperties")
    );
  }

  public Map<String, Object> injectSupplierProperties(final Map<String, Object> config) {
    final Map<String, Object> newConfig = new HashMap<>(config);
    newConfig.putAll(supplierProperties);
    return newConfig;
  }

  @Override
  public Admin getAdmin(final Map<String, Object> config) {
    return defaultSupplier.getAdmin(injectSupplierProperties(config));
  }

  @Override
  public Producer<byte[], byte[]> getProducer(final Map<String, Object> config) {
    return defaultSupplier.getProducer(injectSupplierProperties(config));
  }

  @Override
  public Consumer<byte[], byte[]> getConsumer(final Map<String, Object> config) {
    return defaultSupplier.getConsumer(injectSupplierProperties(config));
  }

  @Override
  public Consumer<byte[], byte[]> getRestoreConsumer(final Map<String, Object> config) {
    return defaultSupplier.getRestoreConsumer(injectSupplierProperties(config));
  }

  @Override
  public Consumer<byte[], byte[]> getGlobalConsumer(final Map<String, Object> config) {
    return defaultSupplier.getGlobalConsumer(injectSupplierProperties(config));
  }
}
