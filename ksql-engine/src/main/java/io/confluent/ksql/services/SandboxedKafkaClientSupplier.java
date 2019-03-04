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

import java.util.Map;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.streams.KafkaClientSupplier;

/**
 * A limited kafka client supplier that can be used while trying out operations.
 *
 * <p>The clients supplied will not allow any operation that changes the state of the Kafka cluster.
 *
 * <p>Most operations result in a {@code UnsupportedOperationException} being thrown as they are
 * not currently called.
 */
class SandboxedKafkaClientSupplier implements KafkaClientSupplier {

  SandboxedKafkaClientSupplier() {
  }

  @Override
  public AdminClient getAdminClient(final Map<String, Object> config) {
    return new SandboxedAdminClient();
  }

  @Override
  public Producer<byte[], byte[]> getProducer(final Map<String, Object> config) {
    return SandboxedProducer.createProxy();
  }

  @Override
  public Consumer<byte[], byte[]> getConsumer(final Map<String, Object> config) {
    return SandboxedConsumer.createProxy();
  }

  @Override
  public Consumer<byte[], byte[]> getRestoreConsumer(final Map<String, Object> config) {
    return SandboxedConsumer.createProxy();
  }

  @Override
  public Consumer<byte[], byte[]> getGlobalConsumer(final Map<String, Object> config) {
    throw new UnsupportedOperationException();
  }
}
