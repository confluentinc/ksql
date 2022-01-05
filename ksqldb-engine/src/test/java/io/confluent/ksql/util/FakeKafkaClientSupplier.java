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

package io.confluent.ksql.util;

import java.util.Collections;
import java.util.Map;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.MockAdminClient;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.Node;
import org.apache.kafka.streams.KafkaClientSupplier;

public class FakeKafkaClientSupplier implements KafkaClientSupplier {

  @Override
  public Admin getAdmin(final Map<String, Object> config) {
    final Node node = new Node(0, "localhost", 1234);
    return new MockAdminClient(Collections.singletonList(node), node);
  }

  @Override
  public Producer<byte[], byte[]> getProducer(final Map<String, Object> config) {
    return new MockProducer<>();
  }

  @Override
  public Consumer<byte[], byte[]> getConsumer(final Map<String, Object> config) {
    return new MockConsumer<>(OffsetResetStrategy.LATEST);
  }

  @Override
  public Consumer<byte[], byte[]> getRestoreConsumer(final Map<String, Object> config) {
    return new MockConsumer<>(OffsetResetStrategy.LATEST);
  }

  @Override
  public Consumer<byte[], byte[]> getGlobalConsumer(final Map<String, Object> config) {
    return new MockConsumer<>(OffsetResetStrategy.LATEST);
  }
}
