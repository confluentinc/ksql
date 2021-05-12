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

import static io.confluent.ksql.util.LimitedProxyBuilder.anyParams;
import static io.confluent.ksql.util.LimitedProxyBuilder.noParams;

import io.confluent.ksql.util.LimitedProxyBuilder;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;

/**
 * A limited consumer that can be used while trying out operations.
 *
 * <p>The client will not allow any operation that changes the state of the Kafka cluster.
 *
 * <p>Most operations result in a {@code UnsupportedOperationException} being thrown as they are
 * not called.
 */
final class SandboxedConsumer {

  static <K, V> Consumer<K, V> createProxy() {
    return LimitedProxyBuilder.forClass(Consumer.class)
        .swallow("close", anyParams())
        .swallow("wakeup", noParams())
        .swallow("unsubscribe", noParams())
        .swallow("groupMetadata", noParams(), new ConsumerGroupMetadata("group"))
        .build();
  }

  private SandboxedConsumer() {
  }
}
