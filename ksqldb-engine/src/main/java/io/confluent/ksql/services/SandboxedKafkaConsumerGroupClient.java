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

import static io.confluent.ksql.util.LimitedProxyBuilder.methodParams;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.services.KafkaConsumerGroupClient.ConsumerGroupSummary;
import io.confluent.ksql.util.LimitedProxyBuilder;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

@SuppressFBWarnings("UPM_UNCALLED_PRIVATE_METHOD") // Methods invoked via reflection.
@SuppressWarnings("unused")  // Methods invoked via reflection.
public final class SandboxedKafkaConsumerGroupClient {

  static KafkaConsumerGroupClient createProxy(final KafkaConsumerGroupClient delegate) {
    final SandboxedKafkaConsumerGroupClient sandbox =
        new SandboxedKafkaConsumerGroupClient(delegate);

    return LimitedProxyBuilder.forClass(KafkaConsumerGroupClient.class)
        .forward("describeConsumerGroup", methodParams(String.class), sandbox)
        .forward("listGroups", methodParams(), sandbox)
        .forward("listConsumerGroupOffsets", methodParams(String.class), sandbox)
        .build();
  }

  private final KafkaConsumerGroupClient delegate;

  private SandboxedKafkaConsumerGroupClient(final KafkaConsumerGroupClient delegate) {
    this.delegate = Objects.requireNonNull(delegate, "delegate");
  }



  public ConsumerGroupSummary describeConsumerGroup(final String groupId) {
    return delegate.describeConsumerGroup(groupId);
  }

  public List<String> listGroups() {
    return delegate.listGroups();
  }

  public Map<TopicPartition, OffsetAndMetadata> listConsumerGroupOffsets(final String groupId) {
    return delegate.listConsumerGroupOffsets(groupId);
  }
}
