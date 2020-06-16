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

import io.confluent.ksql.exception.KafkaResponseGetFailedException;
import io.confluent.ksql.util.ExecutorUtil.RetryBehaviour;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.ConsumerGroupListing;

public class KafkaConsumerGroupClientImpl implements KafkaConsumerGroupClient {

  private final Admin adminClient;

  public KafkaConsumerGroupClientImpl(final Admin adminClient) {
    this.adminClient = adminClient;
  }

  @Override
  public List<String> listGroups() {
    try {
      return ExecutorUtil.executeWithRetries(
          () -> adminClient.listConsumerGroups().all().get(),
          RetryBehaviour.ON_RETRYABLE)
          .stream()
          .map(ConsumerGroupListing::groupId).collect(Collectors.toList());
    } catch (final Exception e) {
      throw new KafkaResponseGetFailedException("Failed to retrieve Kafka consumer groups", e);
    }
  }

  public ConsumerGroupSummary describeConsumerGroup(final String group) {

    try {
      final Map<String, ConsumerGroupDescription> groups = ExecutorUtil
          .executeWithRetries(
              () -> adminClient.describeConsumerGroups(Collections.singleton(group)).all().get(),
              RetryBehaviour.ON_RETRYABLE);

      final Set<ConsumerSummary> results = groups
          .values()
          .stream()
          .flatMap(g ->
              g.members()
                  .stream()
                  .map(member -> {
                    final ConsumerSummary summary = new ConsumerSummary(member.consumerId());
                    summary.addPartitions(member.assignment().topicPartitions());
                    return summary;
                  })).collect(Collectors.toSet());

      return new ConsumerGroupSummary(results);

    } catch (final Exception e) {
      throw new KafkaResponseGetFailedException("Failed to describe Kafka consumer groups", e);
    }
  }
}
