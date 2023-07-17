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

import io.confluent.ksql.exception.KafkaResponseGetFailedException;
import io.confluent.ksql.exception.KsqlGroupAuthorizationException;
import io.confluent.ksql.util.ExecutorUtil;
import io.confluent.ksql.util.ExecutorUtil.RetryBehaviour;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.errors.GroupAuthorizationException;
import org.apache.kafka.common.errors.GroupNotEmptyException;
import org.apache.kafka.common.errors.RetriableException;

public class KafkaConsumerGroupClientImpl implements KafkaConsumerGroupClient {

  private final Supplier<Admin> adminClient;

  public KafkaConsumerGroupClientImpl(final Supplier<Admin> adminClient) {
    this.adminClient = adminClient;
  }

  @Override
  public List<String> listGroups() {
    try {
      return ExecutorUtil.executeWithRetries(
          () -> adminClient.get().listConsumerGroups().all().get(),
          RetryBehaviour.ON_RETRYABLE)
          .stream()
          .map(ConsumerGroupListing::groupId).collect(Collectors.toList());
    } catch (final Exception e) {
      throw new KafkaResponseGetFailedException("Failed to retrieve Kafka consumer groups", e);
    }
  }

  public ConsumerGroupSummary describeConsumerGroup(final String group) {
    try {
      final Map<String, ConsumerGroupDescription> groupDescriptions = ExecutorUtil
          .executeWithRetries(
              () -> adminClient.get()
                  .describeConsumerGroups(Collections.singleton(group))
                  .all()
                  .get(),
              RetryBehaviour.ON_RETRYABLE);

      final Set<ConsumerSummary> results = groupDescriptions
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
    } catch (final GroupAuthorizationException e) {
      throw new KsqlGroupAuthorizationException(AclOperation.DESCRIBE, group);
    } catch (final Exception e) {
      throw new KafkaResponseGetFailedException(
          "Failed to describe Kafka consumer groups: " + group, e);
    }
  }

  @Override
  public Map<TopicPartition, OffsetAndMetadata> listConsumerGroupOffsets(final String group) {
    try {
      return ExecutorUtil.executeWithRetries(
          () -> adminClient.get()
              .listConsumerGroupOffsets(group)
              .partitionsToOffsetAndMetadata()
              .get(),
          RetryBehaviour.ON_RETRYABLE);
    } catch (final GroupAuthorizationException e) {
      throw new KsqlGroupAuthorizationException(AclOperation.DESCRIBE, group);
    } catch (final Exception e) {
      throw new KafkaResponseGetFailedException("Failed to list Kafka consumer groups offsets", e);
    }
  }

  @Override
  public void deleteConsumerGroups(final Set<String> groups) {
    try {
      // it takes heartbeat.interval.ms after a consumer is closed for the broker
      // to recognize that there are no more consumers in the consumer group - for
      // that reason, we retry after 3 seconds (the default heartbeat.interval.ms)
      // in the case that we get a GroupNotEmptyException
      ExecutorUtil.executeWithRetries(
          () -> adminClient.get().deleteConsumerGroups(groups).all().get(),
          e -> (e instanceof RetriableException)
              || (e instanceof GroupNotEmptyException),
          (retry) -> Duration.of(3L * retry, ChronoUnit.SECONDS),
          10
      );
    } catch (final Exception e) {
      throw new KafkaResponseGetFailedException("Failed to delete consumer groups: " + groups, e);
    }
  }
}
