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

package io.confluent.ksql.integration;

import static io.confluent.ksql.serde.FormatFactory.JSON;
import static io.confluent.ksql.serde.FormatFactory.KAFKA;
import static io.confluent.ksql.test.util.AssertEventually.assertThatEventually;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.is;

import com.google.common.collect.ImmutableList;

import io.confluent.common.utils.IntegrationTest;
import io.confluent.ksql.KsqlConfigTestUtil;
import io.confluent.ksql.test.util.ConsumerGroupTestUtil;
import io.confluent.ksql.test.util.TopicTestUtil;
import io.confluent.ksql.services.KafkaConsumerGroupClient;
import io.confluent.ksql.services.KafkaConsumerGroupClient.ConsumerSummary;
import io.confluent.ksql.services.KafkaConsumerGroupClientImpl;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.OrderDataProvider;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import kafka.zookeeper.ZooKeeperClientException;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;

/**
 * Unfortunately needs to be an integration test as there is no way of stubbing results from the
 * admin client as constructors are package-private. Mocking the results would be tedious and
 * distract from the actual testing.
 */
@Category({IntegrationTest.class})
public class KafkaConsumerGroupClientTest {

  private static final int PARTITION_COUNT = 3;

  private static final IntegrationTestHarness TEST_HARNESS = IntegrationTestHarness.build();

  @ClassRule
  public static final RuleChain clusterWithRetry = RuleChain
      .outerRule(Retry.of(3, ZooKeeperClientException.class, 3, TimeUnit.SECONDS))
      .around(TEST_HARNESS);

  private AdminClient adminClient;
  private KafkaConsumerGroupClient consumerGroupClient;
  private String topicName;
  private String group0;
  private String group1;

  @Before
  public void startUp() {
    final KsqlConfig ksqlConfig = KsqlConfigTestUtil.create(TEST_HARNESS.getKafkaCluster());

    adminClient = AdminClient.create(ksqlConfig.getKsqlAdminClientConfigProps());
    consumerGroupClient = new KafkaConsumerGroupClientImpl(() -> adminClient);

    topicName = TopicTestUtil.uniqueTopicName();

    group0 = ConsumerGroupTestUtil.uniqueGroupId("0");
    group1 = ConsumerGroupTestUtil.uniqueGroupId("1");
  }

  @After
  public void shutdown() {
    adminClient.close();
  }

  @Test
  public void shouldListConsumerGroupsWhenTheyExist() {
    givenTopicExistsWithData();
    verifyListsGroups(group0, ImmutableList.of(group0));
    verifyListsGroups(group1, ImmutableList.of(group0, group1));
  }

  @Test
  public void shouldDescribeConsumerGroup() {
    givenTopicExistsWithData();
    try (KafkaConsumer<String, byte[]> c1 = createConsumer(group0)) {
      verifyDescribeConsumerGroup(1, group0, ImmutableList.of(c1));
      try (KafkaConsumer<String, byte[]> c2 = createConsumer(group0)) {
        verifyDescribeConsumerGroup(2, group0, ImmutableList.of(c1, c2));
      }
    }
  }

  @Test
  public void shouldListConsumerGroupOffsetsWhenTheyExist() {
    givenTopicExistsWithData();
    verifyListsConsumerGroupOffsets(group0);
  }

  private void verifyDescribeConsumerGroup(
      final int expectedNumConsumers,
      final String group,
      final List<KafkaConsumer<?, ?>> consumers
  ) {
    final Supplier<ConsumerAndPartitionCount> pollAndGetCounts = () -> {
      consumers.forEach(consumer -> consumer.poll(Duration.ofMillis(1)));

      final Collection<ConsumerSummary> summaries = consumerGroupClient
          .describeConsumerGroup(group).consumers();

      final long partitionCount = summaries.stream()
          .mapToLong(summary -> summary.partitions().size())
          .sum();

      return new ConsumerAndPartitionCount(consumers.size(), (int) partitionCount);
    };

    assertThatEventually(pollAndGetCounts,
        is(new ConsumerAndPartitionCount(expectedNumConsumers, PARTITION_COUNT)));
  }

  private void verifyListsGroups(final String newGroup, final List<String> consumerGroups) {
    try (KafkaConsumer<String, byte[]> consumer = createConsumer(newGroup)) {

      final Supplier<List<String>> pollAndGetGroups = () -> {
        consumer.poll(Duration.ofMillis(1));
        return consumerGroupClient.listGroups();
      };

      assertThatEventually(pollAndGetGroups, hasItems(consumerGroups.toArray(new String[0])));
    }
  }

  private void verifyListsConsumerGroupOffsets(
      final String newGroup
  ) {
    try (KafkaConsumer<String, byte[]> consumer = createConsumer(newGroup)) {
      final Supplier<Map<TopicPartition, OffsetAndMetadata>> pollAndGetGroups = () -> {
        consumer.poll(Duration.ofMillis(1));
        return consumerGroupClient.listConsumerGroupOffsets(newGroup);
      };

      assertThatEventually(pollAndGetGroups,
          hasEntry(new TopicPartition(topicName, 0), new OffsetAndMetadata(0)));
    }
  }

  private void givenTopicExistsWithData() {
    TEST_HARNESS.ensureTopics(PARTITION_COUNT, topicName);
    TEST_HARNESS.produceRows(topicName, new OrderDataProvider(), KAFKA, JSON, System::currentTimeMillis);
  }

  private KafkaConsumer<String, byte[]> createConsumer(final String group) {
    final Map<String, Object> consumerConfigs = TEST_HARNESS.getKafkaCluster().consumerConfig();
    consumerConfigs.put(ConsumerConfig.GROUP_ID_CONFIG, group);

    final KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(
        consumerConfigs,
        new StringDeserializer(),
        new ByteArrayDeserializer());

    consumer.subscribe(Collections.singleton(topicName));
    return consumer;
  }

  private static final class ConsumerAndPartitionCount {

    private final int consumerCount;
    private final int partitionCount;

    private ConsumerAndPartitionCount(final int consumerCount, final int partitionCount) {
      this.consumerCount = consumerCount;
      this.partitionCount = partitionCount;
    }

    @Override
    public boolean equals(final Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      final ConsumerAndPartitionCount that = (ConsumerAndPartitionCount) o;
      return consumerCount == that.consumerCount
          && partitionCount == that.partitionCount;
    }

    @Override
    public int hashCode() {
      return Objects.hash(consumerCount, partitionCount);
    }
  }
}
