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


import static io.confluent.ksql.serde.DataSource.DataSourceSerDe.JSON;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItems;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.KsqlContextTestUtil;
import io.confluent.ksql.test.util.ConsumerGroupTestUtil;
import io.confluent.ksql.test.util.TopicTestUtil;
import io.confluent.ksql.util.KafkaConsumerGroupClient;
import io.confluent.ksql.util.KafkaConsumerGroupClient.ConsumerSummary;
import io.confluent.ksql.util.KafkaConsumerGroupClientImpl;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.OrderDataProvider;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.test.IntegrationTest;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Unfortunately needs to be an integration test as there is no way
 * of stubbing results from the admin client as constructors are package-private. Mocking
 * the results would be tedious and distract from the actual testing.
 */
@Category({IntegrationTest.class})
public class KafkaConsumerGroupClientTest {

  private static final int PARTITION_COUNT = 3;

  @Rule
  public final IntegrationTestHarness testHarness = IntegrationTestHarness.build();

  private AdminClient adminClient;
  private KafkaConsumerGroupClient consumerGroupClient;
  private String topicName;
  private String group0;
  private String group1;

  @Before
  public void startUp() {
    final KsqlConfig ksqlConfig = KsqlContextTestUtil.createKsqlConfig(testHarness.getKafkaCluster());

    adminClient = AdminClient.create(ksqlConfig.getKsqlAdminClientConfigProps());
    consumerGroupClient = new KafkaConsumerGroupClientImpl(adminClient);

    topicName = TopicTestUtil.uniqueTopicName();

    group0 = ConsumerGroupTestUtil.uniqueGroupId("0");
    group1 = ConsumerGroupTestUtil.uniqueGroupId("1");
  }

  @After
  public void shutdown() {
    adminClient.close();
  }

  @Test
  public void shouldListNoConsumerGroupsWhenThereAreNone() {
    assertThat(consumerGroupClient.listGroups(), equalTo(Collections.<String>emptyList()));
  }

  @Test
  public void shouldListConsumerGroupsWhenTheyExist() throws InterruptedException {
    givenTopicExistsWithData();
    verifyListsGroups(group0, ImmutableList.of(group0));
    verifyListsGroups(group1, ImmutableList.of(group0, group1));
  }

  @Test
  public void shouldDescribeGroup() throws InterruptedException {
    givenTopicExistsWithData();
    try (final KafkaConsumer<String, byte[]> c1 = createConsumer(group0)) {
      verifyDescribeGroup(1, group0, Collections.singletonList(c1));
      try (final KafkaConsumer<String, byte[]> c2 = createConsumer(group0)) {
        verifyDescribeGroup(2, group0, Arrays.asList(c1, c2));
      }
    }
  }

  private void verifyDescribeGroup(
      final int expectedConsumers,
      final String group,
      final List<KafkaConsumer<String, byte[]>> consumers)
      throws InterruptedException {
    final List<ConsumerSummary> summaries = new ArrayList<>();
    TestUtils.waitForCondition(() -> {
      consumers.forEach(consumer -> consumer.poll(Duration.ofMillis(1)));
      summaries.clear();
      summaries.addAll(consumerGroupClient.describeConsumerGroup(group).consumers());
      return summaries.size() == expectedConsumers
          && summaries.stream().mapToLong(summary -> summary.partitions().size()).sum()
          == PARTITION_COUNT;
    }, 30000, "didn't receive expected number of consumers and/or partitions for group");

  }

  private void verifyListsGroups(final String newGroup, final List<String> consumerGroups)
      throws InterruptedException {

    try(final KafkaConsumer<String, byte[]> consumer = createConsumer(newGroup)) {
      final List<String> actual = waitForGroups(consumerGroups.size(), consumer);
      assertThat(actual, hasItems(consumerGroups.toArray(new String[0])));
    }
  }

  private List<String> waitForGroups(
      final int expectedNumberOfGroups,
      final KafkaConsumer<String, byte[]> consumer
  ) throws InterruptedException {

    TestUtils.waitForCondition(
        () -> {
          consumer.poll(Duration.ofMillis(1));
          final List<String> strings = consumerGroupClient.listGroups();
          return strings.size() == expectedNumberOfGroups;
        },
        "didn't receive exepected number of groups: " + expectedNumberOfGroups);
    return consumerGroupClient.listGroups();
  }

  private void givenTopicExistsWithData() {
    testHarness.ensureTopics(PARTITION_COUNT, topicName);
    testHarness.produceRows(topicName, new OrderDataProvider(), JSON, System::currentTimeMillis);
  }

  private KafkaConsumer<String, byte[]> createConsumer(final String group) {
    final Map<String, Object> consumerConfigs = testHarness.consumerConfig();
    consumerConfigs.put(ConsumerConfig.GROUP_ID_CONFIG, group);

    final KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(
        consumerConfigs,
        new StringDeserializer(),
        new ByteArrayDeserializer());

    consumer.subscribe(Collections.singleton(topicName));
    return consumer;
  }
}
