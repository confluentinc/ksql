/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.confluent.ksql.integration;


import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Streams;
import io.confluent.ksql.util.KafkaConsumerGroupClient;
import io.confluent.ksql.util.KafkaConsumerGroupClient.ConsumerSummary;
import io.confluent.ksql.util.KafkaConsumerGroupClientImpl;
import io.confluent.ksql.util.OrderDataProvider;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.test.IntegrationTest;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Unfortunately needs to be an integration test as there is no way
 * of stubbing results from the admin client as constructors are package-private. Mocking
 * the results would be tedious and distract from the actual testing.
 */
@Category({IntegrationTest.class})
public class KafkaConsumerGroupClientTest {

  private IntegrationTestHarness testHarness;
  private AdminClient adminClient;
  private KafkaConsumerGroupClient consumerGroupClient;


  @Before
  public void startUp() throws Exception {
    testHarness = new IntegrationTestHarness();
    testHarness.start(ImmutableMap
        .of(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"));
    adminClient = AdminClient.create(testHarness.ksqlConfig.getKsqlAdminClientConfigProps());
    consumerGroupClient = new KafkaConsumerGroupClientImpl(adminClient);
  }

  @After
  public void shutdown() {
    adminClient.close();
    testHarness.stop();
  }

  @Test
  public void shouldListNoConsumerGroupsWhenThereAreNone() {
    assertThat(consumerGroupClient.listGroups(), equalTo(Collections.<String>emptyList()));
  }

  @Test
  public void shouldListConsumerGroupsWhenTheyExist() throws InterruptedException {
    createTopic();
    final List<String> consumerGroups = new ArrayList<>();
    verifyListsGroups("group1", consumerGroups);
    verifyListsGroups("group2", consumerGroups);
  }

  @Test
  public void shouldDescribeGroup() throws InterruptedException {
    createTopic();
    final String group = "theGroup";
    try (final KafkaConsumer<String, byte[]> c1 = createConsumer(group)) {
      verifyDescribeGroup(1, group, Collections.singletonList(c1));
      try (final KafkaConsumer<String, byte[]> c2 = createConsumer(group)) {
       verifyDescribeGroup(2, group, Arrays.asList(c1, c2));
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
          && summaries.stream().mapToLong(summary -> summary.partitions().size()).sum() == 3;
    }, 30000, "didn't receive expected number of consumers and/or partitions for group");

  }

  private void verifyListsGroups(final String newGroup, final List<String> consumerGroups)
      throws InterruptedException {
    consumerGroups.add(newGroup);
    try(final KafkaConsumer<String, byte[]> consumer = createConsumer(newGroup)) {
      final List<String> actual = waitForGroups(consumerGroups.size(), consumer);
      assertThat(actual, containsInAnyOrder(consumerGroups.toArray()));
    }
  }

  private List<String> waitForGroups(final int expectedNumberOfGroups,
      final KafkaConsumer<String, byte[]> consumer) throws InterruptedException {
    TestUtils.waitForCondition(
        () -> {
          consumer.poll(Duration.ofMillis(1));
          return consumerGroupClient.listGroups().size() == expectedNumberOfGroups;
        },
        "didn't receive exepected number of groups: " + expectedNumberOfGroups);
    return consumerGroupClient.listGroups();
  }

  private void createTopic() {
    testHarness.createTopic("test", 3, (short) 1);
    testHarness.publishTestData("test", new OrderDataProvider(), System.currentTimeMillis());
  }

  private KafkaConsumer<String, byte[]> createConsumer(final String group) {
    return testHarness.createSubscribedConsumer("test", group);
  }


}
