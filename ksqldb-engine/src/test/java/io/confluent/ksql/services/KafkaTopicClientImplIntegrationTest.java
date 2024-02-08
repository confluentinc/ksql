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

import static io.confluent.ksql.test.util.AssertEventually.assertThatEventually;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.exception.KafkaResponseGetFailedException;
import io.confluent.ksql.integration.Retry;
import io.confluent.ksql.test.util.EmbeddedSingleNodeKafkaCluster;
import io.confluent.ksql.topic.TopicProperties;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import kafka.zookeeper.ZooKeeperClientException;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.test.IntegrationTest;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;

@Category({IntegrationTest.class})
public class KafkaTopicClientImplIntegrationTest {

  private static final EmbeddedSingleNodeKafkaCluster KAFKA =
      EmbeddedSingleNodeKafkaCluster.build(true);

  @ClassRule
  public static final RuleChain CLUSTER_WITH_RETRY = RuleChain
      .outerRule(Retry.of(3, ZooKeeperClientException.class, 3, TimeUnit.SECONDS))
      .around(KAFKA);

  private String testTopic;
  private KafkaTopicClient client;
  private AdminClient adminClient;

  @Before
  public void setUp() {
    testTopic = UUID.randomUUID().toString();
    KAFKA.createTopics(testTopic);

    adminClient = AdminClient.create(ImmutableMap.of(
        AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.bootstrapServers()));

    client = new KafkaTopicClientImpl(() -> adminClient);

    allowForAsyncTopicCreation();
  }

  @After
  public void tearDown() {
    adminClient.close();
  }

  @Test
  public void shouldGetTopicConfig() {
    // When:
    final Map<String, String> config = client.getTopicConfig(testTopic);

    // Then:
    assertThat(config.keySet(), hasItems(
        TopicConfig.RETENTION_MS_CONFIG,
        TopicConfig.CLEANUP_POLICY_CONFIG,
        TopicConfig.COMPRESSION_TYPE_CONFIG));
  }

  private String getTopicConfig(final String configName) {
    final Map<String, String> configs = client.getTopicConfig(testTopic);
    return configs.get(configName);
  }

  private boolean topicExists(final String topicName) {
    return client.isTopicExists(topicName);
  }

  private TopicDescription getTopicDescription(final String topicName) {
    return client.describeTopic(topicName);
  }

  private void allowForAsyncTopicCreation() {
    final Supplier<Set<String>> topicNamesSupplier = () -> {
      try {
        return adminClient.listTopics().names().get();
      } catch (final Exception e) {
        return Collections.emptySet();
      }
    };

    assertThatEventually(topicNamesSupplier, hasItem(testTopic));
  }
}