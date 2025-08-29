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

import io.confluent.common.utils.IntegrationTest;
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
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;

@Category({IntegrationTest.class})
public class KafkaTopicClientImplIntegrationTest {

  private static final EmbeddedSingleNodeKafkaCluster KAFKA =
      EmbeddedSingleNodeKafkaCluster.build();

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

  @Test
  public void shouldSetTopicConfig() {
    // When:
    final boolean changed = client
        .addTopicConfig(testTopic, ImmutableMap.of(TopicConfig.RETENTION_MS_CONFIG, "1245678"));

    // Then:
    assertThat(changed, is(true));
    assertThatEventually(() -> getTopicConfig(TopicConfig.RETENTION_MS_CONFIG), is("1245678"));
  }

  @Test
  public void shouldNotSetTopicConfigWhenNothingChanged() {
    // Given:
    client.addTopicConfig(testTopic, ImmutableMap.of(TopicConfig.RETENTION_MS_CONFIG, "56784567"));

    // When:
    final boolean changed = client
        .addTopicConfig(testTopic, ImmutableMap.of(TopicConfig.RETENTION_MS_CONFIG, "56784567"));

    // Then:
    assertThat(changed, is(false));
  }

  @Test
  public void shouldNotRemovePreviousOverridesWhenAddingNew() {
    // Given:
    client
        .addTopicConfig(testTopic, ImmutableMap.of(TopicConfig.COMPRESSION_TYPE_CONFIG, "snappy"));

    // When:
    client.addTopicConfig(testTopic, ImmutableMap.of(TopicConfig.RETENTION_MS_CONFIG, "987654321"));

    // Then:
    assertThatEventually(() -> getTopicConfig(TopicConfig.RETENTION_MS_CONFIG), is("987654321"));
    assertThat(getTopicConfig(TopicConfig.COMPRESSION_TYPE_CONFIG), is("snappy"));
  }

  @Test
  public void shouldGetTopicCleanupPolicy() {
    // Given:
    client.addTopicConfig(testTopic, ImmutableMap.of(TopicConfig.CLEANUP_POLICY_CONFIG, "delete"));

    // Then:
    assertThatEventually(() -> client.getTopicCleanupPolicy(testTopic),
        is(KafkaTopicClient.TopicCleanupPolicy.DELETE));
  }

  @Test
  public void shouldListTopics() {
    // When:
    final Set<String> topicNames = client.listTopicNames();

    // Then:
    assertThat(topicNames, hasItem(testTopic));
  }

  @Test
  public void shouldDetectIfTopicExists() {
    assertThat(client.isTopicExists(testTopic), is(true));
    assertThat(client.isTopicExists("Unknown"), is(false));
  }

  @Test
  public void shouldDeleteTopics() {
    // When:
    client.deleteTopics(Collections.singletonList(testTopic));

    // Then:
    assertThat(client.isTopicExists(testTopic), is(false));
  }

  @Test
  public void shouldCreateTopicWithConfig() {
    // Given:
    final String topicName = UUID.randomUUID().toString();
    final Map<String, String> config = ImmutableMap.of(
        TopicConfig.COMPRESSION_TYPE_CONFIG, "snappy",
        TopicConfig.RETENTION_MS_CONFIG, "5000");

    // When:
    client.createTopic(topicName, 2, (short) 1, config);

    // Then:
    assertThatEventually(() -> topicExists(topicName), is(true));
    final TopicDescription topicDescription = getTopicDescription(topicName);
    assertThat(topicDescription.partitions(), hasSize(2));
    assertThat(topicDescription.partitions().get(0).replicas(), hasSize(1));
    final Map<String, String> configs = client.getTopicConfig(topicName);
    assertThat(configs.get(TopicConfig.COMPRESSION_TYPE_CONFIG), is("snappy"));
    assertThat(configs.get(TopicConfig.RETENTION_MS_CONFIG), is("5000"));
  }

  @Test
  public void shouldCreateTopicWithDefaultReplicationFactor() {
    // Given:
    final String topicName = UUID.randomUUID().toString();
    final Map<String, String> config = ImmutableMap.of(
        TopicConfig.RETENTION_MS_CONFIG, "5000");

    // When:
    client.createTopic(topicName, 2, TopicProperties.DEFAULT_REPLICAS, config);

    // Then:
    assertThatEventually(() -> topicExists(topicName), is(true));
    final TopicDescription topicDescription = getTopicDescription(topicName);
    assertThat(topicDescription.partitions(), hasSize(2));
    assertThat(topicDescription.partitions().get(0).replicas(), hasSize(1));
    final Map<String, String> configs = client.getTopicConfig(topicName);
    assertThat(configs.get(TopicConfig.RETENTION_MS_CONFIG), is("5000"));
  }

  @Test
  public void shouldThrowOnDescribeIfTopicDoesNotExist() {// Expect


// When:
    final Exception e = assertThrows(
        KafkaResponseGetFailedException.class,
        () -> client.describeTopic("i_do_not_exist")
    );

// Then:
    assertThat(e.getMessage(), containsString(
        "Failed to Describe Kafka Topic(s):"));
    assertThat(e.getMessage(), containsString(
        "i_do_not_exist"));
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