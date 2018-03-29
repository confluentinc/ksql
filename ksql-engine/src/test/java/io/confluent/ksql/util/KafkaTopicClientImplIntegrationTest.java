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

package io.confluent.ksql.util;

import com.google.common.collect.ImmutableMap;

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

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import io.confluent.ksql.testutils.EmbeddedSingleNodeKafkaCluster;

import static io.confluent.ksql.testutils.AssertEventually.assertThatEventually;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

@Category({IntegrationTest.class})
public class KafkaTopicClientImplIntegrationTest {

  @ClassRule
  public static final EmbeddedSingleNodeKafkaCluster KAFKA =
      EmbeddedSingleNodeKafkaCluster.newBuilder().build();

  private String testTopic;
  private KafkaTopicClient client;
  private AdminClient adminClient;

  @Before
  public void setUp() {
    testTopic = UUID.randomUUID().toString();
    KAFKA.createTopic(testTopic);

    adminClient = AdminClient.create(ImmutableMap.of(
        AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.bootstrapServers()));

    client = new KafkaTopicClientImpl(adminClient);
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
  public void shouldCreateTopic() {
    // Given:
    final String topicName = UUID.randomUUID().toString();

    // When:
    client.createTopic(topicName, 3, (short) 1);

    // Then:
    assertThatEventually(() -> topicExists(topicName), is(true));
    final TopicDescription topicDescription = getTopicDescription(topicName);
    assertThat(topicDescription.partitions(), hasSize(3));
    assertThat(topicDescription.partitions().get(0).replicas(), hasSize(1));
  }

  @Test
  public void shouldCreateTopicWithConfig() {
    // Given:
    final String topicName = UUID.randomUUID().toString();
    final Map<String, String> config = ImmutableMap.of(
        TopicConfig.COMPRESSION_TYPE_CONFIG, "snappy");

    // When:
    client.createTopic(topicName, 2, (short) 1, config);

    // Then:
    assertThatEventually(() -> topicExists(topicName), is(true));
    final TopicDescription topicDescription = getTopicDescription(topicName);
    assertThat(topicDescription.partitions(), hasSize(2));
    assertThat(topicDescription.partitions().get(0).replicas(), hasSize(1));
    final Map<String, String> configs = client.getTopicConfig(topicName);
    assertThat(configs.get(TopicConfig.COMPRESSION_TYPE_CONFIG), is("snappy"));
  }

  private String getTopicConfig(final String configName) {
    final Map<String, String> configs = client.getTopicConfig(testTopic);
    return configs.get(configName);
  }

  private boolean topicExists(final String topicName) {
    return client.isTopicExists(topicName);
  }

  private TopicDescription getTopicDescription(final String topicName) {
    return client.describeTopics(Collections.singletonList(topicName)).get(topicName);
  }

}