/*
 * Copyright 2017 Confluent Inc.
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
 **/

package io.confluent.ksql.util;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AlterConfigsResult;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.DisconnectException;
import org.apache.kafka.common.errors.NotControllerException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.utils.Utils;
import org.easymock.EasyMock;
import org.easymock.EasyMockRunner;
import org.easymock.IArgumentMatcher;
import org.easymock.Mock;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import io.confluent.ksql.exception.KafkaResponseGetFailedException;
import io.confluent.ksql.exception.KafkaTopicException;

import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.mock;
import static org.easymock.EasyMock.niceMock;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@RunWith(EasyMockRunner.class)
public class KafkaTopicClientImplTest {

  private final String topicName1 = "topic1";
  private final String topicName2 = "topic2";
  private final String topicName3 = "topic3";
  private final String internalTopic1 = String.format("%s%s_%s",
                                                      KsqlConstants.KSQL_INTERNAL_TOPIC_PREFIX,
                                                      "default",
                                                      "query_CTAS_USERS_BY_CITY-KSTREAM-AGGREGATE"
                                                      + "-STATE-STORE-0000000006-repartition");
  private final String internalTopic2 = String.format("%s%s_%s",
                                                      KsqlConstants.KSQL_INTERNAL_TOPIC_PREFIX,
                                                      "default",
                                                      "query_CTAS_USERS_BY_CITY-KSTREAM-AGGREGATE"
                                                      + "-STATE-STORE-0000000006-changelog");
  private final String confluentInternalTopic =
      String.format("%s-%s", KsqlConstants.CONFLUENT_INTERNAL_TOPIC_PREFIX,
                    "confluent-control-center");
  private Node node;
  @Mock
  private AdminClient adminClient;

  @Before
  public void init() {
    node = new Node(1, "host", 9092);
    expect(adminClient.describeCluster()).andReturn(describeClusterResult());
    expect(adminClient.describeConfigs(describeBrokerRequest()))
        .andReturn(describeBrokerResult());
  }

  @Test
  public void shouldCreateTopic() {
    expect(adminClient.listTopics()).andReturn(getListTopicsResult());
    expect(adminClient.createTopics(anyObject())).andReturn(getCreateTopicsResult());
    replay(adminClient);

    KafkaTopicClient kafkaTopicClient = new KafkaTopicClientImpl(adminClient);
    kafkaTopicClient.createTopic("test", 1, (short) 1);
    verify(adminClient);
  }

  @Test
  public void shouldUseExistingTopicWithTheSameSpecsInsteadOfCreate() {
    expect(adminClient.listTopics()).andReturn(getListTopicsResult());
    expect(adminClient.describeTopics(anyObject())).andReturn(getDescribeTopicsResult());
    replay(adminClient);

    KafkaTopicClient kafkaTopicClient = new KafkaTopicClientImpl(adminClient);
    kafkaTopicClient.createTopic(topicName1, 1, (short) 1);
    verify(adminClient);
  }

  @Test(expected = KafkaTopicException.class)
  public void shouldFailCreateExistingTopic() {
    expect(adminClient.createTopics(anyObject())).andReturn(getCreateTopicsResult());
    expect(adminClient.listTopics()).andReturn(getListTopicsResult());
    expect(adminClient.describeTopics(anyObject())).andReturn(getDescribeTopicsResult());
    replay(adminClient);
    KafkaTopicClient kafkaTopicClient = new KafkaTopicClientImpl(adminClient);
    kafkaTopicClient.createTopic(topicName1, 1, (short) 2);
    verify(adminClient);
  }

  @Test
  public void shouldNotFailIfTopicAlreadyExistsWhenCreating() {
    expect(adminClient.listTopics()).andReturn(getEmptyListTopicResult());
    expect(adminClient.createTopics(anyObject()))
        .andReturn(createTopicReturningTopicExistsException());
    expect(adminClient.describeTopics(anyObject())).andReturn(getDescribeTopicsResult());
    replay(adminClient);
    KafkaTopicClient kafkaTopicClient = new KafkaTopicClientImpl(adminClient);
    kafkaTopicClient.createTopic(topicName1, 1, (short) 1);
    verify(adminClient);
  }

  @Test
  public void shouldRetryDescribeTopicOnRetriableException() {
    expect(adminClient.listTopics()).andReturn(getEmptyListTopicResult());
    expect(adminClient.createTopics(anyObject()))
        .andReturn(createTopicReturningTopicExistsException());
    expect(adminClient.describeTopics(anyObject()))
        .andReturn(describeTopicReturningUnknownPartitionException()).once();
    // The second time, return the right response.
    expect(adminClient.describeTopics(anyObject())).andReturn(getDescribeTopicsResult()).once();
    replay(adminClient);
    KafkaTopicClient kafkaTopicClient = new KafkaTopicClientImpl(adminClient);
    kafkaTopicClient.createTopic(topicName1, 1, (short) 1);
    verify(adminClient);
  }

  @Test(expected = KafkaResponseGetFailedException.class)
  public void shouldFailToDescribeTopicsWhenRetriesExpire() {
    expect(adminClient.listTopics()).andReturn(getEmptyListTopicResult());
    expect(adminClient.describeTopics(anyObject()))
        .andReturn(describeTopicReturningUnknownPartitionException())
        .andReturn(describeTopicReturningUnknownPartitionException())
        .andReturn(describeTopicReturningUnknownPartitionException())
        .andReturn(describeTopicReturningUnknownPartitionException())
        .andReturn(describeTopicReturningUnknownPartitionException());
    replay(adminClient);
    KafkaTopicClient kafkaTopicClient = new KafkaTopicClientImpl(adminClient);
    kafkaTopicClient.describeTopics(Collections.singleton(topicName1));
    verify(adminClient);
  }

  @Test
  public void shouldRetryListTopics() {
    expect(adminClient.listTopics()).andReturn(listTopicResultWithNotControllerException()).once();
    expect(adminClient.listTopics()).andReturn(getListTopicsResult());
    replay(adminClient);
    KafkaTopicClient kafkaTopicClient = new KafkaTopicClientImpl(adminClient);
    Set<String> names = kafkaTopicClient.listTopicNames();
    assertThat(names, equalTo(Utils.mkSet(topicName1, topicName2, topicName3)));
    verify(adminClient);
  }

  @Test
  public void shouldFilterInternalTopics() {
    expect(adminClient.listTopics()).andReturn(getListTopicsResultWithInternalTopics());
    replay(adminClient);
    KafkaTopicClient kafkaTopicClient = new KafkaTopicClientImpl(adminClient);
    Set<String> names = kafkaTopicClient.listNonInternalTopicNames();
    assertThat(names, equalTo(Utils.mkSet(topicName1, topicName2, topicName3)));
    verify(adminClient);
  }

  @Test
  public void shouldListTopicNames() {
    expect(adminClient.listTopics()).andReturn(getListTopicsResult());
    replay(adminClient);
    KafkaTopicClient kafkaTopicClient = new KafkaTopicClientImpl(adminClient);
    Set<String> names = kafkaTopicClient.listTopicNames();
    assertThat(names, equalTo(Utils.mkSet(topicName1, topicName2, topicName3)));
    verify(adminClient);
  }

  @Test
  public void shouldDeleteTopics() {
    expect(adminClient.deleteTopics(anyObject())).andReturn(getDeleteTopicsResult());
    replay(adminClient);
    KafkaTopicClient kafkaTopicClient = new KafkaTopicClientImpl(adminClient);
    List<String> topics = Collections.singletonList(topicName2);
    kafkaTopicClient.deleteTopics(topics);
    verify(adminClient);
  }

  @Test
  public void shouldDeleteInternalTopics() {
    expect(adminClient.listTopics()).andReturn(getListTopicsResultWithInternalTopics());
    expect(adminClient.deleteTopics(Arrays.asList(internalTopic2, internalTopic1)))
        .andReturn(getDeleteInternalTopicsResult());
    replay(adminClient);
    KafkaTopicClient kafkaTopicClient = new KafkaTopicClientImpl(adminClient);
    String applicationId = String.format("%s%s",
                                         KsqlConstants.KSQL_INTERNAL_TOPIC_PREFIX,
                                         "default_query_CTAS_USERS_BY_CITY");
    kafkaTopicClient.deleteInternalTopics(applicationId);
    verify(adminClient);
  }

  @Test
  public void shouldGetTopicConfig() {
    expect(adminClient.describeConfigs(topicConfigsRequest("fred")))
        .andReturn(topicConfigResponse(
            "fred",
            overriddenConfigEntry(TopicConfig.RETENTION_MS_CONFIG, "12345"),
            defaultConfigEntry(TopicConfig.COMPRESSION_TYPE_CONFIG, "snappy")
        ));
    replay(adminClient);

    KafkaTopicClient kafkaTopicClient = new KafkaTopicClientImpl(adminClient);
    final Map<String, String> config = kafkaTopicClient.getTopicConfig("fred");

    assertThat(config.get(TopicConfig.RETENTION_MS_CONFIG), is("12345"));
    assertThat(config.get(TopicConfig.COMPRESSION_TYPE_CONFIG), is("snappy"));
  }

  @Test(expected = KafkaResponseGetFailedException.class)
  public void shouldThrowOnNoneRetriableGetTopicConfigError() {
    expect(adminClient.describeConfigs(anyObject()))
        .andReturn(topicConfigResponse(new RuntimeException()));
    replay(adminClient);

    KafkaTopicClient kafkaTopicClient = new KafkaTopicClientImpl(adminClient);
    final Map<String, String> config = kafkaTopicClient.getTopicConfig("fred");

    assertThat(config.get(TopicConfig.RETENTION_MS_CONFIG), is("12345"));
    assertThat(config.get(TopicConfig.COMPRESSION_TYPE_CONFIG), is("snappy"));
  }

  @Test
  public void shouldHandleRetriableGetTopicConfigError() {
    expect(adminClient.describeConfigs(anyObject()))
        .andReturn(topicConfigResponse(new DisconnectException()))
        .andReturn(topicConfigResponse(
            "fred",
            overriddenConfigEntry(TopicConfig.RETENTION_MS_CONFIG, "12345"),
            defaultConfigEntry(TopicConfig.COMPRESSION_TYPE_CONFIG, "producer")
        ));
    replay(adminClient);

    KafkaTopicClient kafkaTopicClient = new KafkaTopicClientImpl(adminClient);
    final Map<String, String> config = kafkaTopicClient.getTopicConfig("fred");

    assertThat(config.get(TopicConfig.RETENTION_MS_CONFIG), is("12345"));
    assertThat(config.get(TopicConfig.COMPRESSION_TYPE_CONFIG), is("producer"));
  }


  @Test
  public void shouldSetTopicCleanupPolicyToCompact() throws InterruptedException,
                                                            ExecutionException {
    expect(adminClient.listTopics()).andReturn(getEmptyListTopicResult());

    // Verify that the new topic configuration being passed to the admin client is what we expect.
    NewTopic newTopic = new NewTopic(topicName1, 1, (short) 1);
    newTopic.configs(Collections.singletonMap("cleanup.policy", "compact"));
    expect(adminClient.createTopics(singleNewTopic(newTopic))).andReturn(getCreateTopicsResult());
    replay(adminClient);

    KafkaTopicClient kafkaTopicClient = new KafkaTopicClientImpl(adminClient);
    kafkaTopicClient.createTopic(topicName1,
                                 1,
                                 (short) 1,
                                 Collections.singletonMap("cleanup.policy", "compact"));
    verify(adminClient);
  }

  @Test
  public void shouldAddTopicConfig() {
    final Map<String, ?> overrides = ImmutableMap.of(
        TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT
    );

    expect(adminClient.describeConfigs(topicConfigsRequest("peter")))
        .andReturn(topicConfigResponse(
            "peter",
            overriddenConfigEntry(TopicConfig.RETENTION_MS_CONFIG, "12345"),
            defaultConfigEntry(TopicConfig.COMPRESSION_TYPE_CONFIG, "snappy")
        ));

    expect(adminClient.alterConfigs(
        withResourceConfig(
            new ConfigResource(ConfigResource.Type.TOPIC, "peter"),
            new ConfigEntry(TopicConfig.RETENTION_MS_CONFIG, "12345"),
            new ConfigEntry(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT)
        )))
        .andReturn(alterTopicConfigResponse());
    replay(adminClient);

    KafkaTopicClient kafkaTopicClient = new KafkaTopicClientImpl(adminClient);
    kafkaTopicClient.addTopicConfig("peter", overrides);

    verify(adminClient);
  }

  @Test
  public void shouldNotAlterConfigIfConfigNotChanged() {
    final Map<String, ?> overrides = ImmutableMap.of(
        TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT
    );

    expect(adminClient.describeConfigs(topicConfigsRequest("peter")))
        .andReturn(topicConfigResponse(
            "peter",
            overriddenConfigEntry(TopicConfig.CLEANUP_POLICY_CONFIG,
                                  TopicConfig.CLEANUP_POLICY_COMPACT)
        ));

    replay(adminClient);

    KafkaTopicClient kafkaTopicClient = new KafkaTopicClientImpl(adminClient);
    kafkaTopicClient.addTopicConfig("peter", overrides);

    verify(adminClient);
  }

  @Test
  public void shouldRetryAddingTopicConfig() {
    final Map<String, ?> overrides = ImmutableMap.of(
        TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT
    );

    expect(adminClient.describeConfigs(anyObject()))
        .andReturn(topicConfigResponse(
            "peter",
            overriddenConfigEntry(TopicConfig.RETENTION_MS_CONFIG, "12345"),
            defaultConfigEntry(TopicConfig.COMPRESSION_TYPE_CONFIG, "snappy")
        ));

    expect(adminClient.alterConfigs(anyObject()))
        .andReturn(alterTopicConfigResponse(new DisconnectException()))
        .andReturn(alterTopicConfigResponse());
    replay(adminClient);

    KafkaTopicClient kafkaTopicClient = new KafkaTopicClientImpl(adminClient);
    kafkaTopicClient.addTopicConfig("peter", overrides);

    verify(adminClient);
  }

  @SuppressWarnings("unchecked")
  private static DescribeTopicsResult describeTopicReturningUnknownPartitionException() {
    DescribeTopicsResult describeTopicsResult = niceMock(DescribeTopicsResult.class);
    expect(describeTopicsResult.all())
        .andReturn(failedFuture(new UnknownTopicOrPartitionException("Topic doesn't exist")));
    replay(describeTopicsResult);
    return describeTopicsResult;
  }

  private DescribeTopicsResult getDescribeTopicsResult() {
    TopicPartitionInfo topicPartitionInfo = new TopicPartitionInfo(0, node, Collections
        .singletonList(node), Collections.singletonList(node));
    TopicDescription topicDescription = new TopicDescription(
        topicName1, false, Collections.singletonList(topicPartitionInfo));
    DescribeTopicsResult describeTopicsResult = mock(DescribeTopicsResult.class);
    expect(describeTopicsResult.all()).andReturn(
        KafkaFuture.completedFuture(Collections.singletonMap(topicName1, topicDescription)));
    replay(describeTopicsResult);
    return describeTopicsResult;
  }

  @SuppressWarnings("unchecked")
  private CreateTopicsResult createTopicReturningTopicExistsException() {
    CreateTopicsResult createTopicsResult = niceMock(CreateTopicsResult.class);
    expect(createTopicsResult.all())
        .andReturn(failedFuture(new TopicExistsException("Topic already exists")));
    replay(createTopicsResult);
    return createTopicsResult;
  }

  private CreateTopicsResult getCreateTopicsResult() {
    CreateTopicsResult createTopicsResult = mock(CreateTopicsResult.class);
    expect(createTopicsResult.all()).andReturn(KafkaFuture.allOf());
    replay(createTopicsResult);
    return createTopicsResult;
  }

  private DeleteTopicsResult getDeleteInternalTopicsResult() {
    DeleteTopicsResult deleteTopicsResult = mock(DeleteTopicsResult.class);
    Map<String, KafkaFuture<Void>> deletedTopics = new HashMap<>();
    deletedTopics.put(internalTopic1, KafkaFuture.allOf());
    deletedTopics.put(internalTopic2, KafkaFuture.allOf());
    expect(deleteTopicsResult.values()).andReturn(deletedTopics);
    replay(deleteTopicsResult);
    return deleteTopicsResult;
  }

  private DeleteTopicsResult getDeleteTopicsResult() {
    DeleteTopicsResult deleteTopicsResult = mock(DeleteTopicsResult.class);
    expect(deleteTopicsResult.values()).andReturn(Collections.singletonMap(topicName1, KafkaFuture
        .allOf()));
    replay(deleteTopicsResult);
    return deleteTopicsResult;
  }

  private ListTopicsResult getEmptyListTopicResult() {
    ListTopicsResult listTopicsResult = mock(ListTopicsResult.class);
    List<String> topicNamesList = Collections.emptyList();
    expect(listTopicsResult.names())
        .andReturn(KafkaFuture.completedFuture(new HashSet<>(topicNamesList)));
    replay(listTopicsResult);
    return listTopicsResult;
  }

  @SuppressWarnings("unchecked")
  private ListTopicsResult listTopicResultWithNotControllerException() {
    ListTopicsResult listTopicsResult = mock(ListTopicsResult.class);
    expect(listTopicsResult.names())
        .andReturn(failedFuture(new NotControllerException("Not Controller")));
    replay(listTopicsResult);
    return listTopicsResult;
  }

  private ListTopicsResult getListTopicsResultWithInternalTopics() {
    ListTopicsResult listTopicsResult = mock(ListTopicsResult.class);
    List<String> topicNamesList = Arrays.asList(topicName1, topicName2, topicName3,
                                                internalTopic1, internalTopic2,
                                                confluentInternalTopic);
    expect(listTopicsResult.names())
        .andReturn(KafkaFuture.completedFuture(new HashSet<>(topicNamesList)));
    replay(listTopicsResult);
    return listTopicsResult;
  }

  private ListTopicsResult getListTopicsResult() {
    ListTopicsResult listTopicsResult = mock(ListTopicsResult.class);
    List<String> topicNamesList = Arrays.asList(topicName1, topicName2, topicName3);
    expect(listTopicsResult.names())
        .andReturn(KafkaFuture.completedFuture(new HashSet<>(topicNamesList)));
    replay(listTopicsResult);
    return listTopicsResult;
  }

  private DescribeClusterResult describeClusterResult() {
    Collection<Node> nodes = Collections.singletonList(node);
    DescribeClusterResult describeClusterResult = mock(DescribeClusterResult.class);
    expect(describeClusterResult.nodes()).andReturn(KafkaFuture.completedFuture(nodes));
    replay(describeClusterResult);
    return describeClusterResult;
  }

  private Collection<ConfigResource> describeBrokerRequest() {
    return Collections.singleton(new ConfigResource(ConfigResource.Type.BROKER, node.idString()));
  }

  private DescribeConfigsResult describeBrokerResult() {
    DescribeConfigsResult describeConfigsResult = mock(DescribeConfigsResult.class);
    ConfigEntry configEntryDeleteEnable = new ConfigEntry("delete.topic.enable", "true");
    List<ConfigEntry> configEntries = new ArrayList<>();
    configEntries.add(configEntryDeleteEnable);
    Map<ConfigResource, Config> config = ImmutableMap.of(
        new ConfigResource(ConfigResource.Type.BROKER, node.idString()), new Config(configEntries));
    expect(describeConfigsResult.all()).andReturn(KafkaFuture.completedFuture(config));
    replay(describeConfigsResult);
    return describeConfigsResult;
  }

  private ConfigEntry defaultConfigEntry(final String key, final String value) {
    final ConfigEntry config = mock(ConfigEntry.class);
    expect(config.name()).andReturn(key);
    expect(config.value()).andReturn(value);
    expect(config.source()).andReturn(ConfigEntry.ConfigSource.DEFAULT_CONFIG);
    replay(config);
    return config;
  }

  private ConfigEntry overriddenConfigEntry(final String key, final String value) {
    final ConfigEntry config = mock(ConfigEntry.class);
    expect(config.name()).andReturn(key);
    expect(config.value()).andReturn(value);
    expect(config.source()).andReturn(ConfigEntry.ConfigSource.DYNAMIC_TOPIC_CONFIG);
    replay(config);
    return config;
  }

  private static Collection<ConfigResource> topicConfigsRequest(final String topicName) {
    return ImmutableList.of(
        new ConfigResource(ConfigResource.Type.TOPIC, topicName)
    );
  }

  private static DescribeConfigsResult topicConfigResponse(final String topicName,
                                                           final ConfigEntry... entries) {

    final Map<ConfigResource, Config> config = ImmutableMap.of(
        new ConfigResource(ConfigResource.Type.TOPIC, topicName),
        new Config(Arrays.asList(entries)));

    final DescribeConfigsResult response = mock(DescribeConfigsResult.class);
    expect(response.all()).andReturn(KafkaFuture.completedFuture(config));
    replay(response);
    return response;
  }


  private static DescribeConfigsResult topicConfigResponse(final Exception cause) {
    final DescribeConfigsResult response = mock(DescribeConfigsResult.class);
    expect(response.all()).andReturn(failedFuture(cause));
    replay(response);
    return response;
  }

  private static AlterConfigsResult alterTopicConfigResponse() {
    final AlterConfigsResult response = mock(AlterConfigsResult.class);
    expect(response.all()).andReturn(KafkaFuture.completedFuture(null));
    replay(response);
    return response;
  }

  private static AlterConfigsResult alterTopicConfigResponse(final Exception cause) {
    final AlterConfigsResult response = mock(AlterConfigsResult.class);
    expect(response.all()).andReturn(failedFuture(cause));
    replay(response);
    return response;
  }

  @SuppressWarnings("unchecked")
  private static <T> KafkaFuture<T> failedFuture(final Exception cause) {
    try {
      final KafkaFuture<T> future = mock(KafkaFuture.class);
      future.get();
      expectLastCall().andThrow(new ExecutionException(cause));
      replay(future);
      return future;
    } catch (final Exception e) {
      throw new RuntimeException(e);
    }
  }

  /*
   * Config has broken hashCode & equals method:
   * https://issues.apache.org/jira/browse/KAFKA-6727
   */
  private static Map<ConfigResource, Config> withResourceConfig(final ConfigResource resource,
                                                                final ConfigEntry... entries) {
    final Set<ConfigEntry> expected = Arrays.stream(entries)
        .collect(Collectors.toSet());

    class ConfigMatcher implements IArgumentMatcher {
      @SuppressWarnings("unchecked")
      @Override
      public boolean matches(final Object argument) {
        final Map<ConfigResource, Config> request = (Map<ConfigResource, Config>)argument;
        if (request.size() != 1) {
          return false;
        }

        final Config config = request.get(resource);
        if (config == null) {
          return false;
        }

        final Set<ConfigEntry> actual = new HashSet<>(config.entries());
        return actual.equals(expected);
      }

      @Override
      public void appendTo(final StringBuffer buffer) {
        buffer.append(resource).append("->")
            .append("Config{").append(expected).append("}");
      }
    }
    EasyMock.reportMatcher(new ConfigMatcher());
    return null;
  }

  private static Collection<NewTopic> singleNewTopic(final NewTopic expected) {
    class NewTopicsMatcher implements IArgumentMatcher {
      @SuppressWarnings("unchecked")
      @Override
      public boolean matches(final Object argument) {
        final Collection<NewTopic> newTopics = (Collection<NewTopic>) argument;
        if (newTopics.size() != 1) {
          return false;
        }

        final NewTopic actual = newTopics.iterator().next();
        return Objects.equals(actual.name(), expected.name())
               && Objects.equals(actual.replicationFactor(), expected.replicationFactor())
               && Objects.equals(actual.numPartitions(), expected.numPartitions())
               && Objects.equals(actual.configs(), expected.configs());
      }

      @Override
      public void appendTo(final StringBuffer buffer) {
        buffer.append("{NewTopic").append(expected).append("}");
      }
    }

    EasyMock.reportMatcher(new NewTopicsMatcher());
    return null;
  }
}
