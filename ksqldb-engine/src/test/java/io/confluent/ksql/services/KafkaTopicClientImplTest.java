/*
 * Copyright 2021 Confluent Inc.
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

import static org.apache.kafka.common.config.TopicConfig.CLEANUP_POLICY_COMPACT;
import static org.apache.kafka.common.config.TopicConfig.CLEANUP_POLICY_CONFIG;
import static org.apache.kafka.common.config.TopicConfig.CLEANUP_POLICY_DELETE;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.exception.KafkaDeleteTopicsException;
import io.confluent.ksql.exception.KafkaResponseGetFailedException;
import io.confluent.ksql.exception.KafkaTopicExistsException;
import io.confluent.ksql.exception.KsqlTopicAuthorizationException;
import io.confluent.ksql.services.KafkaTopicClient.TopicCleanupPolicy;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.AlterConfigsResult;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.CreateTopicsOptions;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.DisconnectException;
import org.apache.kafka.common.errors.NotControllerException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.errors.TopicDeletionDisabledException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

@RunWith(MockitoJUnitRunner.class)
public class KafkaTopicClientImplTest {

  private static final Node A_NODE = new Node(1, "host", 9092);

  @Mock
  private AdminClient adminClient;

  private final Map<String, List<TopicPartitionInfo>> topicPartitionInfo = new HashMap<>();
  private final Map<ConfigResource, Config> topicConfigs = new HashMap<>();

  private final Map<String, ?> configs = ImmutableMap.of(TopicConfig.RETENTION_MS_CONFIG, 8640000000L);

  private KafkaTopicClient kafkaTopicClient;

  @SuppressWarnings({"deprecation", "unchecked"})
  @Before
  public void setUp() {
    topicPartitionInfo.clear();
    topicConfigs.clear();

    when(adminClient.listTopics()).thenAnswer(listTopicResult());
    when(adminClient.describeTopics(anyCollection(), any())).thenAnswer(describeTopicsResult());
    when(adminClient.createTopics(any(), any())).thenAnswer(createTopicsResult());
    when(adminClient.deleteTopics(any(Collection.class))).thenAnswer(deleteTopicsResult());
    when(adminClient.describeConfigs(any())).thenAnswer(describeConfigsResult());
    when(adminClient.incrementalAlterConfigs(any())).thenAnswer(alterConfigsResult());
    when(adminClient.alterConfigs(any())).thenAnswer(alterConfigsResult());

    kafkaTopicClient = new KafkaTopicClientImpl(() -> adminClient);
  }

  @Test
  public void shouldCreateTopic() {
    // When:
    kafkaTopicClient.createTopic("someTopic", 1, (short) 2, configs);

    // Then:
    verify(adminClient).createTopics(
        eq(ImmutableSet.of(newTopic("someTopic", 1, 2, configs))),
        argThat(createOptions -> !createOptions.shouldValidateOnly())
    );
  }

  @Test
  public void shouldCreateTopicWithEmptyConfigs() {
    Map<String, ?> configs = ImmutableMap.of();
    // When:
    kafkaTopicClient.createTopic("someTopic", 1, (short) 2);

    // Then:
    verify(adminClient).createTopics(
        eq(ImmutableSet.of(newTopic("someTopic", 1, 2, configs))),
        argThat(createOptions -> !createOptions.shouldValidateOnly())
    );
  }

  @Test
  public void shouldNotCreateTopicIfItAlreadyExistsWithMatchingDetails() {
    // Given:
    givenTopicExists("someTopic", 3, 2);
    givenTopicConfigs(
        "someTopic",
        overriddenConfigEntry(TopicConfig.RETENTION_MS_CONFIG, "8640000000")
    );

    // When:
    kafkaTopicClient.createTopic("someTopic", 3, (short) 2, configs);

    // Then:
    verify(adminClient, never()).createTopics(any(), any());
  }

  @Test
  public void shouldNotCreateTopicIfItAlreadyExistsWithDefaultRf() {
    // Given:
    givenTopicExists("someTopic", 1, 2);
    givenTopicConfigs(
        "someTopic",
        overriddenConfigEntry(TopicConfig.RETENTION_MS_CONFIG, "8640000000")
    );

    // When:
    kafkaTopicClient.createTopic("someTopic", 1, (short) -1, configs);

    // Then:
    verify(adminClient, never()).createTopics(any(), any());
  }

  @Test
  public void shouldThrowFromCreateTopicIfExistingHasDifferentReplicationFactor() {
    // Given:
    givenTopicExists("someTopic", 1, 1);
    givenTopicConfigs(
        "someTopic",
        overriddenConfigEntry(TopicConfig.RETENTION_MS_CONFIG, "8640000000")
    );

    // When:
    final Exception e = assertThrows(
        KafkaTopicExistsException.class,
        () -> kafkaTopicClient.createTopic("someTopic", 1, (short) 2, configs)
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        ", 2 replication factor (topic has 1)"));
  }

  @Test
  public void shouldThrowFromCreateTopicIfExistingHasDifferentRetentionMs() {
    // Given:
    givenTopicExists("someTopic", 1, 1);
    givenTopicConfigs(
        "someTopic",
        overriddenConfigEntry(TopicConfig.RETENTION_MS_CONFIG, "8640000000")
    );

    // When:
    Map<String, ?> newConfigs = ImmutableMap.of(TopicConfig.RETENTION_MS_CONFIG, 1000);
    final Exception e = assertThrows(
        KafkaTopicExistsException.class,
        () -> kafkaTopicClient.createTopic("someTopic", 1, (short) 1, newConfigs)
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "and 1000 retention (topic has 8640000000)."));
  }

  @Test
  public void shouldThrowFromCreateTopicIfNoAclsSet() {
    // Given:
    when(adminClient.createTopics(any(), any()))
        .thenAnswer(createTopicsResult(new TopicAuthorizationException("error")));

    // When:
    final Exception e = assertThrows(
        KsqlTopicAuthorizationException.class,
        () -> kafkaTopicClient.createTopic("someTopic", 1, (short) 2, configs)
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "Authorization denied to Create on topic(s): [someTopic]"));
  }

  @Test
  public void shouldRetryDescribeTopicDuringCreateTopicOnRetryableException() {
    // Given:
    givenTopicExists("topicName", 1, 2);
    givenTopicConfigs(
        "topicName",
        overriddenConfigEntry(TopicConfig.RETENTION_MS_CONFIG, "8640000000")
    );

    when(adminClient.describeTopics(anyCollection(), any()))
        .thenAnswer(describeTopicsResult()) // checks that topic exists
        .thenAnswer(describeTopicsResult(new UnknownTopicOrPartitionException("meh"))) // fails during validateProperties
        .thenAnswer(describeTopicsResult()); // succeeds the third time

    // When:
    kafkaTopicClient.createTopic("topicName", 1, (short) 2, configs);

    // Then:
    verify(adminClient, times(3)).describeTopics(anyCollection(), any());
  }

  @Test
  public void shouldValidateCreateTopic() {
    // Given
    givenTopicConfigs(
        "topicA",
        overriddenConfigEntry(TopicConfig.RETENTION_MS_CONFIG, "8640000000")
    );

    // When:
    kafkaTopicClient.validateCreateTopic("topicA", 2, (short) 1, configs);

    // Then:
    verify(adminClient).createTopics(
        eq(ImmutableSet.of(newTopic("topicA", 2, 1, configs))),
        argThat(CreateTopicsOptions::shouldValidateOnly)
    );
  }

  @Test
  public void shouldNotValidateCreateTopicIfItAlreadyExistsWithMatchingDetails() {
    // Given:
    givenTopicExists("someTopic", 1, 2);
    givenTopicConfigs(
        "someTopic",
        overriddenConfigEntry(TopicConfig.RETENTION_MS_CONFIG, "8640000000")
    );

    // When:
    kafkaTopicClient.validateCreateTopic("someTopic", 1, (short) 2, configs);

    // Then:
    verify(adminClient, never()).createTopics(any(), any());
  }

  @Test
  public void shouldThrowFromValidateCreateTopicIfExistingHasDifferentReplicationFactor() {
    // Given:
    givenTopicExists("someTopic", 1, 1);
    givenTopicConfigs(
        "someTopic",
        overriddenConfigEntry(TopicConfig.RETENTION_MS_CONFIG, "8640000000")
    );

    // When:
    final Exception e = assertThrows(
        KafkaTopicExistsException.class,
        () -> kafkaTopicClient.validateCreateTopic("someTopic", 1, (short) 2, configs)
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        ", 2 replication factor (topic has 1)"));
  }

  @Test
  public void shouldThrowFromValidateCreateTopicIfNoAclsSet() {
    // Given:
    when(adminClient.createTopics(any(), any()))
        .thenAnswer(createTopicsResult(new TopicAuthorizationException("error")));

    // When:
    final Exception e = assertThrows(
        KsqlTopicAuthorizationException.class,
        () -> kafkaTopicClient.validateCreateTopic("someTopic", 1, (short) 2, configs)
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "Authorization denied to Create on topic(s): [someTopic]"));
  }

  @Test
  public void shouldNotValidateCreateTopicIfItAlreadyExistsWithDefaultRf() {
    // Given:
    givenTopicExists("someTopic", 1, 2);
    givenTopicConfigs(
        "someTopic",
        overriddenConfigEntry(TopicConfig.RETENTION_MS_CONFIG, "8640000000")
    );

    // When:
    kafkaTopicClient.validateCreateTopic("someTopic", 1, (short) -1, configs);

    // Then:
    verify(adminClient, never()).createTopics(any(), any());
  }

  @Test
  public void shouldRetryDescribeTopicDuringValidateCreateTopicOnRetryableException() {
    // Given:
    givenTopicExists("topicName", 1, 2);
    givenTopicConfigs(
        "topicName",
        overriddenConfigEntry(TopicConfig.RETENTION_MS_CONFIG, "8640000000")
    );

    when(adminClient.describeTopics(anyCollection(), any()))
        .thenAnswer(describeTopicsResult()) // checks that topic exists
        .thenAnswer(describeTopicsResult(new UnknownTopicOrPartitionException("meh"))) // fails during validateProperties
        .thenAnswer(describeTopicsResult()); // succeeds the third time

    // When:
    kafkaTopicClient.validateCreateTopic("topicName", 1, (short) 2, configs);

    // Then:
    verify(adminClient, times(3)).describeTopics(anyCollection(), any());
  }

  @Test
  public void shouldThrowOnDescribeTopicsWhenRetriesExpire() {
    // Given:
    when(adminClient.describeTopics(anyCollection(), any()))
        .thenAnswer(describeTopicsResult(new UnknownTopicOrPartitionException("meh")))
        .thenAnswer(describeTopicsResult(new UnknownTopicOrPartitionException("meh")))
        .thenAnswer(describeTopicsResult(new UnknownTopicOrPartitionException("meh")))
        .thenAnswer(describeTopicsResult(new UnknownTopicOrPartitionException("meh")))
        .thenAnswer(describeTopicsResult(new UnknownTopicOrPartitionException("meh")));

    // When:
    assertThrows(
        KafkaResponseGetFailedException.class,
        () -> kafkaTopicClient.describeTopics(Collections.singleton("aTopic"))
    );
  }

  @Test
  public void shouldThrowOnDescribeOnTopicAuthorizationException() {
    // Given:
    when(adminClient.describeTopics(anyCollection(), any()))
        .thenAnswer(describeTopicsResult(new TopicAuthorizationException("meh")));

    // When:
    final Exception e = assertThrows(
        KsqlTopicAuthorizationException.class,
        () -> kafkaTopicClient.describeTopics(ImmutableList.of("topic1"))
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "Authorization denied to Describe on topic(s): [topic1]"));
  }

  @Test
  public void shouldListTopicNames() {
    // When:
    givenTopicExists("topicA", 1, 1);
    givenTopicExists("topicB", 1, 2);

    when(adminClient.listTopics())
        .thenAnswer(listTopicResult());

    // When:
    final Set<String> names = kafkaTopicClient.listTopicNames();

    // Then:
    assertThat(names, is(ImmutableSet.of("topicA", "topicB")));
  }

  @Test
  public void shouldRetryListTopics() {
    // When:
    givenTopicExists("topic1", 1, 1);
    givenTopicExists("topic2", 1, 2);

    when(adminClient.listTopics())
        .thenAnswer(listTopicResult(new NotControllerException("Not Controller")))
        .thenAnswer(listTopicResult());

    // When:
    kafkaTopicClient.listTopicNames();

    // Then:
    verify(adminClient, times(2)).listTopics();
  }

  @Test
  public void shouldListTopicsStartOffsets() {
    // When:
    givenTopicExists("topicA", 1, 1);

    when(adminClient.listOffsets(anyMap())).thenAnswer(listTopicOffsets());

    // When:
    final Map<TopicPartition, Long> offsets =
        kafkaTopicClient.listTopicsStartOffsets(ImmutableList.of("topicA"));

    // Then:
    assertThat(offsets,
        is(ImmutableMap.of(new TopicPartition("topicA", 0), 100L)));
  }

  @Test
  public void shouldRetryListTopicsStartOffsets() {
    // When:
    givenTopicExists("topicA", 1, 1);

    when(adminClient.listOffsets(anyMap()))
        .thenAnswer(listTopicOffsets(new NotControllerException("Not Controller")))
        .thenAnswer(listTopicOffsets());

    // When:
    kafkaTopicClient.listTopicsStartOffsets(ImmutableList.of("topicA"));

    // Then:
    verify(adminClient, times(2)).listOffsets(anyMap());
  }


  @Test
  public void shouldListTopicsEndOffsets() {
    // When:
    givenTopicExists("topicA", 1, 1);

    when(adminClient.listOffsets(anyMap())).thenAnswer(listTopicOffsets());

    // When:
    final Map<TopicPartition, Long> offsets =
        kafkaTopicClient.listTopicsEndOffsets(ImmutableList.of("topicA"));

    // Then:
    assertThat(offsets,
        is(ImmutableMap.of(new TopicPartition("topicA", 0), 100L)));
  }

  @Test
  public void shouldRetryListTopicsEndOffsets() {
    // When:
    givenTopicExists("topicA", 1, 1);

    when(adminClient.listOffsets(anyMap()))
        .thenAnswer(listTopicOffsets(new NotControllerException("Not Controller")))
        .thenAnswer(listTopicOffsets());

    // When:
    kafkaTopicClient.listTopicsEndOffsets(ImmutableList.of("topicA"));

    // Then:
    verify(adminClient, times(2)).listOffsets(anyMap());
  }

  @Test
  public void shouldDeleteTopics() {
    // When:
    kafkaTopicClient.deleteTopics(ImmutableSet.of("the-topic"));

    // Then:
    verify(adminClient).deleteTopics(ImmutableSet.of("the-topic"));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void shouldToCallOutToKafkaIfDeleteListIsEmpty() {
    // When:
    kafkaTopicClient.deleteTopics(ImmutableList.of());

    // Then:
    verify(adminClient, never()).deleteTopics(any(Collection.class));
  }

  @Test
  public void shouldDeleteInternalTopics() {
    // Given:
    final String applicationId = "whatEva";
    final String internalTopic1 = applicationId
        + "-KTABLE-FK-JOIN-SUBSCRIPTION-REGISTRATION-0000000012-topic";
    final String internalTopic2 = applicationId
        + "-KTABLE-FK-JOIN-SUBSCRIPTION-RESPONSE-0000000012-topic";
    final String internalTopic3 = applicationId + "-something-changelog";
    final String internalTopic4 = applicationId + "-something-repartition";
    // the next four topics are not prefixed with the application id, so they are not internal
    final String customTopic1 = "what-KTABLE-FK-JOIN-SUBSCRIPTION-REGISTRATION-0000000012-topic";
    final String customTopic2 = "eva-KTABLE-FK-JOIN-SUBSCRIPTION-RESPONSE-0000000012-topic";
    final String customTopic3 = "-something-changelog";
    final String customTopic4 = "-something-repartition";

    givenTopicExists("topic1", 1, 1);
    givenTopicExists(internalTopic1, 1, 1);
    givenTopicExists(internalTopic2, 1, 1);
    givenTopicExists(internalTopic3, 1, 1);
    givenTopicExists(internalTopic4, 1, 1);
    givenTopicExists(customTopic1, 1, 1);
    givenTopicExists(customTopic2, 1, 1);
    givenTopicExists(customTopic3, 1, 1);
    givenTopicExists(customTopic4, 1, 1);
    givenTopicExists("topic2", 1, 1);

    // When:
    kafkaTopicClient.deleteInternalTopics(applicationId);

    // Then:
    verify(adminClient).deleteTopics(ImmutableList.of(
        internalTopic1, internalTopic2, internalTopic3, internalTopic4
    ));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void shouldFailToDeleteOnTopicDeletionDisabledException() {
    // Given:
    when(adminClient.deleteTopics(any(Collection.class)))
        .thenAnswer(deleteTopicsResult(new TopicDeletionDisabledException("error")));

    // When:
    assertThrows(
        TopicDeletionDisabledException.class,
        () -> kafkaTopicClient.deleteTopics(ImmutableList.of("some-topic"))
    );
  }

  @Test
  @SuppressWarnings("unchecked")
  public void shouldFailToDeleteOnTopicAuthorizationException() {
    // Given:
    when(adminClient.deleteTopics(any(Collection.class)))
        .thenAnswer(deleteTopicsResult(new TopicAuthorizationException("error")));

    // When:
    final Exception e = assertThrows(
        KsqlTopicAuthorizationException.class,
        () -> kafkaTopicClient.deleteTopics(ImmutableList.of("theTopic"))
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "Authorization denied to Delete on topic(s): [theTopic]"));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void shouldFailToDeleteOnKafkaDeleteTopicsException() {
    // Given:
    when(adminClient.deleteTopics(any(Collection.class)))
        .thenAnswer(deleteTopicsResult(new Exception("error")));

    // When:
    assertThrows(
        KafkaDeleteTopicsException.class,
        () -> kafkaTopicClient.deleteTopics(ImmutableList.of("aTopic"))
    );
  }

  @Test
  @SuppressWarnings("unchecked")
  public void shouldNotThrowKafkaDeleteTopicsExceptionWhenMissingTopic() {
    // Given:
    when(adminClient.deleteTopics(any(Collection.class)))
        .thenAnswer(deleteTopicsResult(new UnknownTopicOrPartitionException("error")));

    // When:
    kafkaTopicClient.deleteTopics(ImmutableList.of("aTopic"));

    // Then: did NOT throw.
  }

  @Test
  public void shouldGetTopicConfig() {
    // Given:
    givenTopicConfigs(
        "fred",
        overriddenConfigEntry(TopicConfig.RETENTION_MS_CONFIG, "12345"),
        defaultConfigEntry(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "1")
    );

    // When:
    final Map<String, String> config = kafkaTopicClient.getTopicConfig("fred");

    // Then:
    assertThat(config.get(TopicConfig.RETENTION_MS_CONFIG), is("12345"));
    assertThat(config.get(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG), is("1"));
  }

  @Test
  public void shouldNotFailWhenTopicConfigValueIsNull() {
    // Given:
    givenTopicConfigs(
        "fred",
        overriddenConfigEntry(TopicConfig.RETENTION_MS_CONFIG, "12345"),
        overriddenConfigEntry(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, null)
    );

    // When:
    final Map<String, String> config = kafkaTopicClient.getTopicConfig("fred");

    // Then:
    assertThat(config.get(TopicConfig.RETENTION_MS_CONFIG), is("12345"));
    assertThat(config.containsKey(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG), is(false));
  }

  @Test
  public void shouldGetTopicCleanUpPolicyDelete() {
    // Given:
    givenTopicConfigs(
        "foo",
        overriddenConfigEntry(CLEANUP_POLICY_CONFIG, CLEANUP_POLICY_DELETE)
    );

    // When / Then:
    assertThat(kafkaTopicClient.getTopicCleanupPolicy("foo"),
        is(TopicCleanupPolicy.DELETE));
  }

  @Test
  public void shouldGetTopicCleanUpPolicyCompact() {
    // Given:
    givenTopicConfigs(
        "foo",
        overriddenConfigEntry(CLEANUP_POLICY_CONFIG, CLEANUP_POLICY_COMPACT)
    );

    // When / Then:
    assertThat(kafkaTopicClient.getTopicCleanupPolicy("foo"),
        is(TopicCleanupPolicy.COMPACT));
  }

  @Test
  public void shouldGetTopicCleanUpPolicyCompactAndDelete() {
    // Given:
    givenTopicConfigs(
        "foo",
        overriddenConfigEntry(CLEANUP_POLICY_CONFIG,
            CLEANUP_POLICY_COMPACT + "," + CLEANUP_POLICY_DELETE)
    );

    // When / Then:
    assertThat(kafkaTopicClient.getTopicCleanupPolicy("foo"),
        is(TopicCleanupPolicy.COMPACT_DELETE));
  }

  @Test
  public void shouldThrowOnNoneRetryableGetTopicConfigError() {
    // Given:
    when(adminClient.describeConfigs(any()))
        .thenAnswer(describeConfigsResult(new RuntimeException()));

    // When:
    assertThrows(
        KafkaResponseGetFailedException.class,
        () -> kafkaTopicClient.getTopicConfig("fred")
    );
  }

  @Test
  public void shouldHandleRetryableGetTopicConfigError() {
    // Given:
    givenTopicConfigs(
        "fred",
        overriddenConfigEntry(TopicConfig.RETENTION_MS_CONFIG, "12345"),
        defaultConfigEntry(TopicConfig.COMPRESSION_TYPE_CONFIG, "producer")
    );

    when(adminClient.describeConfigs(any()))
        .thenAnswer(describeConfigsResult(new DisconnectException()))
        .thenAnswer(describeConfigsResult());

    // When:
    kafkaTopicClient.getTopicConfig("fred");

    // Then:
    verify(adminClient, times(2)).describeConfigs(any());
  }

  @Test
  public void shouldThrowKsqlTopicAuthorizationExceptionFromGetTopicConfig() {
    // Given:
    final String topicName = "foobar";
    when(adminClient.describeConfigs(ImmutableList.of(topicResource(topicName))))
        .thenAnswer(describeConfigsResult(new TopicAuthorizationException(ImmutableSet.of(topicName))));

    // When:
    final Exception e = assertThrows(
        KsqlTopicAuthorizationException.class,
        () -> kafkaTopicClient.getTopicConfig(topicName)
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "Authorization denied to Describe_configs on topic(s): [" + topicName + "]"));
  }

  @Test
  public void shouldSetTopicCleanupPolicyToCompact() {
    // Given:
    final Map<String, String> configs = ImmutableMap.of(
        "cleanup.policy", "compact",
        TopicConfig.RETENTION_MS_CONFIG, "5000");

    // When:
    kafkaTopicClient.createTopic("topic-name", 1, (short) 2, configs);

    // Then:
    verify(adminClient).createTopics(
        eq(ImmutableSet.of(newTopic("topic-name", 1, 2, configs))),
        any()
    );
  }

  @Test
  public void shouldSetStringTopicConfig() {
    // Given:
    givenTopicConfigs(
        "peter",
        overriddenConfigEntry(TopicConfig.RETENTION_MS_CONFIG, "12345"),
        defaultConfigEntry(TopicConfig.COMPRESSION_TYPE_CONFIG, "snappy")
    );

    final Map<String, ?> configOverrides = ImmutableMap.of(
        CLEANUP_POLICY_CONFIG, CLEANUP_POLICY_COMPACT
    );

    // When:
    final boolean changed = kafkaTopicClient.addTopicConfig("peter", configOverrides);

    // Then:
    assertThat("should return true", changed);
    verify(adminClient).incrementalAlterConfigs(ImmutableMap.of(
        topicResource("peter"),
        ImmutableSet.of(
            setConfig(CLEANUP_POLICY_CONFIG, CLEANUP_POLICY_COMPACT)
        )
    ));
  }

  @Test
  public void shouldSetNonStringTopicConfig() {
    // Given:
    givenTopicConfigs(
        "peter",
        overriddenConfigEntry(TopicConfig.RETENTION_MS_CONFIG, "12345"),
        defaultConfigEntry(TopicConfig.COMPRESSION_TYPE_CONFIG, "snappy")
    );

    final Map<String, ?> configOverrides = ImmutableMap.of(
        TopicConfig.RETENTION_MS_CONFIG, 54321L
    );

    // When:
    final boolean changed = kafkaTopicClient.addTopicConfig("peter", configOverrides);

    // Then:
    assertThat("should return true", changed);
    verify(adminClient).incrementalAlterConfigs(ImmutableMap.of(
        topicResource("peter"),
        ImmutableSet.of(
            setConfig(TopicConfig.RETENTION_MS_CONFIG, "54321")
        )
    ));
  }

  @SuppressWarnings("deprecation")
  @Test
  public void shouldFallBackToAddTopicConfigForOlderBrokers() {
    // Given:
    givenTopicConfigs(
        "peter",
        overriddenConfigEntry(TopicConfig.RETENTION_MS_CONFIG, "1234"),
        defaultConfigEntry(TopicConfig.COMPRESSION_TYPE_CONFIG, "snappy")
    );

    final Map<String, ?> overrides = ImmutableMap.of(
        CLEANUP_POLICY_CONFIG, CLEANUP_POLICY_COMPACT
    );

    when(adminClient.incrementalAlterConfigs(any()))
        .thenAnswer(alterConfigsResult(new UnsupportedVersionException("")));

    // When:
    kafkaTopicClient.addTopicConfig("peter", overrides);

    // Then:
    verify(adminClient).alterConfigs(ImmutableMap.of(
        topicResource("peter"),
        new Config(ImmutableSet.of(
            new ConfigEntry(CLEANUP_POLICY_CONFIG, CLEANUP_POLICY_COMPACT),
            new ConfigEntry(TopicConfig.RETENTION_MS_CONFIG, "1234")
        ))
    ));
  }

  @Test
  public void shouldNotAlterStringConfigIfMatchingConfigOverrideExists() {
    // Given:
    givenTopicConfigs(
        "peter",
        overriddenConfigEntry(CLEANUP_POLICY_CONFIG, CLEANUP_POLICY_COMPACT),
        defaultConfigEntry(TopicConfig.COMPRESSION_TYPE_CONFIG, "snappy")
    );

    final Map<String, ?> overrides = ImmutableMap.of(
        CLEANUP_POLICY_CONFIG, CLEANUP_POLICY_COMPACT
    );

    // When:
    final boolean result = kafkaTopicClient.addTopicConfig("peter", overrides);

    // Then:
    assertThat("should return false", !result);
    verify(adminClient, never()).incrementalAlterConfigs(any());
  }

  @Test
  public void shouldNotAlterNonStringConfigIfMatchingConfigOverrideExists() {
    // Given:
    givenTopicConfigs(
        "peter",
        overriddenConfigEntry(TopicConfig.RETENTION_MS_CONFIG, "12345"),
        defaultConfigEntry(TopicConfig.COMPRESSION_TYPE_CONFIG, "snappy")
    );

    final Map<String, ?> overrides = ImmutableMap.of(
        TopicConfig.RETENTION_MS_CONFIG, 12345L
    );

    // When:
    final boolean result = kafkaTopicClient.addTopicConfig("peter", overrides);

    // Then:
    assertThat("should return false", !result);
    verify(adminClient, never()).incrementalAlterConfigs(any());
  }

  @Test
  public void shouldRetryAddingTopicConfig() {
    // Given:
    givenTopicConfigs(
        "peter",
        overriddenConfigEntry(TopicConfig.RETENTION_MS_CONFIG, "12345"),
        defaultConfigEntry(TopicConfig.COMPRESSION_TYPE_CONFIG, "snappy")
    );

    final Map<String, ?> overrides = ImmutableMap.of(
        CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT
    );

    when(adminClient.incrementalAlterConfigs(any()))
        .thenAnswer(alterConfigsResult(new DisconnectException()))
        .thenAnswer(alterConfigsResult());

    // When:
    kafkaTopicClient.addTopicConfig("peter", overrides);

    // Then:
    verify(adminClient, times(2)).incrementalAlterConfigs(any());
  }

  @Test
  public void shouldNotListAllTopicsWhenCallingIsTopicExists() {
    // Given
    givenTopicExists("foobar", 1, 1);

    // When
    kafkaTopicClient.isTopicExists("foobar");

    // Then
    verify(adminClient, never()).listTopics();
  }

  @Test
  public void shouldNotRetryIsTopicExistsOnUnknownTopicException() {
    // When
    kafkaTopicClient.isTopicExists("foobar");

    // Then
    verify(adminClient, times(1)).describeTopics(anyCollection(), any());
  }

  @Test
  public void shouldNotRetryDescribeTopicsExistsAnyException() {
    // When
    when(adminClient.describeTopics(anyCollection(), any()))
            .thenAnswer(describeTopicsResult(new UnknownTopicOrPartitionException("meh")));
    final String topicName = "foobar";
    final Exception e = assertThrows(
            KafkaResponseGetFailedException.class,
            () -> kafkaTopicClient.describeTopic(topicName, true)
    );

    // Then
    verify(adminClient, times(1)).describeTopics(anyCollection(), any());
    assertThat(e.getMessage(),
            containsString("Failed to Describe Kafka Topic(s): [" + topicName + "]"));
  }

  @Test
  public void shouldThrowIsTopicExistsOnAuthorizationException() {
    // Given
    when(adminClient.describeTopics(eq(ImmutableList.of("foobar")), any()))
        .thenAnswer(describeTopicsResult(new TopicAuthorizationException("foobar")));

    // When
    final Exception e = assertThrows(
        KsqlTopicAuthorizationException.class,
        () -> kafkaTopicClient.isTopicExists("foobar")
    );

    // Then
    assertThat(e.getMessage(),
        containsString("Authorization denied to Describe on topic(s): [foobar]"));
  }

  private static ConfigEntry defaultConfigEntry(final String key, final String value) {
    final ConfigEntry config = mock(ConfigEntry.class);
    when(config.name()).thenReturn(key);
    when(config.value()).thenReturn(value);
    when(config.isDefault()).thenReturn(true);
    return config;
  }

  private static ConfigEntry overriddenConfigEntry(final String key, final String value) {
    final ConfigEntry config = mock(ConfigEntry.class);
    when(config.name()).thenReturn(key);
    when(config.value()).thenReturn(value);
    when(config.isDefault()).thenReturn(false);
    return config;
  }

  private void givenTopicExists(final String name, final int partitions, final int rf) {

    final List<Node> replicas = ImmutableList.copyOf(IntStream.range(0, rf)
        .mapToObj(idx -> A_NODE)
        .collect(Collectors.toList()));

    final List<TopicPartitionInfo> partitionInfo = ImmutableList
        .copyOf(IntStream.range(0, partitions)
            .mapToObj(idx -> new TopicPartitionInfo(idx, A_NODE, replicas, replicas))
            .collect(Collectors.toList()));

    topicPartitionInfo.put(name, partitionInfo);
  }

  private void givenTopicConfigs(
      final String name,
      final ConfigEntry... configs
  ) {
    topicConfigs.put(topicResource(name), new Config(ImmutableList.copyOf(configs)));
  }

  private Answer<ListTopicsResult> listTopicResult() {
    return inv -> {
      final ListTopicsResult listTopicsResult = mock(ListTopicsResult.class);

      when(listTopicsResult.names())
          .thenReturn(
              KafkaFuture.completedFuture(ImmutableSet.copyOf(topicPartitionInfo.keySet())));

      return listTopicsResult;
    };
  }

  private Answer<ListOffsetsResult> listTopicOffsets() {
    return inv -> {
      final ListOffsetsResult result = mock(ListOffsetsResult.class);
      when(result.all()).thenReturn(KafkaFuture.completedFuture(ImmutableMap.of(
          new TopicPartition("topicA", 0),
          new ListOffsetsResultInfo(100L, 0L, Optional.empty()))));
      return result;
    };
  }

  private Answer<ListOffsetsResult> listTopicOffsets(final Exception e) {
    return inv -> {
      final ListOffsetsResult result = mock(ListOffsetsResult.class);
      final KafkaFuture<Map<TopicPartition, ListOffsetsResultInfo>> f = failedFuture(e);
      when(result.all()).thenReturn(f);
      return result;
    };
  }

  private static Answer<ListTopicsResult> listTopicResult(final Exception e) {
    return inv -> {
      final ListTopicsResult listTopicsResult = mock(ListTopicsResult.class);
      final KafkaFuture<Set<String>> f = failedFuture(e);
      when(listTopicsResult.names()).thenReturn(f);
      return listTopicsResult;
    };
  }

  private Answer<DescribeTopicsResult> describeTopicsResult() {
    return inv -> {
      final Collection<String> topicNames = inv.getArgument(0);
      if (topicNames == null) {
        // Called from mock
        return null;
      }

      final Map<String, TopicDescription> result = topicNames.stream()
          .filter(topicPartitionInfo::containsKey)
          .map(name -> new TopicDescription(name, false, topicPartitionInfo.get(name)))
          .collect(Collectors.toMap(TopicDescription::name, Function.identity()));

      Map<String, KafkaFuture<TopicDescription>> describe = new HashMap<>();
      for (String name : topicNames) {
        describe.put(name, result.containsKey(name)
            ? KafkaFuture.completedFuture(result.get(name))
            : failedFuture(new UnknownTopicOrPartitionException()));
      }

      final DescribeTopicsResult describeTopicsResult = mock(DescribeTopicsResult.class);
      when(describeTopicsResult.topicNameValues()).thenReturn(describe );
      when(describeTopicsResult.allTopicNames()).thenReturn(KafkaFuture.completedFuture(result));
      return describeTopicsResult;
    };
  }

  private static Answer<DescribeTopicsResult> describeTopicsResult(final Exception e) {
    return inv -> {
      final Collection<String> topicNames = inv.getArgument(0);
      final DescribeTopicsResult describeTopicsResult = mock(DescribeTopicsResult.class);

      Map<String, KafkaFuture<TopicDescription>> map = new HashMap<>();
      for (String name : topicNames) {
        if (map.put(name, failedFuture(e)) != null) {
          throw new IllegalStateException("Duplicate key");
        }
      }
      when(describeTopicsResult.topicNameValues()).thenReturn(map);

      final KafkaFuture<Map<String, TopicDescription>> f = failedFuture(e);
      when(describeTopicsResult.allTopicNames()).thenReturn(f);
      return describeTopicsResult;
    };
  }

  private static Answer<CreateTopicsResult> createTopicsResult() {
    return inv -> {
      final CreateTopicsResult createTopicsResult = mock(CreateTopicsResult.class);
      when(createTopicsResult.all()).thenReturn(KafkaFuture.allOf());
      return createTopicsResult;
    };
  }

  private static Answer<CreateTopicsResult> createTopicsResult(final Exception e) {
    return inv -> {
      final CreateTopicsResult createTopicsResult = mock(CreateTopicsResult.class);
      final KafkaFuture<Void> f = failedFuture(e);
      when(createTopicsResult.all()).thenReturn(f);
      return createTopicsResult;
    };
  }

  private Answer<DeleteTopicsResult> deleteTopicsResult() {
    return inv -> {
      final Collection<String> topicNames = inv.getArgument(0);
      if (topicNames == null) {
        // Called from mock
        return null;
      }

      final Map<String, KafkaFuture<Void>> result = topicNames.stream()
          .collect(Collectors.toMap(
              Function.identity(),
              name -> topicPartitionInfo.containsKey(name)
                  ? KafkaFuture.allOf()
                  : failedFuture(new UnknownTopicOrPartitionException())
          ));

      final DeleteTopicsResult deleteTopicsResult = mock(DeleteTopicsResult.class);
      when(deleteTopicsResult.topicNameValues()).thenReturn(result);
      return deleteTopicsResult;
    };
  }

  private static Answer<DeleteTopicsResult> deleteTopicsResult(final Exception e) {
    return inv -> {
      final Collection<String> topicNames = inv.getArgument(0);
      if (topicNames == null) {
        // Called from mock
        return null;
      }

      final Map<String, KafkaFuture<Void>> result = topicNames.stream()
          .collect(Collectors.toMap(
              Function.identity(),
              name -> failedFuture(e))
          );

      final DeleteTopicsResult deleteTopicsResult = mock(DeleteTopicsResult.class);
      when(deleteTopicsResult.topicNameValues()).thenReturn(result);
      return deleteTopicsResult;
    };
  }

  private Answer<DescribeConfigsResult> describeConfigsResult() {
    return inv -> {
      final Collection<ConfigResource> resources = inv.getArgument(0);
      if (resources == null) {
        // Called from mock
        System.err.println("frm mock: success");
        return null;
      }

      System.err.println("success");
      final Map<ConfigResource, Config> result = resources.stream()
          .filter(topicConfigs::containsKey)
          .collect(Collectors.toMap(
              Function.identity(),
              topicConfigs::get)
          );

      final DescribeConfigsResult describeConfigsResult = mock(DescribeConfigsResult.class);
      when(describeConfigsResult.all()).thenReturn(KafkaFuture.completedFuture(result));
      return describeConfigsResult;
    };
  }

  private static Answer<DescribeConfigsResult> describeConfigsResult(final Exception e) {
    return inv -> {
      final Collection<ConfigResource> resources = inv.getArgument(0);
      if (resources == null) {
        // Called from mock
        System.err.println("frm mock: failed");
        return null;
      }

      System.err.println("failed");
      final DescribeConfigsResult describeConfigsResult = mock(DescribeConfigsResult.class);
      final KafkaFuture<Map<ConfigResource, Config>> f = failedFuture(e);
      when(describeConfigsResult.all()).thenReturn(f);
      return describeConfigsResult;
    };
  }

  private static Answer<AlterConfigsResult> alterConfigsResult() {
    return inv -> {
      final AlterConfigsResult response = mock(AlterConfigsResult.class);
      when(response.all()).thenReturn(KafkaFuture.completedFuture(null));
      return response;
    };
  }

  private static Answer<AlterConfigsResult> alterConfigsResult(final Exception e) {
    return inv -> {
      final AlterConfigsResult response = mock(AlterConfigsResult.class);
      final KafkaFuture<Void> f = failedFuture(e);
      when(response.all()).thenReturn(f);
      return response;
    };
  }

  @SuppressFBWarnings(
      value = "REC_CATCH_EXCEPTION",
      justification = "code won't compile without it"
  )
  @SuppressWarnings("unchecked")
  private static <T> KafkaFuture<T> failedFuture(final Exception cause) {
    try {
      final KafkaFuture<T> future = mock(KafkaFuture.class);
      doThrow(new ExecutionException(cause)).when(future).get();
      doThrow(new ExecutionException(cause)).when(future).get(anyLong(), any());
      return future;
    } catch (final Exception e) {
      throw new AssertionError("invalid test", e);
    }
  }

  private static NewTopic newTopic(
      final String name,
      final int partitions,
      final int rf,
      final Map<String, ?> configs
  ) {
    final NewTopic newTopic = new NewTopic(name, partitions, (short) rf);
    newTopic.configs(toStringConfigs(configs));
    return newTopic;
  }

  private static ConfigResource topicResource(final String topicName) {
    return new ConfigResource(ConfigResource.Type.TOPIC, topicName);
  }

  private static AlterConfigOp setConfig(final String name, final String value) {
    return new AlterConfigOp(new ConfigEntry(name, value), AlterConfigOp.OpType.SET);
  }

  private static Map<String, String> toStringConfigs(final Map<String, ?> configs) {
    return configs.entrySet().stream()
        .collect(Collectors.toMap(Entry::getKey, e -> e.getValue().toString()));
  }
}
