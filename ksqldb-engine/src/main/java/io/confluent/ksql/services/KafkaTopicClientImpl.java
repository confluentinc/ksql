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

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import io.confluent.ksql.exception.KafkaDeleteTopicsException;
import io.confluent.ksql.exception.KafkaResponseGetFailedException;
import io.confluent.ksql.exception.KsqlTopicAuthorizationException;
import io.confluent.ksql.topic.TopicProperties;
import io.confluent.ksql.util.ExecutorUtil;
import io.confluent.ksql.util.ExecutorUtil.RetryBehaviour;
import io.confluent.ksql.util.KsqlConstants;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.KsqlServerException;
import io.confluent.ksql.util.Pair;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.CreateTopicsOptions;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.DescribeTopicsOptions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.errors.TopicDeletionDisabledException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Note: all calls make cross machine calls and are synchronous.
 */
// CHECKSTYLE_RULES.OFF: ClassDataAbstractionCoupling
@ThreadSafe
public class KafkaTopicClientImpl implements KafkaTopicClient {
  // CHECKSTYLE_RULES.ON: ClassDataAbstractionCoupling

  private static final Logger LOG = LoggerFactory.getLogger(KafkaTopicClient.class);

  private static final String DEFAULT_REPLICATION_PROP = "default.replication.factor";
  private static final String DELETE_TOPIC_ENABLE = "delete.topic.enable";
  private static final ThreadLocal<Boolean> RETRY_ON_UNKNOWN_TOPIC =
          ThreadLocal.withInitial(() -> true);

  private final Supplier<Admin> adminClient;

  /**
   * Construct a topic client from an existing admin client. Note, the admin client is shared
   * between all methods of this class, i.e the admin client is created only once and then reused.
   *
   * @param sharedAdminClient the admin client .
   */
  public KafkaTopicClientImpl(final Supplier<Admin> sharedAdminClient) {
    this.adminClient = Objects.requireNonNull(sharedAdminClient, "sharedAdminClient");
  }

  @Override
  public void setRetryOnUnknownTopic(final boolean retry) {
    RETRY_ON_UNKNOWN_TOPIC.set(retry);
  }

  @Override
  public void createTopic(
      final String topic,
      final int numPartitions,
      final short replicationFactor,
      final Map<String, ?> configs,
      final CreateTopicsOptions createOptions
  ) {
    if (isTopicExists(topic)) {
      validateTopicProperties(topic, numPartitions, replicationFactor);
      return;
    }

    final short resolvedReplicationFactor = replicationFactor == TopicProperties.DEFAULT_REPLICAS
        ? getDefaultClusterReplication()
        : replicationFactor;

    final NewTopic newTopic = new NewTopic(topic, numPartitions, resolvedReplicationFactor);
    newTopic.configs(toStringConfigs(configs));

    try {
      LOG.info("Creating topic '{}' {}",
          topic,
          (createOptions.shouldValidateOnly()) ? "(ONLY VALIDATE)" : ""
      );

      ExecutorUtil.executeWithRetries(
          () -> adminClient.get().createTopics(
              Collections.singleton(newTopic),
              createOptions
          ).all().get(),
          ExecutorUtil.RetryBehaviour.ON_RETRYABLE);
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new KafkaResponseGetFailedException(
          "Failed to guarantee existence of topic " + topic, e);

    } catch (final TopicExistsException e) {
      // if the topic already exists, it is most likely because another node just created it.
      // ensure that it matches the partition count and replication factor before returning
      // success
      validateTopicProperties(topic, numPartitions, replicationFactor);

    } catch (final TopicAuthorizationException e) {
      throw new KsqlTopicAuthorizationException(
          AclOperation.CREATE, Collections.singleton(topic));

    } catch (final Exception e) {
      throw new KafkaResponseGetFailedException(
          "Failed to guarantee existence of topic " + topic, e);

    }
  }

  /**
   * We need this method because {@link Admin#createTopics(Collection)} does not allow you to pass
   * in only partitions. Instead, we determine the default number from the cluster config and then
   * pass that value back.
   *
   * @return the default broker configuration
   */
  private short getDefaultClusterReplication() {
    try {
      final String defaultReplication = getConfig()
          .get(DEFAULT_REPLICATION_PROP)
          .value();
      return Short.parseShort(defaultReplication);
    } catch (final KsqlServerException e) {
      throw e;
    } catch (final Exception e) {
      throw new KsqlServerException("Could not get default replication from Kafka cluster!", e);
    }
  }

  @Override
  public boolean isTopicExists(final String topic) {
    LOG.trace("Checking for existence of topic '{}'", topic);
    try {
      ExecutorUtil.executeWithRetries(
          () -> adminClient.get().describeTopics(
              ImmutableList.of(topic),
              new DescribeTopicsOptions().includeAuthorizedOperations(true)
          ).values().get(topic).get(),
          RetryBehaviour.ON_RETRYABLE.and(e -> !(e instanceof UnknownTopicOrPartitionException))
      );
      return true;
    } catch (final TopicAuthorizationException e) {
      throw new KsqlTopicAuthorizationException(
          AclOperation.DESCRIBE, Collections.singleton(topic));
    } catch (final Exception e) {
      if (Throwables.getRootCause(e) instanceof UnknownTopicOrPartitionException) {
        return false;
      }

      throw new KafkaResponseGetFailedException("Failed to check if exists for topic: " + topic, e);
    }
  }

  @Override
  public Set<String> listTopicNames() {
    try {
      return ExecutorUtil.executeWithRetries(
          () -> adminClient.get().listTopics().names().get(),
          ExecutorUtil.RetryBehaviour.ON_RETRYABLE);
    } catch (final Exception e) {
      throw new KafkaResponseGetFailedException("Failed to retrieve Kafka Topic names", e);
    }
  }

  @Override
  public Map<String, TopicDescription> describeTopics(final Collection<String> topicNames) {
    try {
      return ExecutorUtil.executeWithRetries(
          () -> adminClient.get().describeTopics(
              topicNames,
              new DescribeTopicsOptions().includeAuthorizedOperations(true)
          ).all().get(),
          RETRY_ON_UNKNOWN_TOPIC.get()
              ? ExecutorUtil.RetryBehaviour.ON_RETRYABLE
              : ExecutorUtil.RetryBehaviour.ON_RETRYABLE.and(
                      e -> !(e instanceof UnknownTopicOrPartitionException))
      );
    } catch (final ExecutionException e) {
      throw new KafkaResponseGetFailedException(
          "Failed to Describe Kafka Topic(s): " + topicNames, e.getCause());
    } catch (final TopicAuthorizationException e) {
      throw new KsqlTopicAuthorizationException(
          AclOperation.DESCRIBE, topicNames);
    } catch (final Exception e) {
      if (Throwables.getRootCause(e) instanceof UnknownTopicOrPartitionException) {
        LOG.error("Failed to describe topic(s): {} due to Missing Topic(s)", topicNames, e);
      }
      throw new KafkaResponseGetFailedException(
          "Failed to Describe Kafka Topic(s): " + topicNames, e);
    }
  }

  @Override
  public Map<String, String> getTopicConfig(final String topicName) {
    return topicConfig(topicName, true);
  }

  @Override
  public boolean addTopicConfig(final String topicName, final Map<String, ?> overrides) {
    final ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topicName);

    final Map<String, String> stringConfigs = toStringConfigs(overrides);

    try {
      final Map<String, String> existingConfig = topicConfig(topicName, false);

      final boolean changed = stringConfigs.entrySet().stream()
          .anyMatch(e -> !Objects.equals(existingConfig.get(e.getKey()), e.getValue()));
      if (!changed) {
        return false;
      }

      final Set<AlterConfigOp> entries = stringConfigs.entrySet().stream()
          .map(e -> new ConfigEntry(e.getKey(), e.getValue()))
          .map(ce -> new AlterConfigOp(ce, AlterConfigOp.OpType.SET))
          .collect(Collectors.toSet());

      final Map<ConfigResource, Collection<AlterConfigOp>> request =
          Collections.singletonMap(resource, entries);

      ExecutorUtil.executeWithRetries(
          () -> adminClient.get().incrementalAlterConfigs(request).all().get(),
          ExecutorUtil.RetryBehaviour.ON_RETRYABLE);

      return true;
    } catch (final UnsupportedVersionException e) {
      return addTopicConfigLegacy(topicName, stringConfigs);
    } catch (final Exception e) {
      throw new KafkaResponseGetFailedException(
          "Failed to set config for Kafka Topic " + topicName, e);
    }
  }

  @Override
  public TopicCleanupPolicy getTopicCleanupPolicy(final String topicName) {
    final String policy = getTopicConfig(topicName)
        .getOrDefault(TopicConfig.CLEANUP_POLICY_CONFIG, "")
        .toLowerCase();

    if (policy.equals("compact")) {
      return TopicCleanupPolicy.COMPACT;
    } else if (policy.equals("delete")) {
      return TopicCleanupPolicy.DELETE;
    } else if (policy.contains("compact") && policy.contains("delete")) {
      return TopicCleanupPolicy.COMPACT_DELETE;
    } else {
      throw new KsqlException("Could not get the topic configs for : " + topicName);
    }
  }

  @Override
  public void deleteTopics(final Collection<String> topicsToDelete) {
    if (topicsToDelete.isEmpty()) {
      return;
    }

    final DeleteTopicsResult deleteTopicsResult = adminClient.get().deleteTopics(topicsToDelete);
    final Map<String, KafkaFuture<Void>> results = deleteTopicsResult.topicNameValues();
    final List<String> failList = Lists.newArrayList();
    final List<Pair<String, Throwable>> exceptionList = Lists.newArrayList();
    for (final Map.Entry<String, KafkaFuture<Void>> entry : results.entrySet()) {
      try {
        entry.getValue().get(30, TimeUnit.SECONDS);
      } catch (final Exception e) {
        final Throwable rootCause = ExceptionUtils.getRootCause(e);

        if (rootCause instanceof TopicDeletionDisabledException) {
          throw new TopicDeletionDisabledException("Topic deletion is disabled. "
              + "To delete the topic, you must set '" + DELETE_TOPIC_ENABLE + "' to true in "
              + "the Kafka broker configuration.");
        } else if (rootCause instanceof TopicAuthorizationException) {
          throw new KsqlTopicAuthorizationException(
              AclOperation.DELETE, Collections.singleton(entry.getKey()));
        } else if (!(rootCause instanceof UnknownTopicOrPartitionException)) {
          LOG.error(String.format("Could not delete topic '%s'", entry.getKey()), e);
          failList.add(entry.getKey());
          exceptionList.add(new Pair<>(entry.getKey(), rootCause));
        }
      }
    }

    if (!failList.isEmpty()) {
      throw new KafkaDeleteTopicsException("Failed to clean up topics: "
          + String.join(",", failList), exceptionList);
    }
  }

  @Override
  public void deleteInternalTopics(final String applicationId) {
    try {
      final Set<String> topicNames = listTopicNames();
      final List<String> internalTopics = Lists.newArrayList();
      for (final String topicName : topicNames) {
        if (isInternalTopic(topicName, applicationId)) {
          internalTopics.add(topicName);
        }
      }
      if (!internalTopics.isEmpty()) {
        Collections.sort(internalTopics); // prevents flaky tests
        deleteTopics(internalTopics);
      }
    } catch (final Exception e) {
      LOG.error("Exception while trying to clean up internal topics for application id: {}.",
          applicationId, e
      );
    }
  }

  @Override
  public Map<TopicPartition, Long> listTopicsOffsets(
      final Collection<String> topicNames,
      final OffsetSpec offsetSpec
  ) {
    final Map<TopicPartition, OffsetSpec> offsetsRequest =
        describeTopics(topicNames).entrySet().stream()
            .flatMap(entry ->
                entry.getValue().partitions()
                    .stream()
                    .map(tpInfo -> new TopicPartition(entry.getKey(), tpInfo.partition())))
            .collect(Collectors.toMap(tp -> tp, tp -> offsetSpec));
    try {
      return ExecutorUtil.executeWithRetries(
          () -> adminClient.get().listOffsets(offsetsRequest).all().get()
              .entrySet()
              .stream()
              .collect(Collectors.toMap(
                  Entry::getKey,
                  entry -> entry.getValue().offset())),
          RetryBehaviour.ON_RETRYABLE);
    } catch (final TopicAuthorizationException e) {
      throw new KsqlTopicAuthorizationException(AclOperation.DESCRIBE, e.unauthorizedTopics());
    } catch (final ExecutionException e) {
      throw new KafkaResponseGetFailedException(
          "Failed to get topic offsets. partitions: " + offsetsRequest.keySet(), e.getCause());
    } catch (final Exception e) {
      throw new KafkaResponseGetFailedException(
          "Failed to get topic offsets. partitions: " + offsetsRequest.keySet(), e);
    }
  }

  private Config getConfig() {
    return KafkaClusterUtil.getConfig(adminClient.get());
  }

  private static boolean isInternalTopic(final String topicName, final String applicationId) {
    final boolean prefixMatches = topicName.startsWith(applicationId + "-");
    final boolean suffixMatches = topicName.endsWith(KsqlConstants.STREAMS_CHANGELOG_TOPIC_SUFFIX)
        || topicName.endsWith(KsqlConstants.STREAMS_REPARTITION_TOPIC_SUFFIX)
        || topicName.matches(KsqlConstants.STREAMS_JOIN_REGISTRATION_TOPIC_PATTERN)
        || topicName.matches(KsqlConstants.STREAMS_JOIN_RESPONSE_TOPIC_PATTERN);
    return prefixMatches && suffixMatches;
  }

  private void validateTopicProperties(
      final String topic,
      final int requiredNumPartition,
      final int requiredNumReplicas
  ) {
    final TopicDescription existingTopic = describeTopic(topic);
    TopicValidationUtil
        .validateTopicProperties(requiredNumPartition, requiredNumReplicas, existingTopic);
    LOG.debug(
        "Did not create topic {} with {} partitions and replication-factor {} since it exists",
        topic, requiredNumPartition, requiredNumReplicas);
  }

  private Map<String, String> topicConfig(
      final String topicName,
      final boolean includeDefaults
  ) {
    final ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topicName);
    final List<ConfigResource> request = Collections.singletonList(resource);

    try {
      final Config config = ExecutorUtil.executeWithRetries(
          () -> adminClient.get().describeConfigs(request).all().get(),
          ExecutorUtil.RetryBehaviour.ON_RETRYABLE).get(resource);
      return config.entries().stream()
          .filter(e -> e.value() != null)
          .filter(e -> includeDefaults || !e.isDefault())
          .collect(Collectors.toMap(ConfigEntry::name, ConfigEntry::value));
    } catch (final Exception e) {
      throw new KafkaResponseGetFailedException(
          "Failed to get config for Kafka Topic " + topicName, e);
    }
  }

  // 'alterConfigs' deprecated, but new `incrementalAlterConfigs` only available on Kafka v2.3+
  // So we need to continue to support older brokers until our min requirements reaches v2.3
  @SuppressWarnings({"deprecation", "RedundantSuppression"})
  private boolean addTopicConfigLegacy(
      final String topicName,
      final Map<String, String> overrides
  ) {
    final ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topicName);

    try {
      final Map<String, String> existingConfig = topicConfig(topicName, false);
      existingConfig.putAll(overrides);

      final Set<ConfigEntry> entries = existingConfig.entrySet().stream()
          .map(e -> new ConfigEntry(e.getKey(), e.getValue()))
          .collect(Collectors.toSet());

      final Map<ConfigResource, Config> request =
          Collections.singletonMap(resource, new Config(entries));

      ExecutorUtil.executeWithRetries(
          () -> adminClient.get().alterConfigs(request).all().get(),
          ExecutorUtil.RetryBehaviour.ON_RETRYABLE);

      return true;
    } catch (final Exception e) {
      throw new KafkaResponseGetFailedException(
          "Failed to set config for Kafka Topic " + topicName, e);
    }
  }

  private static Map<String, String> toStringConfigs(final Map<String, ?> configs) {
    return configs.entrySet().stream()
        .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().toString()));
  }
}
