/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.rest.util;

import io.confluent.ksql.KsqlEngine;
import io.confluent.ksql.metastore.KsqlTopic;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.rest.server.KsqlRestConfig;
import io.confluent.ksql.services.KafkaTopicClient;
import io.confluent.ksql.services.KafkaTopicClientImpl;
import io.confluent.ksql.util.ExecutorUtil;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.QueryMetadata;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.streams.processor.internals.DefaultKafkaClientSupplier;

public class ClusterTerminator {

  private final KsqlConfig ksqlConfig;
  private final KsqlEngine ksqlEngine;
  private final KafkaTopicClient kafkaTopicClient;

  public ClusterTerminator(
      final KsqlConfig ksqlConfig,
      final KsqlEngine ksqlEngine
  ) {
    this(ksqlConfig,
         ksqlEngine,
         new KafkaTopicClientImpl(
             new DefaultKafkaClientSupplier()
                 .getAdminClient(ksqlConfig.getKsqlAdminClientConfigProps())));
  }

  public ClusterTerminator(
      final KsqlConfig ksqlConfig,
      final KsqlEngine ksqlEngine,
      final KafkaTopicClient kafkaTopicClient
  ) {
    Objects.requireNonNull(ksqlConfig, "ksqlConfig is null.");
    Objects.requireNonNull(ksqlEngine, "ksqlEngine is null.");
    Objects.requireNonNull(kafkaTopicClient, "kafkaTopicClient is null.");
    this.ksqlConfig = ksqlConfig;
    this.ksqlEngine = ksqlEngine;
    this.kafkaTopicClient = kafkaTopicClient;
  }

  public void terminateCluster(final List<String> deleteTopicPatterns) {
    terminatePersistentQueries();
    deleteSinkTopics(deleteTopicPatterns, kafkaTopicClient);
    deleteCommandTopic(kafkaTopicClient);
    ksqlEngine.close();
  }

  private void terminatePersistentQueries() {
    ksqlEngine.getPersistentQueries().forEach(QueryMetadata::close);
  }

  private void deleteSinkTopics(
      final List<String> deleteTopicPatterns,
      final KafkaTopicClient serviceContext) {
    if (deleteTopicPatterns.isEmpty()) {
      return;
    }

    final List<Pattern> patterns = deleteTopicPatterns.stream()
        .map(Pattern::compile)
        .collect(Collectors.toList());

    final MetaStore metaStore = ksqlEngine.getMetaStore();
    final List<String> toDelete = metaStore.getAllKsqlTopics().values().stream()
        .filter(KsqlTopic::isKsqlSink)
        .map(KsqlTopic::getKafkaTopicName)
        .filter(topicName -> topicShouldBeDeleted(topicName, patterns))
        .collect(Collectors.toList());
    deleteTopics(toDelete, serviceContext);
  }

  private List<String> filterNonExistingTopics(
      final List<String> topicList,
      final KafkaTopicClient topicClient) {
    final Set<String> existingTopicNames = topicClient.listTopicNames();
    return topicList.stream().filter(existingTopicNames::contains).collect(Collectors.toList());
  }

  private static boolean topicShouldBeDeleted(
      final String topicName, final List<Pattern> patterns
  ) {
    return patterns.stream()
        .anyMatch(pattern -> pattern.matcher(topicName).matches());
  }

  private void deleteCommandTopic(final KafkaTopicClient serviceContext) {
    final String ksqlServiceId = ksqlConfig.getString(KsqlConfig.KSQL_SERVICE_ID_CONFIG);
    final String commandTopic = KsqlRestConfig.getCommandTopic(ksqlServiceId);
    deleteTopics(Collections.singletonList(commandTopic), serviceContext);
  }

  private void deleteTopics(
      final List<String> topicsToBeDeleted,
      final KafkaTopicClient topicClient) {
    try {
      ExecutorUtil.executeWithRetries(
          () -> topicClient.deleteTopics(
              filterNonExistingTopics(topicsToBeDeleted, topicClient)),
          ExecutorUtil.RetryBehaviour.ALWAYS);
    } catch (final Exception e) {
      throw new KsqlException(
          "Exception while deleting topics: " + StringUtils.join(topicsToBeDeleted, ","));
    }
  }
}
