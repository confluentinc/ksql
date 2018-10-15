/**
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
 **/

package io.confluent.ksql.rest.util;

import io.confluent.ksql.KsqlEngine;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.rest.server.KsqlRestConfig;
import io.confluent.ksql.util.ExecutorUtil;
import io.confluent.ksql.util.KafkaTopicClient;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.PersistentQueryMetadata;
import io.confluent.ksql.util.QueryMetadata;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class ClusterTerminator {

  private final KsqlConfig ksqlConfig;
  private final KsqlEngine ksqlEngine;
  private final List<String> deleteTopicList;
  private final KafkaTopicClient kafkaTopicClient;

  public ClusterTerminator(
      final KsqlConfig ksqlConfig,
      final KsqlEngine ksqlEngine,
      final List<String> deleteTopicList
  ) {
    this.ksqlConfig = ksqlConfig;
    this.ksqlEngine = ksqlEngine;
    this.kafkaTopicClient = ksqlEngine.getTopicClient();
    this.deleteTopicList = deleteTopicList;
  }

  @SuppressWarnings("unchecked")
  public void terminateCluster() {
    ksqlEngine.stopAcceptingStatemens();
    terminateCluster(ksqlEngine, deleteTopicList);
    // Delete the command topic
    deleteCommandTopic(ksqlConfig, ksqlEngine);
  }

  private void terminateCluster(
      final KsqlEngine ksqlEngine,
      final List<String> deleteTopicList
  ) {
    terminateAllQueries();

    // if we have the explicit list of topics to delete.
    final MetaStore metaStore = ksqlEngine.getMetaStore();
    final List<Pattern> patterns = getDeleteTopicPatterns(deleteTopicList);
    if (!deleteTopicList.isEmpty()) {
      metaStore.getAllKsqlTopics()
          .forEach((s, structuredDataSource) -> {
            if (structuredDataSource.isKsqlSink()) {
              deleteKafkaTopicIfRequested(structuredDataSource.getKafkaTopicName(), patterns);
            }
          });
    }
  }

  private void terminateAllQueries() {
    getListForSet(ksqlEngine).forEach(
        queryMetadata -> {
          if (queryMetadata instanceof PersistentQueryMetadata) {
            final PersistentQueryMetadata persistentQueryMetadata
                = (PersistentQueryMetadata) queryMetadata;
            ksqlEngine.terminateQuery(persistentQueryMetadata.getQueryId(), true);
          }  else {
            queryMetadata.close();
          }
        }
    );
  }

  // This is needed because the checkstyle complains if we create this in place.
  private List<QueryMetadata> getListForSet(
      final KsqlEngine ksqlEngine
  ) {
    final List<QueryMetadata> queryMetadataList = new ArrayList<>();
    queryMetadataList.addAll(ksqlEngine.getAllLiveQueries());
    return queryMetadataList;
  }

  private void deleteKafkaTopicIfRequested(
      final String kafkaTopicName,
      final List<Pattern> patterns) {
    for (final Pattern pattern: patterns) {
      if (pattern.matcher(kafkaTopicName).matches()) {
        kafkaTopicClient.deleteTopics(Collections.singletonList(kafkaTopicName));
        return;
      }
    }
  }

  private void deleteCommandTopic(
      final KsqlConfig ksqlConfig,
      final KsqlEngine ksqlEngine) {
    final String ksqlServiceId = ksqlConfig.getString(KsqlConfig.KSQL_SERVICE_ID_CONFIG);
    final String commandTopic = KsqlRestConfig.getCommandTopic(ksqlServiceId);
    final KafkaTopicClient kafkaTopicClient = ksqlEngine.getTopicClient();
    try {
      ExecutorUtil.executeWithRetries(
          () -> kafkaTopicClient.deleteTopics(
              Collections.singletonList(commandTopic)),
          ExecutorUtil.RetryBehaviour.ALWAYS);
    } catch (final Exception e) {
      throw new KsqlException("Could not delete the command topic: "
          + commandTopic, e);
    }
  }

  private List<Pattern> getDeleteTopicPatterns(final List<String> deleteTopicList) {
    return deleteTopicList
        .stream()
        .map(Pattern::compile)
        .collect(Collectors.toList());
  }

}
