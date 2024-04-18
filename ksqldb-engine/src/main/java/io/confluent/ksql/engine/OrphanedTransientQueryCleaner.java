/*
 * Copyright 2020 Confluent Inc.
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

package io.confluent.ksql.engine;

import static java.util.Objects.requireNonNull;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.exception.KafkaResponseGetFailedException;
import io.confluent.ksql.services.KafkaTopicClient;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.util.KsqlConfig;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.kafka.streams.StreamsConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OrphanedTransientQueryCleaner {

  private static final Logger LOG = LoggerFactory.getLogger(OrphanedTransientQueryCleaner.class);

  private final QueryCleanupService cleanupService;
  private final KsqlConfig ksqlConfig;

  @SuppressFBWarnings(value = "EI_EXPOSE_REP")
  public OrphanedTransientQueryCleaner(final QueryCleanupService cleanupService,
                                       final KsqlConfig ksqlConfig) {
    this.cleanupService = requireNonNull(cleanupService);
    this.ksqlConfig = ksqlConfig;
  }

  /**
   * Cleans up any internal topics that may exist for the given set of query application
   * ids, since it's assumed that they are completed.
   * @param serviceContext The service context
   * @param queryApplicationIds The set of completed query application ids
   */
  public void cleanupOrphanedInternalTopics(
      final ServiceContext serviceContext,
      final Set<String> queryApplicationIds
  ) {
    final KafkaTopicClient topicClient = serviceContext.getTopicClient();
    final Set<String> topicNames;
    try {
      topicNames = topicClient.listTopicNames();
    } catch (KafkaResponseGetFailedException e) {
      LOG.error("Couldn't fetch topic names", e);
      return;
    }
    // Find any transient query topics
    final Set<String> orphanedQueryApplicationIds = topicNames.stream()
        .map(topicName -> queryApplicationIds.stream().filter(topicName::startsWith).findFirst())
        .filter(Optional::isPresent)
        .map(Optional::get)
        .collect(Collectors.toSet());
    for (final String queryApplicationId : orphanedQueryApplicationIds) {
      cleanupService.addCleanupTask(
          new QueryCleanupService.QueryCleanupTask(
              serviceContext,
              queryApplicationId,
              Optional.empty(),
              true,
              ksqlConfig.getKsqlStreamConfigProps()
                  .getOrDefault(
                      StreamsConfig.STATE_DIR_CONFIG,
                      StreamsConfig.configDef().defaultValues().get(StreamsConfig.STATE_DIR_CONFIG))
                  .toString(),
              ksqlConfig.getString(KsqlConfig.KSQL_SERVICE_ID_CONFIG),
              ksqlConfig.getString(KsqlConfig.KSQL_PERSISTENT_QUERY_NAME_PREFIX_CONFIG)));
    }
  }
}
