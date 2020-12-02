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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import io.confluent.ksql.config.SessionConfig;
import io.confluent.ksql.services.KafkaTopicClient;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.QueryApplicationId;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OrphanedTransientQueryCleaner {

  private static final Logger LOG = LoggerFactory.getLogger(OrphanedTransientQueryCleaner.class);

  private static Pattern TRANSIENT_QUERY_APP_ID = Pattern.compile("(.*\\d+_\\d+)(-[a-zA-Z]+)+");

  private final QueryCleanupService cleanupService;

  public OrphanedTransientQueryCleaner(final QueryCleanupService cleanupService) {
    this.cleanupService = requireNonNull(cleanupService);
  }

  public void cleanupOrphanedInternalTopics(
      final ServiceContext serviceContext,
      final SessionConfig config
  ) {
    final KsqlConfig ksqlConfig = config.getConfig(false);
    final String nodeId = ksqlConfig.getString(KsqlConfig.KSQL_NODE_ID_CONFIG);
    if (Strings.isNullOrEmpty(nodeId)) {
      // If no node id is set, there are no internal topics to clean up
      return;
    }
    final KafkaTopicClient topicClient = serviceContext.getTopicClient();
    final Set<String> topicNames = topicClient.listTopicNames();
    // Find any transient query topics
    final String queryApplicationIdPrefix = QueryApplicationId.buildPrefix(ksqlConfig, false);
    final List<String> orphanedTopics = topicNames.stream()
        .filter(topicName -> topicName.startsWith(queryApplicationIdPrefix))
        .collect(Collectors.toList());
    final Set<String> queryApplicationIds = orphanedTopics.stream()
        .map(topicName -> {
          final Optional<String> queryApplicationId
              = parseTransientQueryApplicationIdFromTopicName(topicName);
          if (!queryApplicationId.isPresent()) {
            LOG.error("Couldn't parse application id from topic " + topicName);
          }
          return queryApplicationId;
        })
        .filter(Optional::isPresent)
        .map(Optional::get)
        .collect(Collectors.toSet());
    for (final String queryApplicationId : queryApplicationIds) {
      cleanupService.addCleanupTask(
          new QueryCleanupService.QueryCleanupTask(
              serviceContext,
              queryApplicationId,
              true
          ));
    }
  }

  @VisibleForTesting
  static Optional<String> parseTransientQueryApplicationIdFromTopicName(final String topicName) {
    final Matcher matcher = TRANSIENT_QUERY_APP_ID.matcher(topicName);
    if (matcher.matches()) {
      return Optional.of(matcher.group(1));
    }
    return Optional.empty();
  }
}
