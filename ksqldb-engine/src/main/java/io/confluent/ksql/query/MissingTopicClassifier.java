/*
 * Copyright 2020 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"; you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.query;

import io.confluent.ksql.query.QueryError.Type;
import io.confluent.ksql.services.KafkaTopicClient;
import java.util.Objects;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@code MissingTopicClassifier} classifies errors by querying the broker
 * to check that all topics that the query relies on being accessible exist
 * and are accessible
 */
public class MissingTopicClassifier implements QueryErrorClassifier {

  private static final Logger LOG = LoggerFactory.getLogger(MissingTopicClassifier.class);

  private final Set<String> requiredTopics;
  private final KafkaTopicClient topicClient;
  private final String queryId;

  public MissingTopicClassifier(
      final String queryId,
      final Set<String> requiredTopics,
      final KafkaTopicClient topicClient
  ) {
    this.queryId = Objects.requireNonNull(queryId, "queryId");
    this.requiredTopics = Objects.requireNonNull(requiredTopics, "requiredTopics");
    this.topicClient = Objects.requireNonNull(topicClient, "topicClient");
    LOG.info("Query {} requires topics {}", queryId, requiredTopics);
  }

  @Override
  public Type classify(final Throwable e) {
    LOG.info(
        "Attempting to classify missing topic error. Query ID: {} Required topics: {}",
        queryId,
        requiredTopics
    );

    for (String requiredTopic : requiredTopics) {
      if (!topicClient.isTopicExists(requiredTopic)) {
        LOG.warn("Query {} requires topic {} which cannot be found.", queryId, requiredTopic);
        return Type.USER;
      }
    }

    return Type.UNKNOWN;
  }

}
