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

package io.confluent.ksql.topic;

import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.naming.SourceTopicNamingStrategy;
import io.confluent.ksql.parser.properties.with.CreateSourceProperties;
import io.confluent.ksql.services.KafkaTopicClient;
import io.confluent.ksql.util.KsqlConfig;
import java.util.Set;

/**
 * Helper methods around topic naming
 */
public final class TopicNaming {

  private TopicNaming() {
  }

  /**
   * Get the topic name for a source building built.
   *
   * <p>Topic name can be explicitly provided by the user in the statement, or can be obtained
   * from the registered {@link SourceTopicNamingStrategy}.
   *
   * @param sourceName the name of the source being created.
   * @param properties the properties of the source being created.
   * @param ksqlConfig the system config to use.
   * @param topicClient the topic client to use.
   * @return the name of the source's Kafka topic.
   */
  public static String getSourceTopicName(
      final SourceName sourceName,
      final CreateSourceProperties properties,
      final KsqlConfig ksqlConfig,
      final KafkaTopicClient topicClient
  ) {
    return properties.getKafkaTopic()
        .orElseGet(() -> findMatchingTopic(sourceName, ksqlConfig, topicClient));
  }

  private static String findMatchingTopic(
      final SourceName sourceName,
      final KsqlConfig ksqlConfig,
      final KafkaTopicClient topicClient
  ) {
    final SourceTopicNamingStrategy resolver = ksqlConfig.getConfiguredInstance(
        KsqlConfig.KSQL_SOURCE_TOPIC_NAMING_STRATEGY_CONFIG,
        SourceTopicNamingStrategy.class
    );

    final Set<String> topicNames = topicClient.listTopicNames();
    return resolver.resolveExistingTopic(sourceName, topicNames);
  }
}
