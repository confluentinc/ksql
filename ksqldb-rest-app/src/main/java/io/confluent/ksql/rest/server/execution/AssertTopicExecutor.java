/*
 * Copyright 2019 Confluent Inc.
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

package io.confluent.ksql.rest.server.execution;

import static io.confluent.ksql.util.KsqlConfig.KSQL_ASSERT_TOPIC_DEFAULT_TIMEOUT_MS;

import io.confluent.ksql.KsqlExecutionContext;
import io.confluent.ksql.execution.expression.tree.Literal;
import io.confluent.ksql.parser.tree.AssertTopic;
import io.confluent.ksql.rest.SessionProperties;
import io.confluent.ksql.rest.entity.AssertTopicEntity;
import io.confluent.ksql.services.KafkaTopicClient;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.util.KsqlException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.TopicPartitionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class AssertTopicExecutor {

  private static final Logger LOG = LoggerFactory.getLogger(AssertTopicExecutor.class);

  private AssertTopicExecutor() {

  }

  public static StatementExecutorResponse execute(
      final ConfiguredStatement<AssertTopic> statement,
      final SessionProperties sessionProperties,
      final KsqlExecutionContext executionContext,
      final ServiceContext serviceContext
  ) {
    return AssertExecutor.execute(statement.getMaskedStatementText(),
        statement.getStatement(),
        executionContext.getKsqlConfig().getInt(KSQL_ASSERT_TOPIC_DEFAULT_TIMEOUT_MS),
        serviceContext,
        (stmt, sc) -> assertTopic(
            sc.getTopicClient(),
            ((AssertTopic) stmt).getTopic(),
            ((AssertTopic) stmt).getConfig(),
            stmt.checkExists()),
        (str, stmt) -> new AssertTopicEntity(
            str,
            ((AssertTopic) stmt).getTopic(),
            stmt.checkExists())
    );
  }

  private static void assertTopic(
      final KafkaTopicClient client,
      final String topic,
      final Map<String, Literal> config,
      final boolean assertExists
  ) {
    final boolean topicExists;
    try {
      topicExists = client.isTopicExists(topic);
    } catch (final Exception e) {
      throw new KsqlException("Cannot check topic existence: " + e.getMessage());
    }

    if (!assertExists) {
      if (config.size() > 0) {
        LOG.warn("Will skip topic config check for topic non-existence assertion.");
      }
      if (topicExists) {
        throw new KsqlException("Topic " + topic + " exists");
      }
    } else {
      if (topicExists) {
        final List<TopicPartitionInfo> partitionList = client.describeTopic(topic).partitions();
        checkConfigs(topic, config, partitionList.size(), partitionList.get(0).replicas().size());
      } else {
        throw new KsqlException("Topic " + topic + " does not exist");
      }
    }
  }

  private static void checkConfigs(
      final String topic,
      final Map<String, Literal> config,
      final int partitions,
      final int replicas
  ) {
    final List<String> configErrors = new ArrayList<>();
    config.forEach((k, v) -> {
      if (k.toLowerCase().equals("partitions")) {
        if (!configMatches(v.getValue(), partitions)) {
          configErrors.add(
              createConfigError(topic, "partitions", v.getValue().toString(), partitions));
        }
      } else if (k.toLowerCase().equals("replicas")) {
        if (!configMatches(v.getValue(), replicas)) {
          configErrors.add(
              createConfigError(topic, "replicas", v.getValue().toString(), replicas));
        }
      } else {
        configErrors.add("Cannot assert unknown topic property: " + k);
      }
    });
    if (configErrors.size() > 0) {
      throw new KsqlException(String.join("\n", configErrors));
    }
  }

  private static boolean configMatches(final Object expected, final int actual) {
    if (expected instanceof Integer && (Integer) expected == actual) {
      return true;
    }
    return false;
  }

  private static String createConfigError(
      final String topic, final String config, final String expected, final int actual) {
    return String.format(
        "Mismatched configuration for topic %s: For config %s, expected %s got %d",
        topic, config, expected, actual);
  }
}
