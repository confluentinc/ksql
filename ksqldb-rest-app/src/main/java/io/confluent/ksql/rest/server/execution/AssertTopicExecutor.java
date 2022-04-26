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

import static io.confluent.ksql.rest.Errors.assertionFailedError;
import static io.confluent.ksql.util.KsqlConfig.KSQL_ASSERT_TOPIC_DEFAULT_TIMEOUT_MS;

import io.confluent.ksql.KsqlExecutionContext;
import io.confluent.ksql.execution.expression.tree.Literal;
import io.confluent.ksql.parser.tree.AssertTopic;
import io.confluent.ksql.rest.SessionProperties;
import io.confluent.ksql.rest.entity.AssertTopicEntity;
import io.confluent.ksql.rest.server.resources.KsqlRestException;
import io.confluent.ksql.services.KafkaTopicClient;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.RetryUtil;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public final class AssertTopicExecutor {

  private static final int RETRY_MS = 100;

  private AssertTopicExecutor() {

  }

  public static StatementExecutorResponse execute(
      final ConfiguredStatement<AssertTopic> statement,
      final SessionProperties sessionProperties,
      final KsqlExecutionContext executionContext,
      final ServiceContext serviceContext
  ) {
    final KafkaTopicClient client = serviceContext.getTopicClient();
    final int timeout = statement.getStatement().getTimeout().isPresent()
        ? (int) statement.getStatement().getTimeout().get().toDuration().toMillis()
        : executionContext.getKsqlConfig().getInt(KSQL_ASSERT_TOPIC_DEFAULT_TIMEOUT_MS);
    try {
      RetryUtil.retryWithBackoff(
          timeout/RETRY_MS,
          RETRY_MS,
          RETRY_MS,
          () -> assertTopic(
              client, statement.getStatement().getTopic(), statement.getStatement().getConfig())
      );
    } catch (final KsqlException e) {
      throw new KsqlRestException(assertionFailedError(e.getMessage()));
    }
    return StatementExecutorResponse.handled(Optional.of(
        new AssertTopicEntity(statement.getStatementText(), statement.getStatement().getTopic())));
  }

  private static void assertTopic(
      final KafkaTopicClient client,
      final String topic,
      final Map<String, Literal> config
  ) {
    boolean topicExists;
    try {
      topicExists = client.isTopicExists(topic);
    } catch (final Exception e) {
      throw new KsqlException("Cannot check that topic exists: " + e.getMessage());
    }

    if(topicExists) {
      final int partitions = client.describeTopic(topic).partitions().size();
      final int replicas = client.describeTopic(topic).partitions().get(0).replicas().size();
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
    } else {
      throw new KsqlException("Topic " + topic + " does not exist");
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
