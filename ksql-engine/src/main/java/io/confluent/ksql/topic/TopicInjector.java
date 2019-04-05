/*
 * Copyright 2019 Confluent Inc.
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

package io.confluent.ksql.topic;

import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.statement.Injector;
import io.confluent.ksql.util.KsqlConfig;
import java.util.Map;

/**
 * Injects the topic into the WITH clause for statements that have
 * incomplete topic property information.
 */
public interface TopicInjector extends Injector {

  @Override
  default <T extends Statement> ConfiguredStatement<T> inject(
      final ConfiguredStatement<T> statement) {
    return ConfiguredStatement.of(
        forStatement(
            PreparedStatement.of(statement.getStatementText(), statement.getStatement()),
            statement.getConfig(),
            statement.getOverrides()),
        statement.getOverrides(),
        statement.getConfig());
  }

  /**
   * Attempt to inject topic name, number of partitions and number of replicas into the topic
   * properties of the supplied {@code statement}.

   * <p>If a statement that is not {@code CreateAsSelect} is passed in, this results in a
   * no-op that returns the incoming statement.</p>
   *
   * @see TopicProperties.Builder
   *
   * @param statement           the statement to inject the topic properties into
   * @param ksqlConfig          the default configurations for the service
   * @param propertyOverrides   the overrides for this statement
   * @param <T>                 the type of statement, will do nothing unless
   *                            {@code <T extends CreateAsSelect>}
   *
   * @return a statement that has the kafka topic properties injected
   */
  <T extends Statement> PreparedStatement<T> forStatement(
      PreparedStatement<T> statement,
      KsqlConfig ksqlConfig,
      Map<String, Object> propertyOverrides);
}
