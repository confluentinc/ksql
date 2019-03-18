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
import io.confluent.ksql.util.KsqlConfig;
import java.util.Map;

/**
 * Type for injecting the topic into the WITH clause for statements that have
 * none or incomplete topic property information.
 */
public interface TopicInjector {

  /**
   * Attempt to inject topic name, number of partitions and number of replicas into the topic
   * properties of the supplied {@code statement}.
   *
   * The following precedence order is maintained for deriving properties:
   *
   * <ul>
   *   <li>The statement itself, if it has a WITH clause</li>
   *   <li>The property overrides, if present (note that this is a legacy approach)</li>
   *   <li>The KsqlConfig property, if present (note that this is a legacy approach)</li>
   *   <li>The topic properties from the source that it is reading from. If the source is a join,
   *   then the left value is used as the source.</li>
   *   <li>Generated based on some recipe - this is the case for topic name, which will never
   *   use the source topic (obviously!)</li>
   * </ul>
   *
   * <p>It is possible that only partial information exists at higher levels of precedence. If
   * this is the case, the values will be inferred in cascading fashion (e.g. topic name from
   * WITH clause, replicas from property overrides and partitions source topic).</p>
   *
   * <p>If a statement that is not {@code CreateAsSelect} is passed in, this results in a
   * no-op tha returns the incoming statement.</p>
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
