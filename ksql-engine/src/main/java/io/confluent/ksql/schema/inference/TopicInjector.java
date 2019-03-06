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

package io.confluent.ksql.schema.inference;

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
   * Attempt to inject the sink kafka topic properties into the supplied {@code statement}. The
   * following conditions hold:
   *
   * <ul>
   *   <li>If the statement is not CTAS/CSAS, this operation does nothing</li>
   *   <li>If the statement does not have any kafka topic properties, then the topic name
   *       partitions and replica count will be injecetd. </li>
   *   <li>If the statement specifies a name, a name will not be injected</li>
   *   <li>If the statement specifies number partitions, number partitions are not injected</li>
   *   <li>If the statement specifies number replicas, number replicas are not injected</li>
   * </ul>
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
