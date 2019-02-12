/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.ksql.util;

import io.confluent.ksql.ddl.DdlConfig;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.parser.SqlFormatter;
import io.confluent.ksql.parser.tree.CreateAsSelect;
import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.parser.tree.IntegerLiteral;
import io.confluent.ksql.parser.tree.StringLiteral;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.commons.lang3.StringUtils;

public final class StatementUtil {

  private StatementUtil() {
  }

  /**
   * Resolves all topic properties from the statement so that topic name, number partitions
   * and number of replicas are explicitly specified.
   *
   * @return a prepared statement with the resolved properties and updated statement text
   */
  public static PreparedStatement withInferredSinkTopic(
      final PreparedStatement statement,
      final Map<String, Object> propertyOverrides,
      final KsqlConfig ksqlConfig
  ) {
    if (!(statement.getStatement() instanceof CreateAsSelect)) {
      return statement;
    }

    final CreateAsSelect cas = (CreateAsSelect) statement.getStatement();

    final String topic = topicName(cas, ksqlConfig);
    final int partitions = numPartitions(cas, ksqlConfig, propertyOverrides);
    final short replicas = numReplicas(cas, ksqlConfig, propertyOverrides);

    final Map<String, Expression> properties = new HashMap<>(cas.getProperties());
    properties.put(DdlConfig.KAFKA_TOPIC_NAME_PROPERTY, new StringLiteral(topic));
    properties.put(KsqlConstants.SINK_NUMBER_OF_REPLICAS, new IntegerLiteral(replicas));
    properties.put(KsqlConstants.SINK_NUMBER_OF_PARTITIONS, new IntegerLiteral(partitions));

    final CreateAsSelect withTopic = cas.copyWith(properties);
    final String withTopicText = SqlFormatter.formatSql(withTopic) + ";";

    return PreparedStatement.of(withTopicText, withTopic);
  }

  /**
   * If the topic name is specified in the request, use that topic name directly, otherwise
   * construct it using the default topic prefix and the name of the output source.
   */
  private static String topicName(final CreateAsSelect cas, final KsqlConfig cfg) {
    final Expression topicProperty = cas.getProperties().get(DdlConfig.KAFKA_TOPIC_NAME_PROPERTY);
    return (topicProperty == null)
        ? cfg.getString(KsqlConfig.KSQL_OUTPUT_TOPIC_NAME_PREFIX_CONFIG) + cas.getName().getSuffix()
        : StringUtils.strip(topicProperty.toString(), "'");
  }

  private static int numPartitions(
      final CreateAsSelect cas,
      final KsqlConfig cfg,
      final Map<String, Object> propertyOverrides
  ) {
    final Expression partitions = cas.getProperties().get(KsqlConstants.SINK_NUMBER_OF_PARTITIONS);
    if (partitions != null) {
      return Integer.parseInt(partitions.toString());
    }

    final Object override = propertyOverrides.get(KsqlConfig.SINK_NUMBER_OF_PARTITIONS_PROPERTY);
    return Optional.ofNullable(override)
        .map(Object::toString)
        .map(Integer::parseInt)
        .orElseGet(() -> cfg.getInt(KsqlConfig.SINK_NUMBER_OF_PARTITIONS_PROPERTY));
  }

  private static short numReplicas(
      final CreateAsSelect cas,
      final KsqlConfig cfg,
      final Map<String, Object> propertyOverrides
  ) {
    final Expression replicas = cas.getProperties().get(KsqlConstants.SINK_NUMBER_OF_REPLICAS);
    if (replicas != null) {
      return Short.parseShort(replicas.toString());
    }

    final Object override = propertyOverrides.get(KsqlConfig.SINK_NUMBER_OF_REPLICAS_PROPERTY);
    return Optional.ofNullable(override)
        .map(Object::toString)
        .map(Short::parseShort)
        .orElseGet(() -> cfg.getShort(KsqlConfig.SINK_NUMBER_OF_REPLICAS_PROPERTY));
  }
}
