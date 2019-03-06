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

import io.confluent.ksql.KsqlExecutionContext;
import io.confluent.ksql.analyzer.Analysis;
import io.confluent.ksql.analyzer.QueryAnalyzer;
import io.confluent.ksql.ddl.DdlConfig;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.StructuredDataSource;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.parser.SqlFormatter;
import io.confluent.ksql.parser.tree.CreateAsSelect;
import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.parser.tree.IntegerLiteral;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.parser.tree.StringLiteral;
import io.confluent.ksql.services.KafkaTopicClient;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlConstants;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.admin.TopicDescription;

public class DefaultTopicInjector implements TopicInjector {

  private final KafkaTopicClient topicClient;
  private final MetaStore metaStore;

  public DefaultTopicInjector(
      final KsqlExecutionContext executionContext
  ) {
    this(executionContext.getServiceContext().getTopicClient(), executionContext.getMetaStore());
  }

  DefaultTopicInjector(
      final KafkaTopicClient topicClient,
      final MetaStore metaStore) {

    this.topicClient = topicClient;
    this.metaStore = metaStore;
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T extends Statement> PreparedStatement<T> forStatement(
      final PreparedStatement<T> statement,
      final KsqlConfig ksqlConfig,
      final Map<String, Object> propertyOverrides) {
    if (!(statement.getStatement() instanceof CreateAsSelect)) {
      return statement;
    }

    final CreateAsSelect cas = (CreateAsSelect) statement.getStatement();

    final String topic = topicName(cas, ksqlConfig);

    final TopicDescription description = describeSource(cas, metaStore, ksqlConfig, topicClient);
    final int partitions = numPartitions(cas, ksqlConfig, propertyOverrides, description);
    final short replicas = numReplicas(cas, ksqlConfig, propertyOverrides, description);

    final Map<String, Expression> properties = new HashMap<>(cas.getProperties());
    properties.put(DdlConfig.KAFKA_TOPIC_NAME_PROPERTY, new StringLiteral(topic));
    properties.put(KsqlConstants.SINK_NUMBER_OF_REPLICAS, new IntegerLiteral(replicas));
    properties.put(KsqlConstants.SINK_NUMBER_OF_PARTITIONS, new IntegerLiteral(partitions));

    final CreateAsSelect withTopic = cas.copyWith(properties);
    final String withTopicText = SqlFormatter.formatSql(withTopic) + ";";

    return (PreparedStatement<T>) PreparedStatement.of(withTopicText, withTopic);
  }

  private static TopicDescription describeSource(
      final CreateAsSelect cas,
      final MetaStore metaStore,
      final KsqlConfig ksqlConfig,
      final KafkaTopicClient topicClient
  ) {
    final Analysis analysis = new QueryAnalyzer(
        metaStore,
        ksqlConfig.getString(KsqlConfig.KSQL_OUTPUT_TOPIC_NAME_PREFIX_CONFIG))
        .analyze(SqlFormatter.formatSql(cas), cas.getQuery());
    final StructuredDataSource theSource = analysis.getTheSourceNode();

    final String kafkaTopicName = theSource.getKsqlTopic().getKafkaTopicName();
    return topicClient.describeTopic(kafkaTopicName);
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
      final Map<String, Object> propertyOverrides,
      final TopicDescription description) {
    final Expression partitions = cas.getProperties().get(KsqlConstants.SINK_NUMBER_OF_PARTITIONS);
    if (partitions != null) {
      return Integer.parseInt(partitions.toString());
    }

    final Object override = propertyOverrides.get(KsqlConfig.SINK_NUMBER_OF_PARTITIONS_PROPERTY);
    final Integer legacyNumPartitions = cfg.getInt(KsqlConfig.SINK_NUMBER_OF_PARTITIONS_PROPERTY);

    return Optional.ofNullable(override)
        .map(Object::toString)
        .map(Integer::parseInt)
        .orElseGet(() -> ObjectUtils.defaultIfNull(
            legacyNumPartitions,
            description.partitions().size()));
  }

  private static short numReplicas(
      final CreateAsSelect cas,
      final KsqlConfig cfg,
      final Map<String, Object> propertyOverrides,
      final TopicDescription description) {
    final Expression replicas = cas.getProperties().get(KsqlConstants.SINK_NUMBER_OF_REPLICAS);
    if (replicas != null) {
      return Short.parseShort(replicas.toString());
    }

    final Object override = propertyOverrides.get(KsqlConfig.SINK_NUMBER_OF_REPLICAS_PROPERTY);
    final Short legacyNumReplicas = cfg.getShort(KsqlConfig.SINK_NUMBER_OF_REPLICAS_PROPERTY);

    return Optional.ofNullable(override)
        .map(Object::toString)
        .map(Short::parseShort)
        .orElseGet(() -> ObjectUtils.defaultIfNull(
            legacyNumReplicas,
            (short) description.partitions().get(0).replicas().size()));
  }
}
