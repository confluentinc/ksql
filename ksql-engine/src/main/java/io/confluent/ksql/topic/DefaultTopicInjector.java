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
import java.util.Objects;
import java.util.Optional;
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
    this.topicClient = Objects.requireNonNull(topicClient, "topicClient");
    this.metaStore = Objects.requireNonNull(metaStore, "metaStore");
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

    final PreparedStatement<? extends CreateAsSelect> cas =
        (PreparedStatement<? extends CreateAsSelect>) statement;

    final TopicDescription source = describeSource(topicClient, ksqlConfig, cas);
    final TopicProperties info = new TopicProperties.Builder()
        .withName(ksqlConfig.getString(KsqlConfig.KSQL_OUTPUT_TOPIC_NAME_PREFIX_CONFIG)
            + cas.getStatement().getName().getSuffix())
        .withWithClause(cas.getStatement().getProperties())
        .withOverrides(propertyOverrides)
        .withLegacyKsqlConfig(ksqlConfig)
        .withSource(source)
        .build();

    final Map<String, Expression> properties = new HashMap<>(cas.getStatement().getProperties());
    properties.put(DdlConfig.KAFKA_TOPIC_NAME_PROPERTY, new StringLiteral(info.topicName));
    properties.put(KsqlConstants.SINK_NUMBER_OF_REPLICAS, new IntegerLiteral(info.replicas));
    properties.put(KsqlConstants.SINK_NUMBER_OF_PARTITIONS, new IntegerLiteral(info.partitions));

    final CreateAsSelect withTopic = cas.getStatement().copyWith(properties);
    final String withTopicText = SqlFormatter.formatSql(withTopic) + ";";

    return (PreparedStatement<T>) PreparedStatement.of(withTopicText, withTopic);
  }

  private TopicDescription describeSource(
      final KafkaTopicClient topicClient,
      final KsqlConfig ksqlConfig,
      final PreparedStatement<? extends CreateAsSelect> cas
  ) {
    final Analysis analysis = new QueryAnalyzer(
        metaStore,
        ksqlConfig.getString(KsqlConfig.KSQL_OUTPUT_TOPIC_NAME_PREFIX_CONFIG))
        .analyze(
            cas.getStatementText(),
            cas.getStatement().getQuery(),
            Optional.of(cas.getStatement().getSink()));

    final StructuredDataSource theSource = analysis.getTheSource();
    final String kafkaTopicName = theSource.getKsqlTopic().getKafkaTopicName();
    return topicClient.describeTopic(kafkaTopicName);
  }
}
