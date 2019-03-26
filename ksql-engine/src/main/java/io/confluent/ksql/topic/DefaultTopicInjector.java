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

import com.google.common.annotations.VisibleForTesting;
import io.confluent.ksql.KsqlExecutionContext;
import io.confluent.ksql.ddl.DdlConfig;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.StructuredDataSource;
import io.confluent.ksql.parser.DefaultTraversalVisitor;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.parser.SqlFormatter;
import io.confluent.ksql.parser.tree.AliasedRelation;
import io.confluent.ksql.parser.tree.CreateAsSelect;
import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.parser.tree.IntegerLiteral;
import io.confluent.ksql.parser.tree.Join;
import io.confluent.ksql.parser.tree.Node;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.parser.tree.StringLiteral;
import io.confluent.ksql.parser.tree.Table;
import io.confluent.ksql.services.KafkaTopicClient;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlConstants;
import io.confluent.ksql.util.KsqlException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
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

  @Override
  public <T extends Statement> PreparedStatement<T> forStatement(
      final PreparedStatement<T> statement,
      final KsqlConfig ksqlConfig,
      final Map<String, Object> propertyOverrides) {
    return forStatement(statement, ksqlConfig, propertyOverrides, new TopicProperties.Builder());
  }

  @SuppressWarnings("unchecked")
  @VisibleForTesting
  <T extends Statement> PreparedStatement<T> forStatement(
      final PreparedStatement<T> statement,
      final KsqlConfig ksqlConfig,
      final Map<String, Object> propertyOverrides,
      final TopicProperties.Builder topicPropertiesBuilder) {
    if (!(statement.getStatement() instanceof CreateAsSelect)) {
      return statement;
    }

    final PreparedStatement<? extends CreateAsSelect> cas =
        (PreparedStatement<? extends CreateAsSelect>) statement;

    final TopicDescription source = describeSource(topicClient, cas);
    final TopicProperties info = topicPropertiesBuilder
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
      final PreparedStatement<? extends CreateAsSelect> cas
  ) {
    final SourceTopicExtractor extractor = new SourceTopicExtractor();
    extractor.process(cas.getStatement().getQuery(), null);
    final String kafkaTopicName = extractor.primaryKafkaTopicName;
    return topicClient.describeTopic(kafkaTopicName);
  }

  private final class SourceTopicExtractor extends DefaultTraversalVisitor<Node, Void> {

    private String primaryKafkaTopicName = null;

    @Override
    protected Node visitJoin(final Join node, final Void context) {
      process(node.getLeft(), context);
      return null;
    }

    @Override
    protected Node visitAliasedRelation(final AliasedRelation node, final Void context) {
      final String structuredDataSourceName = ((Table) node.getRelation()).getName().getSuffix();
      final StructuredDataSource source = metaStore.getSource(structuredDataSourceName);
      if (source == null) {
        throw new KsqlException(structuredDataSourceName + " does not exist.");
      }

      primaryKafkaTopicName = source.getKsqlTopic().getKafkaTopicName();
      return node;
    }
  }
}
