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
import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.KsqlExecutionContext;
import io.confluent.ksql.ddl.DdlConfig;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.model.StructuredDataSource;
import io.confluent.ksql.parser.DefaultTraversalVisitor;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.parser.SqlFormatter;
import io.confluent.ksql.parser.tree.AliasedRelation;
import io.confluent.ksql.parser.tree.CreateAsSelect;
import io.confluent.ksql.parser.tree.CreateTableAsSelect;
import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.parser.tree.IntegerLiteral;
import io.confluent.ksql.parser.tree.Join;
import io.confluent.ksql.parser.tree.Node;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.parser.tree.StringLiteral;
import io.confluent.ksql.parser.tree.Table;
import io.confluent.ksql.services.KafkaTopicClient;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.statement.Injector;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlConstants;
import io.confluent.ksql.util.KsqlException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.config.TopicConfig;

public class DefaultTopicInjector implements Injector {

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
  public <T extends Statement> ConfiguredStatement<T> inject(
      final ConfiguredStatement<T> statement
  ) {
    return inject(statement, new TopicProperties.Builder());
  }

  @SuppressWarnings("unchecked")
  @VisibleForTesting
  <T extends Statement> ConfiguredStatement<T> inject(
      final ConfiguredStatement<T> statement,
      final TopicProperties.Builder topicPropertiesBuilder) {
    if (!(statement.getStatement() instanceof CreateAsSelect)) {
      return statement;
    }

    final ConfiguredStatement<? extends CreateAsSelect> cas =
        (ConfiguredStatement<? extends CreateAsSelect>) statement;

    final String prefix =
        cas.getOverrides().getOrDefault(
            KsqlConfig.KSQL_OUTPUT_TOPIC_NAME_PREFIX_CONFIG,
            cas.getConfig().getString(KsqlConfig.KSQL_OUTPUT_TOPIC_NAME_PREFIX_CONFIG)).toString();

    final TopicProperties info = topicPropertiesBuilder
        .withName(prefix + cas.getStatement().getName().getSuffix())
        .withWithClause(cas.getStatement().getProperties())
        .withOverrides(cas.getOverrides())
        .withKsqlConfig(cas.getConfig())
        .withSource(() -> describeSource(topicClient, cas))
        .build();

    final boolean shouldCompactTopic = statement.getStatement() instanceof CreateTableAsSelect
        && !((CreateAsSelect) statement.getStatement()).getQuery().getWindow().isPresent();
    final Map<String, ?> config = shouldCompactTopic
        ? ImmutableMap.of(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT)
        : Collections.emptyMap();
    topicClient.createTopic(info.getTopicName(), info.getPartitions(), info.getReplicas(), config);

    final Map<String, Expression> props = new HashMap<>(cas.getStatement().getProperties());
    props.put(DdlConfig.KAFKA_TOPIC_NAME_PROPERTY, new StringLiteral(info.getTopicName()));
    props.put(KsqlConstants.SINK_NUMBER_OF_REPLICAS, new IntegerLiteral(info.getReplicas()));
    props.put(KsqlConstants.SINK_NUMBER_OF_PARTITIONS, new IntegerLiteral(info.getPartitions()));

    final CreateAsSelect withTopic = cas.getStatement().copyWith(props);
    final String withTopicText = SqlFormatter.formatSql(withTopic) + ";";

    return (ConfiguredStatement<T>) ConfiguredStatement.of(
        PreparedStatement.of(withTopicText, withTopic),
        cas.getOverrides(),
        cas.getConfig());
  }

  private TopicDescription describeSource(
      final KafkaTopicClient topicClient,
      final ConfiguredStatement<? extends CreateAsSelect> cas
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
      final StructuredDataSource<?> source = metaStore.getSource(structuredDataSourceName);
      if (source == null) {
        throw new KsqlException(structuredDataSourceName + " does not exist.");
      }

      primaryKafkaTopicName = source.getKsqlTopic().getKafkaTopicName();
      return node;
    }
  }
}
