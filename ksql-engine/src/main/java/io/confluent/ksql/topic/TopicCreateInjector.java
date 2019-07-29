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
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.parser.SqlFormatter;
import io.confluent.ksql.parser.properties.with.CreateSourceAsProperties;
import io.confluent.ksql.parser.properties.with.CreateSourceProperties;
import io.confluent.ksql.parser.tree.CreateAsSelect;
import io.confluent.ksql.parser.tree.CreateSource;
import io.confluent.ksql.parser.tree.CreateTable;
import io.confluent.ksql.parser.tree.CreateTableAsSelect;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.properties.with.CommonCreateConfigs;
import io.confluent.ksql.services.KafkaTopicClient;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.statement.Injector;
import io.confluent.ksql.topic.TopicProperties.Builder;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import org.apache.kafka.common.config.TopicConfig;

/**
 * An injector which injects the topic name, number of partitions and number of
 * replicas into the topic properties of the supplied {@code statement}.
 *
 * <p>If a statement that is not {@code CreateAsSelect} or {@code CreateSource }
 * is passed in, this results in a no-op that returns the incoming statement.</p>
 *
 * @see TopicProperties.Builder
 */
public class TopicCreateInjector implements Injector {

  private final KafkaTopicClient topicClient;
  private final MetaStore metaStore;

  public TopicCreateInjector(
      final KsqlExecutionContext executionContext,
      final ServiceContext serviceContext
  ) {
    this(serviceContext.getTopicClient(), executionContext.getMetaStore());
  }

  TopicCreateInjector(
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
      final TopicProperties.Builder topicPropertiesBuilder
  ) {
    if (statement.getStatement() instanceof CreateAsSelect) {
      return (ConfiguredStatement<T>) injectForCreateAsSelect(
          (ConfiguredStatement<? extends CreateAsSelect>) statement,
          topicPropertiesBuilder);
    }

    if (statement.getStatement() instanceof CreateSource) {
      return (ConfiguredStatement<T>) injectForCreateSource(
          (ConfiguredStatement<? extends CreateSource>) statement,
          topicPropertiesBuilder);
    }

    return statement;
  }

  private ConfiguredStatement<? extends CreateSource> injectForCreateSource(
      final ConfiguredStatement<? extends CreateSource> statement,
      final TopicProperties.Builder topicPropertiesBuilder
  ) {
    final CreateSource createSource = statement.getStatement();
    final CreateSourceProperties properties = createSource.getProperties();

    final String topicName = properties.getKafkaTopic();

    if (topicClient.isTopicExists(topicName)) {
      topicPropertiesBuilder.withSource(() -> topicClient.describeTopic(topicName));
    } else if (!properties.getPartitions().isPresent()) {
      final CreateSource example = createSource.copyWith(
          createSource.getElements(),
          properties.withPartitionsAndReplicas(2, (short) 1));
      throw new KsqlException(
          "Topic '" + topicName + "' does not exist. If you want to create a new topic for the "
              + "stream/table please re-run the statement providing the required '"
              + CommonCreateConfigs.SOURCE_NUMBER_OF_PARTITIONS + "' configuration in the WITH "
              + "clause (and optionally '" + CommonCreateConfigs.SOURCE_NUMBER_OF_REPLICAS + "'). "
              + "For example: " + SqlFormatter.formatSql(example));
    }

    topicPropertiesBuilder
        .withName(topicName)
        .withWithClause(
            Optional.of(properties.getKafkaTopic()),
            properties.getPartitions(),
            properties.getReplicas());

    createTopic(topicPropertiesBuilder, statement, createSource instanceof CreateTable);

    return statement;
  }

  @SuppressWarnings("unchecked")
  private <T extends CreateAsSelect> ConfiguredStatement<?> injectForCreateAsSelect(
      final ConfiguredStatement<T> statement,
      final TopicProperties.Builder topicPropertiesBuilder
  ) {
    final String prefix =
        statement.getOverrides().getOrDefault(
            KsqlConfig.KSQL_OUTPUT_TOPIC_NAME_PREFIX_CONFIG,
            statement.getConfig().getString(KsqlConfig.KSQL_OUTPUT_TOPIC_NAME_PREFIX_CONFIG))
            .toString();

    final T createAsSelect = statement.getStatement();
    final CreateSourceAsProperties properties = createAsSelect.getProperties();

    final SourceTopicsExtractor extractor = new SourceTopicsExtractor(metaStore);
    extractor.process(statement.getStatement().getQuery(), null);
    final String sourceTopicName = extractor.getPrimaryKafkaTopicName();

    topicPropertiesBuilder
        .withName(prefix + createAsSelect.getName().getSuffix())
        .withSource(() -> topicClient.describeTopic(sourceTopicName))
        .withWithClause(
            properties.getKafkaTopic(),
            properties.getPartitions(),
            properties.getReplicas());

    final boolean shouldCompactTopic = createAsSelect instanceof CreateTableAsSelect
        && !createAsSelect.getQuery().getWindow().isPresent();

    final TopicProperties info = createTopic(topicPropertiesBuilder, statement, shouldCompactTopic);

    final T withTopic = (T) createAsSelect.copyWith(properties.withTopic(
        info.getTopicName(),
        info.getPartitions(),
        info.getReplicas()
    ));

    final String withTopicText = SqlFormatter.formatSql(withTopic) + ";";

    return statement.withStatement(withTopicText, withTopic);
  }

  private TopicProperties createTopic(
      final Builder topicPropertiesBuilder,
      final ConfiguredStatement<?> statement,
      final boolean shouldCompactTopic
  ) {
    final TopicProperties info = topicPropertiesBuilder
        .withOverrides(statement.getOverrides())
        .withKsqlConfig(statement.getConfig())
        .build();

    final Map<String, ?> config = shouldCompactTopic
        ? ImmutableMap.of(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT)
        : Collections.emptyMap();

    topicClient.createTopic(info.getTopicName(), info.getPartitions(), info.getReplicas(), config);

    return info;
  }
}
