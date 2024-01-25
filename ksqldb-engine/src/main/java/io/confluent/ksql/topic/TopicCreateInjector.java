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
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.parser.KsqlParser;
import io.confluent.ksql.parser.SqlFormatter;
import io.confluent.ksql.parser.properties.with.CreateSourceAsProperties;
import io.confluent.ksql.parser.properties.with.CreateSourceProperties;
import io.confluent.ksql.parser.tree.CreateAsSelect;
import io.confluent.ksql.parser.tree.CreateSource;
import io.confluent.ksql.parser.tree.CreateStreamAsSelect;
import io.confluent.ksql.parser.tree.CreateTable;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.properties.with.CommonCreateConfigs;
import io.confluent.ksql.services.KafkaTopicClient;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.statement.InjectorWithSideEffects;
import io.confluent.ksql.topic.TopicProperties.Builder;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
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
public class TopicCreateInjector implements InjectorWithSideEffects {

  private static final String CLEANUP_POLICY_PRESENT_EXCEPTION =
      String.format(
          "Invalid config variable in the WITH clause: %s.%n"
              + "The %s config is automatically inferred based "
              + "on the type of source (STREAM or TABLE).%n"
              + "Users can't set the %s config manually.",
          CommonCreateConfigs.SOURCE_TOPIC_CLEANUP_POLICY,
          CommonCreateConfigs.SOURCE_TOPIC_CLEANUP_POLICY,
          CommonCreateConfigs.SOURCE_TOPIC_CLEANUP_POLICY);

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

  @VisibleForTesting
  <T extends Statement> ConfiguredStatement<T> inject(
      final ConfiguredStatement<T> statement,
      final TopicProperties.Builder topicPropertiesBuilder
  ) {
    return injectInternal(statement, topicPropertiesBuilder).getStatement();
  }

  @Override
  public <T extends Statement> ConfiguredStatementWithSideEffects<T> injectWithSideEffects(
          final ConfiguredStatement<T> statement
  ) {
    return injectInternal(statement, new TopicProperties.Builder());
  }

  @Override
  public <T extends Statement> void revertSideEffects(
          final ConfiguredStatementWithSideEffects<T> statement
  ) {
    final Collection<String> topicsToDelete = new ArrayList<>();
    for (Object e : statement.getSideEffects()) {
      if (e instanceof TopicCreationSideEffect) {
        topicsToDelete.add(((TopicCreationSideEffect) e).topicName);
      }
    }
    if (!topicsToDelete.isEmpty()) {
      topicClient.deleteTopics(topicsToDelete);
    }
  }

  @SuppressWarnings("unchecked")
  private <T extends Statement> ConfiguredStatementWithSideEffects<T> injectInternal(
      final ConfiguredStatement<T> statement,
      final TopicProperties.Builder topicPropertiesBuilder
  ) {
    if (statement.getStatement() instanceof CreateAsSelect) {
      return (ConfiguredStatementWithSideEffects<T>) injectForCreateAsSelect(
              (ConfiguredStatement<CreateAsSelect>) statement,
              topicPropertiesBuilder
      );
    }
    if (statement.getStatement() instanceof CreateSource) {
      return (ConfiguredStatementWithSideEffects<T>) injectForCreateSource(
          (ConfiguredStatement<? extends CreateSource>) statement,
          topicPropertiesBuilder);
    }

    return ConfiguredStatementWithSideEffects.withNoEffects(statement);
  }

  private ConfiguredStatementWithSideEffects<CreateSource> injectForCreateSource(
      final ConfiguredStatement<? extends CreateSource> statement,
      final TopicProperties.Builder topicPropertiesBuilder
  ) {
    final CreateSource createSource = statement.getStatement();
    final CreateSourceProperties properties = createSource.getProperties();

    if (properties.getCleanupPolicy().isPresent()) {
      throw new KsqlException(CLEANUP_POLICY_PRESENT_EXCEPTION);
    }

    final String topicCleanUpPolicy = createSource instanceof CreateTable
        ? TopicConfig.CLEANUP_POLICY_COMPACT : TopicConfig.CLEANUP_POLICY_DELETE;

    final String topicName = properties.getKafkaTopic();

    if (topicClient.isTopicExists(topicName)) {
      topicPropertiesBuilder
          .withSource(
              () -> topicClient.describeTopic(topicName),
              () -> topicClient.getTopicConfig(topicName));
    } else if (!properties.getPartitions().isPresent()) {
      final CreateSource example = createSource.copyWith(
          createSource.getElements(),
          properties.withPartitions(2));
      throw new KsqlException(
          "Topic '" + topicName + "' does not exist. If you want to create a new topic for the "
              + "stream/table please re-run the statement providing the required '"
              + CommonCreateConfigs.SOURCE_NUMBER_OF_PARTITIONS + "' configuration in the WITH "
              + "clause (and optionally '" + CommonCreateConfigs.SOURCE_NUMBER_OF_REPLICAS + "'). "
              + "For example: " + SqlFormatter.formatSql(example));
    }

    throwIfRetentionPresentForTable(topicCleanUpPolicy, properties.getRetentionInMillis());

    topicPropertiesBuilder
        .withName(topicName)
        .withWithClause(
            Optional.of(properties.getKafkaTopic()),
            properties.getPartitions(),
            properties.getReplicas(),
            properties.getRetentionInMillis());

    final TopicCreationResult result = createTopic(topicPropertiesBuilder, topicCleanUpPolicy);

    final ConfiguredStatement<CreateSource> stmt = buildConfiguredStatement(
            statement,
            properties.withCleanupPolicy(topicCleanUpPolicy));
    return new ConfiguredStatementWithSideEffects<>(stmt, result.getSideEffects());
  }

  @SuppressWarnings("unchecked")
  private <T extends CreateAsSelect> ConfiguredStatementWithSideEffects<T> injectForCreateAsSelect(
      final ConfiguredStatement<T> statement,
      final TopicProperties.Builder topicPropertiesBuilder
  ) {
    final String prefix = statement.getSessionConfig().getConfig(true)
        .getString(KsqlConfig.KSQL_OUTPUT_TOPIC_NAME_PREFIX_CONFIG);

    final T createAsSelect = statement.getStatement();
    final CreateSourceAsProperties properties = createAsSelect.getProperties();

    if (properties.getCleanupPolicy().isPresent()) {
      throw new KsqlException(CLEANUP_POLICY_PRESENT_EXCEPTION);
    }

    final SourceTopicsExtractor extractor = new SourceTopicsExtractor(metaStore);
    extractor.process(statement.getStatement().getQuery(), null);
    final String sourceTopicName = extractor.getPrimarySourceTopic().getKafkaTopicName();

    topicPropertiesBuilder
        .withName(prefix + createAsSelect.getName().text())
        .withSource(
            () -> topicClient.describeTopic(sourceTopicName),
            () -> topicClient.getTopicConfig(sourceTopicName))
        .withWithClause(
            properties.getKafkaTopic(),
            properties.getPartitions(),
            properties.getReplicas(),
            properties.getRetentionInMillis());

    final String topicCleanUpPolicy;
    final Map<String, Object> additionalTopicConfigs = new HashMap<>();
    if (createAsSelect instanceof CreateStreamAsSelect) {
      topicCleanUpPolicy = TopicConfig.CLEANUP_POLICY_DELETE;
    } else {
      if (createAsSelect.getQuery().getWindow().isPresent()) {
        topicCleanUpPolicy =
            TopicConfig.CLEANUP_POLICY_COMPACT + "," + TopicConfig.CLEANUP_POLICY_DELETE;

        createAsSelect.getQuery().getWindow().get().getKsqlWindowExpression().getRetention()
            .ifPresent(retention -> additionalTopicConfigs.put(
                TopicConfig.RETENTION_MS_CONFIG,
                retention.toDuration().toMillis()
            ));
      } else {
        topicCleanUpPolicy = TopicConfig.CLEANUP_POLICY_COMPACT;
      }
    }

    throwIfRetentionPresentForTable(topicCleanUpPolicy, properties.getRetentionInMillis());

    final TopicCreationResult result = createTopic(
            topicPropertiesBuilder,
            topicCleanUpPolicy,
            additionalTopicConfigs
    );
    final TopicProperties info = result.getTopicProperties();

    final CreateSourceAsProperties injectedProperties = properties.withTopic(
        info.getTopicName(),
        info.getPartitions(),
        info.getReplicas(),
        (Long) additionalTopicConfigs
            .getOrDefault(TopicConfig.RETENTION_MS_CONFIG, info.getRetentionInMillis()))
        .withCleanupPolicy(topicCleanUpPolicy);

    return new ConfiguredStatementWithSideEffects<>(
        (ConfiguredStatement<T>) buildConfiguredStatement(statement, injectedProperties),
        result.getSideEffects()
    );
  }

  private void throwIfRetentionPresentForTable(
      final String topicCleanUpPolicy,
      final Optional<Long> retentionInMillis
  ) {
    if (topicCleanUpPolicy.equals(TopicConfig.CLEANUP_POLICY_COMPACT)
        && retentionInMillis.isPresent()) {
      throw new KsqlException(
          "Invalid config variable in the WITH clause: RETENTION_MS."
          + " Non-windowed tables do not support retention.");
    }
  }

  private TopicCreationResult createTopic(
      final Builder topicPropertiesBuilder,
      final String topicCleanUpPolicy
  ) {
    return createTopic(topicPropertiesBuilder, topicCleanUpPolicy, Collections.emptyMap());
  }

  private TopicCreationResult createTopic(
      final Builder topicPropertiesBuilder,
      final String topicCleanUpPolicy,
      final Map<String, Object> additionalTopicConfigs
  ) {
    final TopicProperties info = topicPropertiesBuilder.build();

    final Map<String, Object> config = new HashMap<>();
    config.put(TopicConfig.CLEANUP_POLICY_CONFIG, topicCleanUpPolicy);

    // Set the retention.ms as max(RETENTION_MS, RETENTION)
    if (additionalTopicConfigs.containsKey(TopicConfig.RETENTION_MS_CONFIG)) {
      config.put(
          TopicConfig.RETENTION_MS_CONFIG,
          Math.max(
              info.getRetentionInMillis(),
              (Long) additionalTopicConfigs.get(TopicConfig.RETENTION_MS_CONFIG))
      );
      additionalTopicConfigs.remove(TopicConfig.RETENTION_MS_CONFIG);
    } else {
      config.put(TopicConfig.RETENTION_MS_CONFIG, info.getRetentionInMillis());
    }

    config.putAll(additionalTopicConfigs);

    // Note: The retention.ms config has no effect if cleanup.policy=compact
    // config is set for topics that are backed by tables
    if (topicCleanUpPolicy.equals(TopicConfig.CLEANUP_POLICY_COMPACT)) {
      config.remove(TopicConfig.RETENTION_MS_CONFIG);
    }

    final boolean created = topicClient.createTopic(
            info.getTopicName(),
            info.getPartitions(),
            info.getReplicas(),
            config
    );

    return TopicCreationResult.resultFor(info, created);
  }

  private static ConfiguredStatement<CreateSource> buildConfiguredStatement(
      final ConfiguredStatement<? extends CreateSource> original,
      final CreateSourceProperties injectedProps
  ) {
    final CreateSource statement = original.getStatement();

    final CreateSource withProps = statement.copyWith(
        original.getStatement().getElements(),
        injectedProps
    );

    final KsqlParser.PreparedStatement<CreateSource> prepared = buildPreparedStatement(withProps);
    return ConfiguredStatement.of(prepared, original.getSessionConfig());
  }

  private static ConfiguredStatement<CreateAsSelect> buildConfiguredStatement(
      final ConfiguredStatement<? extends CreateAsSelect> original,
      final CreateSourceAsProperties injectedProps
  ) {
    final CreateAsSelect statement = original.getStatement();

    final CreateAsSelect withProps = statement.copyWith(injectedProps);

    final KsqlParser.PreparedStatement<CreateAsSelect> prepared = buildPreparedStatement(withProps);
    return ConfiguredStatement.of(prepared, original.getSessionConfig());
  }

  private static <T extends Statement> KsqlParser.PreparedStatement<T> buildPreparedStatement(
      final T stmt
  ) {
    String formattedSql = SqlFormatter.formatSql(stmt);
    if (!formattedSql.endsWith(";")) {
      formattedSql = formattedSql + ";";
    }
    return KsqlParser.PreparedStatement.of(formattedSql, stmt);
  }

  private static final class TopicCreationSideEffect {
    public final String topicName;

    private TopicCreationSideEffect(final String topicName) {
      this.topicName = topicName;
    }

    @Override
    public String toString() {
      return String.format("TopicCreationSideEffect{topicName='%s'}", topicName);
    }
  }

  private static final class TopicCreationResult {
    private final TopicProperties topicProperties;
    private final List<?> sideEffects;

    private TopicCreationResult(final TopicProperties topicProperties, final List<?> sideEffects) {
      this.topicProperties = topicProperties;
      this.sideEffects = new ArrayList<>(sideEffects);
    }

    public List<Object> getSideEffects() {
      return Collections.unmodifiableList(sideEffects);
    }

    public TopicProperties getTopicProperties() {
      return topicProperties;
    }

    public static TopicCreationResult resultFor(
            final TopicProperties properties,
            final boolean created
    ) {
      return new TopicCreationResult(properties,
              created
              ? Collections.singletonList(new TopicCreationSideEffect(properties.getTopicName()))
              : Collections.emptyList());
    }

  }
}
