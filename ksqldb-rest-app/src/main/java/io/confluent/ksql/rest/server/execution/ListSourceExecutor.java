/*
 * Copyright 2019 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.rest.server.execution;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import io.confluent.ksql.KsqlExecutionContext;
import io.confluent.ksql.exception.KafkaResponseGetFailedException;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.model.DataSource;
import io.confluent.ksql.metastore.model.KsqlStream;
import io.confluent.ksql.metastore.model.KsqlTable;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.parser.tree.DescribeStreams;
import io.confluent.ksql.parser.tree.DescribeTables;
import io.confluent.ksql.parser.tree.ListStreams;
import io.confluent.ksql.parser.tree.ListTables;
import io.confluent.ksql.parser.tree.ShowColumns;
import io.confluent.ksql.parser.tree.StatementWithExtendedClause;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.rest.SessionProperties;
import io.confluent.ksql.rest.entity.ConsumerPartitionOffsets;
import io.confluent.ksql.rest.entity.KsqlEntity;
import io.confluent.ksql.rest.entity.KsqlWarning;
import io.confluent.ksql.rest.entity.QueryOffsetSummary;
import io.confluent.ksql.rest.entity.QueryStatusCount;
import io.confluent.ksql.rest.entity.QueryTopicOffsetSummary;
import io.confluent.ksql.rest.entity.RunningQuery;
import io.confluent.ksql.rest.entity.SourceDescription;
import io.confluent.ksql.rest.entity.SourceDescriptionEntity;
import io.confluent.ksql.rest.entity.SourceDescriptionFactory;
import io.confluent.ksql.rest.entity.SourceDescriptionList;
import io.confluent.ksql.rest.entity.SourceInfo.Stream;
import io.confluent.ksql.rest.entity.SourceInfo.Table;
import io.confluent.ksql.rest.entity.StreamsList;
import io.confluent.ksql.rest.entity.TablesList;
import io.confluent.ksql.rest.server.KsqlRestApplication;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlConstants;
import io.confluent.ksql.util.KsqlStatementException;
import io.confluent.ksql.util.PersistentQueryMetadata;
import io.confluent.ksql.util.QueryApplicationId;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;

// CHECKSTYLE_RULES.OFF: ClassDataAbstractionCoupling
public final class ListSourceExecutor {
  // CHECKSTYLE_RULES.ON: ClassDataAbstractionCoupling

  private ListSourceExecutor() {
  }

  private static Optional<KsqlEntity> sourceDescriptionList(
      final ConfiguredStatement<? extends StatementWithExtendedClause> statement,
      final SessionProperties sessionProperties,
      final KsqlExecutionContext executionContext,
      final ServiceContext serviceContext,
      final List<? extends DataSource> sources,
      final boolean extended
  ) {
    final RemoteHostExecutor remoteHostExecutor = RemoteHostExecutor.create(
        statement,
        sessionProperties,
        executionContext,
        serviceContext.getKsqlClient()
    );

    final Multimap<String, SourceDescription> remoteSourceDescriptions = extended
        ? RemoteSourceDescriptionExecutor.fetchSourceDescriptions(remoteHostExecutor)
        : ImmutableMultimap.of();


    final List<SourceDescriptionWithWarnings> descriptions = sources.stream()
        .map(
            s -> describeSource(
                statement.getSessionConfig().getConfig(false),
                executionContext,
                serviceContext,
                s.getName(),
                extended,
                statement,
                sessionProperties,
                remoteSourceDescriptions.get(s.getName().toString())
            )
        )
        .collect(Collectors.toList());
    return Optional.of(
        new SourceDescriptionList(
            statement.getMaskedStatementText(),
            descriptions.stream().map(d -> d.description).collect(Collectors.toList()),
            descriptions.stream().flatMap(d -> d.warnings.stream()).collect(Collectors.toList())
        )
    );
  }

  public static StatementExecutorResponse streams(
      final ConfiguredStatement<ListStreams> statement,
      final SessionProperties sessionProperties,
      final KsqlExecutionContext executionContext,
      final ServiceContext serviceContext
  ) {
    final List<KsqlStream<?>> ksqlStreams = getSpecificStreams(executionContext);

    final ListStreams listStreams = statement.getStatement();
    if (listStreams.getShowExtended()) {
      return StatementExecutorResponse.handled(sourceDescriptionList(
          statement,
          sessionProperties,
          executionContext,
          serviceContext,
          ksqlStreams,
          listStreams.getShowExtended()
      ));
    }

    return StatementExecutorResponse.handled(Optional.of(new StreamsList(
        statement.getMaskedStatementText(),
        ksqlStreams.stream()
            .map(ListSourceExecutor::sourceSteam)
            .collect(Collectors.toList()))));
  }

  public static StatementExecutorResponse tables(
      final ConfiguredStatement<ListTables> statement,
      final SessionProperties sessionProperties,
      final KsqlExecutionContext executionContext,
      final ServiceContext serviceContext
  ) {
    final List<KsqlTable<?>> ksqlTables = getSpecificTables(executionContext);

    final ListTables listTables = statement.getStatement();
    if (listTables.getShowExtended()) {
      return StatementExecutorResponse.handled(sourceDescriptionList(
          statement,
          sessionProperties,
          executionContext,
          serviceContext,
          ksqlTables,
          listTables.getShowExtended()
      ));
    }
    return StatementExecutorResponse.handled(Optional.of(new TablesList(
        statement.getMaskedStatementText(),
        ksqlTables.stream()
            .map(ListSourceExecutor::sourceTable)
            .collect(Collectors.toList()))));
  }

  public static StatementExecutorResponse describeStreams(
      final ConfiguredStatement<DescribeStreams> statement,
      final SessionProperties sessionProperties,
      final KsqlExecutionContext executionContext,
      final ServiceContext serviceContext
  ) {
    final List<KsqlStream<?>> ksqlStreams = getSpecificStreams(executionContext);

    final DescribeStreams describeStreams = statement.getStatement();
    return StatementExecutorResponse.handled(sourceDescriptionList(
        statement,
        sessionProperties,
        executionContext,
        serviceContext,
        ksqlStreams,
        describeStreams.getShowExtended()));
  }

  public static StatementExecutorResponse describeTables(
      final ConfiguredStatement<DescribeTables> statement,
      final SessionProperties sessionProperties,
      final KsqlExecutionContext executionContext,
      final ServiceContext serviceContext
  ) {
    final List<KsqlTable<?>> ksqlTables = getSpecificTables(executionContext);

    final DescribeTables describeTables = statement.getStatement();
    return StatementExecutorResponse.handled(sourceDescriptionList(
        statement,
        sessionProperties,
        executionContext,
        serviceContext,
        ksqlTables,
        describeTables.getShowExtended()));
  }

  public static StatementExecutorResponse columns(
      final ConfiguredStatement<ShowColumns> statement,
      final SessionProperties sessionProperties,
      final KsqlExecutionContext executionContext,
      final ServiceContext serviceContext
  ) {
    final ShowColumns showColumns = statement.getStatement();
    final SourceDescriptionWithWarnings descriptionWithWarnings = describeSource(
        statement.getSessionConfig().getConfig(false),
        executionContext,
        serviceContext,
        showColumns.getTable(),
        showColumns.isExtended(),
        statement,
        sessionProperties,
        ImmutableList.of()
    );
    return StatementExecutorResponse.handled(Optional.of(
        new SourceDescriptionEntity(
            statement.getMaskedStatementText(),
            descriptionWithWarnings.description,
            descriptionWithWarnings.warnings)));
  }

  private static List<KsqlTable<?>> getSpecificTables(
      final KsqlExecutionContext executionContext
  ) {
    return executionContext.getMetaStore().getAllDataSources().values().stream()
        .filter(KsqlTable.class::isInstance)
        .filter(structuredDataSource -> !structuredDataSource.getName().equals(
            KsqlRestApplication.getCommandsStreamName()))
        .map(table -> (KsqlTable<?>) table)
        .collect(Collectors.toList());
  }

  private static List<KsqlStream<?>> getSpecificStreams(
      final KsqlExecutionContext executionContext
  ) {
    return executionContext.getMetaStore().getAllDataSources().values().stream()
        .filter(KsqlStream.class::isInstance)
        .filter(structuredDataSource -> !structuredDataSource.getName().equals(
            KsqlRestApplication.getCommandsStreamName()))
        .map(table -> (KsqlStream<?>) table)
        .collect(Collectors.toList());
  }

  private static SourceDescriptionWithWarnings describeSource(
      final KsqlConfig ksqlConfig,
      final KsqlExecutionContext ksqlExecutionContext,
      final ServiceContext serviceContext,
      final SourceName name,
      final boolean extended,
      final ConfiguredStatement<? extends StatementWithExtendedClause> statement,
      final SessionProperties sessionProperties,
      final Collection<SourceDescription> remoteSourceDescriptions

  ) {
    final DataSource dataSource = ksqlExecutionContext.getMetaStore().getSource(name);
    if (dataSource == null) {
      throw new KsqlStatementException(String.format(
          "Could not find STREAM/TABLE '%s' in the Metastore",
          name.text()
      ), statement.getMaskedStatementText());
    }

    final List<RunningQuery> readQueries = getQueries(ksqlExecutionContext,
        q -> q.getSourceNames().contains(dataSource.getName()));
    final List<RunningQuery> writeQueries = getQueries(ksqlExecutionContext,
        q -> q.getSinkName().equals(Optional.of(dataSource.getName())));

    Optional<TopicDescription> topicDescription =
        Optional.empty();
    List<QueryOffsetSummary> queryOffsetSummaries = Collections.emptyList();
    List<String> sourceConstraints = Collections.emptyList();

    final List<KsqlWarning> warnings = new LinkedList<>();
    try {
      topicDescription = Optional.of(
          serviceContext.getTopicClient().describeTopic(dataSource.getKafkaTopicName(), true)
      );
      sourceConstraints = getSourceConstraints(name, ksqlExecutionContext.getMetaStore());
    } catch (final KafkaException | KafkaResponseGetFailedException e) {
      warnings.add(new KsqlWarning("Error from Kafka: " + e.getMessage()));
    }

    if (extended) {
      queryOffsetSummaries = queryOffsetSummaries(ksqlConfig, serviceContext, writeQueries);


      return new SourceDescriptionWithWarnings(
          warnings,
          SourceDescriptionFactory.create(
              dataSource,
              extended,
              readQueries,
              writeQueries,
              topicDescription,
              queryOffsetSummaries,
              sourceConstraints,
              remoteSourceDescriptions.stream().flatMap(sd -> sd.getClusterStatistics().stream()),
              remoteSourceDescriptions.stream().flatMap(sd -> sd.getClusterErrorStats().stream()),
              sessionProperties.getKsqlHostInfo(),
              ksqlExecutionContext.metricCollectors()
          )
      );
    }

    return new SourceDescriptionWithWarnings(
        warnings,
        SourceDescriptionFactory.create(
            dataSource,
            extended,
            readQueries,
            writeQueries,
            topicDescription,
            queryOffsetSummaries,
            sourceConstraints,
            java.util.stream.Stream.empty(),
            java.util.stream.Stream.empty(),
            sessionProperties.getKsqlHostInfo(),
            ksqlExecutionContext.metricCollectors()
        )
    );
  }

  private static List<String> getSourceConstraints(
      final SourceName sourceName,
      final MetaStore metaStore
  ) {
    return metaStore.getSourceConstraints(sourceName).stream()
        .map(SourceName::text)
        .collect(Collectors.toList());
  }

  private static List<QueryOffsetSummary> queryOffsetSummaries(
      final KsqlConfig ksqlConfig,
      final ServiceContext serviceContext,
      final List<RunningQuery> writeQueries
  ) {
    final Map<String, Map<TopicPartition, OffsetAndMetadata>> offsetsPerQuery =
        new HashMap<>(writeQueries.size());
    final Map<String, Set<String>> topicsPerQuery = new HashMap<>();
    final Set<String> allTopics = new HashSet<>();
    // Get topics and offsets per running query
    for (RunningQuery query : writeQueries) {
      final QueryId queryId = query.getId();
      final String applicationId =
          QueryApplicationId.build(ksqlConfig, true, queryId);
      final Map<TopicPartition, OffsetAndMetadata> topicAndConsumerOffsets =
          serviceContext.getConsumerGroupClient().listConsumerGroupOffsets(applicationId);
      offsetsPerQuery.put(applicationId, topicAndConsumerOffsets);
      final Set<String> topics = topicAndConsumerOffsets.keySet().stream()
          .map(TopicPartition::topic)
          .collect(Collectors.toSet());
      topicsPerQuery.put(applicationId, topics);
      allTopics.addAll(topics);
    }
    // Get topics descriptions and start/end offsets
    final Map<String, TopicDescription> sourceTopicDescriptions =
        serviceContext.getTopicClient().describeTopics(allTopics);
    final Map<TopicPartition, Long> topicAndStartOffsets =
        serviceContext.getTopicClient().listTopicsStartOffsets(allTopics);
    final Map<TopicPartition, Long> topicAndEndOffsets =
        serviceContext.getTopicClient().listTopicsEndOffsets(allTopics);
    // Build consumer offsets summary
    final List<QueryOffsetSummary> offsetSummaries = new ArrayList<>();
    for (Entry<String, Set<String>> entry : topicsPerQuery.entrySet()) {
      final List<QueryTopicOffsetSummary> topicSummaries = new ArrayList<>();
      for (String topic : entry.getValue()) {
        topicSummaries.add(
            new QueryTopicOffsetSummary(
                topic,
                consumerPartitionOffsets(
                    sourceTopicDescriptions.get(topic),
                    topicAndStartOffsets,
                    topicAndEndOffsets,
                    offsetsPerQuery.get(entry.getKey()))));
      }
      offsetSummaries.add(new QueryOffsetSummary(entry.getKey(), topicSummaries));
    }
    return offsetSummaries;
  }

  private static List<ConsumerPartitionOffsets> consumerPartitionOffsets(
      final TopicDescription topicDescription,
      final Map<TopicPartition, Long> topicAndStartOffsets,
      final Map<TopicPartition, Long> topicAndEndOffsets,
      final Map<TopicPartition, OffsetAndMetadata> topicAndConsumerOffsets
  ) {
    final List<ConsumerPartitionOffsets> consumerPartitionOffsets = new ArrayList<>();
    for (TopicPartitionInfo topicPartitionInfo : topicDescription.partitions()) {
      final TopicPartition tp = new TopicPartition(topicDescription.name(),
          topicPartitionInfo.partition());
      final Long startOffsetResultInfo = topicAndStartOffsets.get(tp);
      final Long endOffsetResultInfo = topicAndEndOffsets.get(tp);
      final OffsetAndMetadata offsetAndMetadata = topicAndConsumerOffsets.get(tp);
      consumerPartitionOffsets.add(
          new ConsumerPartitionOffsets(
              topicPartitionInfo.partition(),
              startOffsetResultInfo,
              endOffsetResultInfo,
              // null when consumer has not poll yet from a topic-partition
              offsetAndMetadata != null ? offsetAndMetadata.offset() : 0
          ));
    }
    return consumerPartitionOffsets;
  }

  private static List<RunningQuery> getQueries(
      final KsqlExecutionContext ksqlEngine,
      final Predicate<PersistentQueryMetadata> predicate
  ) {
    return ksqlEngine.getPersistentQueries()
        .stream()
        .filter(predicate)
        .map(q -> new RunningQuery(
            q.getStatementString(),
            q.getSinkName().isPresent()
                ? ImmutableSet.of(q.getSinkName().get().text())
                : ImmutableSet.of(),
            q.getResultTopic().isPresent()
                ? ImmutableSet.of(q.getResultTopic().get().getKafkaTopicName())
                : ImmutableSet.of(),
            q.getQueryId(),
            new QueryStatusCount(Collections.singletonMap(q.getQueryStatus(), 1)),
            KsqlConstants.KsqlQueryType.PERSISTENT)).collect(Collectors.toList());
  }

  private static Stream sourceSteam(final KsqlStream<?> dataSource) {
    return new Stream(
        dataSource.getName().text(),
        dataSource.getKsqlTopic().getKafkaTopicName(),
        dataSource.getKsqlTopic().getKeyFormat().getFormat(),
        dataSource.getKsqlTopic().getValueFormat().getFormat(),
        dataSource.getKsqlTopic().getKeyFormat().isWindowed()
    );
  }

  private static Table sourceTable(final KsqlTable<?> dataSource) {
    return new Table(
        dataSource.getName().text(),
        dataSource.getKsqlTopic().getKafkaTopicName(),
        dataSource.getKsqlTopic().getKeyFormat().getFormat(),
        dataSource.getKsqlTopic().getValueFormat().getFormat(),
        dataSource.getKsqlTopic().getKeyFormat().isWindowed()
    );
  }

  private static final class SourceDescriptionWithWarnings {

    private final List<KsqlWarning> warnings;
    private final SourceDescription description;

    private SourceDescriptionWithWarnings(
        final List<KsqlWarning> warnings,
        final SourceDescription description) {
      this.warnings = warnings;
      this.description = description;
    }
  }
}
