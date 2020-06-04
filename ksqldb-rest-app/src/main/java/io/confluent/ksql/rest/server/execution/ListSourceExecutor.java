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

import com.google.common.collect.ImmutableSet;
import io.confluent.ksql.KsqlExecutionContext;
import io.confluent.ksql.exception.KafkaResponseGetFailedException;
import io.confluent.ksql.metastore.model.DataSource;
import io.confluent.ksql.metastore.model.KsqlStream;
import io.confluent.ksql.metastore.model.KsqlTable;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.parser.tree.ListStreams;
import io.confluent.ksql.parser.tree.ListTables;
import io.confluent.ksql.parser.tree.ShowColumns;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.rest.SessionProperties;
import io.confluent.ksql.rest.entity.KsqlEntity;
import io.confluent.ksql.rest.entity.KsqlWarning;
import io.confluent.ksql.rest.entity.QueryStatusCount;
import io.confluent.ksql.rest.entity.RunningQuery;
import io.confluent.ksql.rest.entity.SourceConsumerGroupOffset;
import io.confluent.ksql.rest.entity.SourceConsumerGroupOffsets;
import io.confluent.ksql.rest.entity.SourceDescription;
import io.confluent.ksql.rest.entity.SourceDescriptionEntity;
import io.confluent.ksql.rest.entity.SourceDescriptionFactory;
import io.confluent.ksql.rest.entity.SourceDescriptionList;
import io.confluent.ksql.rest.entity.SourceInfo.Stream;
import io.confluent.ksql.rest.entity.SourceInfo.Table;
import io.confluent.ksql.rest.entity.StreamsList;
import io.confluent.ksql.rest.entity.TablesList;
import io.confluent.ksql.rest.server.KsqlRestApplication;
import io.confluent.ksql.services.KafkaConsumerGroupClient.ConsumerGroupSummary;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlConstants;
import io.confluent.ksql.util.KsqlStatementException;
import io.confluent.ksql.util.PersistentQueryMetadata;
import io.confluent.ksql.util.ReservedInternalTopics;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;

// CHECKSTYLE_RULES.OFF: ClassDataAbstractionCoupling
public final class ListSourceExecutor {
  // CHECKSTYLE_RULES.ON: ClassDataAbstractionCoupling

  private ListSourceExecutor() { }

  private static Optional<KsqlEntity> sourceDescriptionList(
      final ConfiguredStatement<?> statement,
      final SessionProperties sessionProperties,
      final KsqlExecutionContext executionContext,
      final ServiceContext serviceContext,
      final List<? extends DataSource> sources
  ) {
    final List<SourceDescriptionWithWarnings> descriptions = sources.stream()
        .map(
            s -> describeSource(
                statement.getConfig(),
                executionContext,
                serviceContext,
                s.getName(),
                true,
                statement.getStatementText())
        )
        .collect(Collectors.toList());
    return Optional.of(
        new SourceDescriptionList(
            statement.getStatementText(),
            descriptions.stream().map(d -> d.description).collect(Collectors.toList()),
            descriptions.stream().flatMap(d -> d.warnings.stream()).collect(Collectors.toList())
        )
    );
  }

  public static Optional<KsqlEntity> streams(
      final ConfiguredStatement<ListStreams> statement,
      final SessionProperties sessionProperties,
      final KsqlExecutionContext executionContext,
      final ServiceContext serviceContext
  ) {
    final List<KsqlStream<?>> ksqlStreams = getSpecificStreams(executionContext);

    final ListStreams listStreams = statement.getStatement();
    if (listStreams.getShowExtended()) {
      return sourceDescriptionList(
          statement,
          sessionProperties,
          executionContext,
          serviceContext,
          ksqlStreams
      );
    }

    return Optional.of(new StreamsList(
        statement.getStatementText(),
        ksqlStreams.stream()
            .map(ListSourceExecutor::sourceSteam)
            .collect(Collectors.toList())));
  }

  public static Optional<KsqlEntity> tables(
      final ConfiguredStatement<ListTables> statement,
      final SessionProperties sessionProperties,
      final KsqlExecutionContext executionContext,
      final ServiceContext serviceContext
  ) {
    final List<KsqlTable<?>> ksqlTables = getSpecificTables(executionContext);

    final ListTables listTables = statement.getStatement();
    if (listTables.getShowExtended()) {
      return sourceDescriptionList(
          statement,
          sessionProperties,
          executionContext,
          serviceContext,
          ksqlTables
      );
    }
    return Optional.of(new TablesList(
        statement.getStatementText(),
        ksqlTables.stream()
            .map(ListSourceExecutor::sourceTable)
            .collect(Collectors.toList())));
  }

  public static Optional<KsqlEntity> columns(
      final ConfiguredStatement<ShowColumns> statement,
      final SessionProperties sessionProperties,
      final KsqlExecutionContext executionContext,
      final ServiceContext serviceContext
  ) {
    final ShowColumns showColumns = statement.getStatement();
    final SourceDescriptionWithWarnings descriptionWithWarnings = describeSource(
        statement.getConfig(),
        executionContext,
        serviceContext,
        showColumns.getTable(),
        showColumns.isExtended(),
        statement.getStatementText()
    );
    return Optional.of(
        new SourceDescriptionEntity(
            statement.getStatementText(),
            descriptionWithWarnings.description,
            descriptionWithWarnings.warnings
        )
    );
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
      final KsqlExecutionContext ksqlEngine,
      final ServiceContext serviceContext,
      final SourceName name,
      final boolean extended,
      final String statementText) {
    final DataSource dataSource = ksqlEngine.getMetaStore().getSource(name);
    if (dataSource == null) {
      throw new KsqlStatementException(String.format(
          "Could not find STREAM/TABLE '%s' in the Metastore",
          name.text()
      ), statementText);
    }

    final List<RunningQuery> sourceQueries = getQueries(ksqlEngine,
        q -> q.getSourceNames().contains(dataSource.getName()));
    final List<RunningQuery> sinkQueries = getQueries(ksqlEngine,
        q -> q.getSinkName().equals(dataSource.getName()));

    Optional<org.apache.kafka.clients.admin.TopicDescription> topicDescriptionOptional = Optional
        .empty();
    Optional<SourceConsumerGroupOffsets> sourceConsumerOffsets = Optional.empty();
    final List<KsqlWarning> warnings = new LinkedList<>();
    if (extended) {
      try {
        final String kafkaTopicName = dataSource.getKafkaTopicName();
        final TopicDescription topicDescription = serviceContext.getTopicClient()
            .describeTopic(kafkaTopicName);
        topicDescriptionOptional = Optional.of(topicDescription);
        if (!sourceQueries.isEmpty()) {
          final QueryId queryId = sourceQueries.get(0).getId();
          final String persistenceQueryPrefix =
              ksqlConfig.getString(KsqlConfig.KSQL_PERSISTENT_QUERY_NAME_PREFIX_CONFIG);
          final String applicationId = getQueryApplicationId(
              getServiceId(ksqlConfig),
              persistenceQueryPrefix,
              queryId
          );
          final Optional<ConsumerGroupSummary> consumerGroupDescription = Optional.of(
              serviceContext.getConsumerGroupClient().describeConsumerGroup(applicationId)
          );
          final Map<TopicPartition, OffsetAndMetadata> topicAndConsumerOffsets =
              serviceContext.getConsumerGroupClient().listConsumerGroupOffsets(applicationId);
          final Map<TopicPartition, ListOffsetsResultInfo> topicAndStartOffsets =
              serviceContext.getTopicClient().listTopicStartOffsets(kafkaTopicName);
          final Map<TopicPartition, ListOffsetsResultInfo> topicAndEndOffsets =
              serviceContext.getTopicClient().listTopicEndOffsets(kafkaTopicName);
          sourceConsumerOffsets = consumerGroupDescription.map(cg ->
              new SourceConsumerGroupOffsets(
                  applicationId,
                  topicDescription.name(),
                  consumerOffsets(
                      topicDescription,
                      topicAndStartOffsets,
                      topicAndEndOffsets,
                      topicAndConsumerOffsets)));
        }
      } catch (final KafkaException | KafkaResponseGetFailedException e) {
        warnings.add(new KsqlWarning("Error from Kafka: " + e.getMessage()));
      }
    }

    return new SourceDescriptionWithWarnings(
        warnings,
        SourceDescriptionFactory.create(
            dataSource,
            extended,
            sourceQueries,
            sinkQueries,
            topicDescriptionOptional,
            sourceConsumerOffsets
        )
    );
  }

  private static List<SourceConsumerGroupOffset> consumerOffsets(
      final TopicDescription topicDescription,
      final Map<TopicPartition, ListOffsetsResultInfo> topicAndStartOffsets,
      final Map<TopicPartition, ListOffsetsResultInfo> topicAndEndOffsets,
      final Map<TopicPartition, OffsetAndMetadata> topicAndConsumerOffsets
  ) {
    final List<SourceConsumerGroupOffset> sourceConsumerGroupOffsets = new ArrayList<>();
    for (TopicPartitionInfo topicPartitionInfo : topicDescription.partitions()) {
      final TopicPartition tp = new TopicPartition(topicDescription.name(),
          topicPartitionInfo.partition());
      final ListOffsetsResultInfo startOffsetResultInfo = topicAndStartOffsets.get(tp);
      final ListOffsetsResultInfo endOffsetResultInfo = topicAndEndOffsets.get(tp);
      final OffsetAndMetadata offsetAndMetadata = topicAndConsumerOffsets.get(tp);
      sourceConsumerGroupOffsets.add(
          new SourceConsumerGroupOffset(
              topicPartitionInfo.partition(),
              startOffsetResultInfo != null ? startOffsetResultInfo.offset() : 0,
              endOffsetResultInfo != null ? endOffsetResultInfo.offset() : 0,
              offsetAndMetadata != null ? offsetAndMetadata.offset() : 0
          ));
    }
    return sourceConsumerGroupOffsets;
  }

  private static String getServiceId(final KsqlConfig ksqlConfig) {
    return ReservedInternalTopics.KSQL_INTERNAL_TOPIC_PREFIX
        + ksqlConfig.getString(KsqlConfig.KSQL_SERVICE_ID_CONFIG);
  }

  private static String getQueryApplicationId(
      final String serviceId,
      final String queryPrefix,
      final QueryId queryId) {
    return serviceId + queryPrefix + queryId;
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
            ImmutableSet.of(q.getSinkName().text()),
            ImmutableSet.of(q.getResultTopic().getKafkaTopicName()),
            q.getQueryId(),
            QueryStatusCount.fromStreamsStateCounts(
                Collections.singletonMap(q.getState(), 1)),
            KsqlConstants.KsqlQueryType.PERSISTENT)).collect(Collectors.toList());
  }

  private static Stream sourceSteam(final KsqlStream<?> dataSource) {
    return new Stream(
        dataSource.getName().text(),
        dataSource.getKsqlTopic().getKafkaTopicName(),
        dataSource.getKsqlTopic().getValueFormat().getFormat().name()
    );
  }

  private static Table sourceTable(final KsqlTable<?> dataSource) {
    return new Table(
        dataSource.getName().text(),
        dataSource.getKsqlTopic().getKafkaTopicName(),
        dataSource.getKsqlTopic().getValueFormat().getFormat().name(),
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
