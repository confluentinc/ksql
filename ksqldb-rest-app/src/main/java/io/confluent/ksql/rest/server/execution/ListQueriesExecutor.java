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
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.KsqlExecutionContext;
import io.confluent.ksql.parser.tree.ListQueries;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.rest.SessionProperties;
import io.confluent.ksql.rest.entity.KsqlEntity;
import io.confluent.ksql.rest.entity.KsqlHostInfoEntity;
import io.confluent.ksql.rest.entity.Queries;
import io.confluent.ksql.rest.entity.QueryDescription;
import io.confluent.ksql.rest.entity.QueryDescriptionFactory;
import io.confluent.ksql.rest.entity.QueryDescriptionList;
import io.confluent.ksql.rest.entity.QueryStatusCount;
import io.confluent.ksql.rest.entity.RunningQuery;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.util.KsqlConstants;
import io.confluent.ksql.util.KsqlConstants.KsqlQueryStatus;
import io.confluent.ksql.util.Pair;
import io.confluent.ksql.util.PersistentQueryMetadata;
import io.confluent.ksql.util.QueryMetadata;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.kafka.streams.state.HostInfo;

@SuppressFBWarnings("SE_BAD_FIELD")
public final class ListQueriesExecutor {

  private ListQueriesExecutor() {
  }

  public static Optional<KsqlEntity> execute(
      final ConfiguredStatement<ListQueries> statement,
      final SessionProperties sessionProperties,
      final KsqlExecutionContext executionContext,
      final ServiceContext serviceContext
  ) {
    final RemoteDataAugmenter remoteDataAugmenter = RemoteDataAugmenter.create(
        statement.getStatementText(),
        sessionProperties,
        executionContext,
        serviceContext.getKsqlClient()
    );
    return statement.getStatement().getShowExtended()
        ? executeExtended(statement, sessionProperties, executionContext, remoteDataAugmenter)
        : executeSimple(statement, executionContext, remoteDataAugmenter);
  }

  private static Optional<KsqlEntity> executeSimple(
      final ConfiguredStatement<ListQueries> statement,
      final KsqlExecutionContext executionContext,
      final RemoteDataAugmenter remoteDataAugmenter
  ) {
    final Map<QueryId, RunningQuery> runningQueries = remoteDataAugmenter.augmentWithRemote(
        getLocalSimple(executionContext),
        ListQueriesExecutor::mergeSimple
    );
    return Optional.of(new Queries(
        statement.getStatementText(),
        runningQueries.values()));
  }

  private static Map<QueryId, RunningQuery> getLocalSimple(
      final KsqlExecutionContext executionContext
  ) {
    return executionContext
        .getAllLiveQueries()
        .stream()
        .collect(Collectors.toMap(
            QueryMetadata::getQueryId,
            q -> {
              if (q instanceof PersistentQueryMetadata) {

                final PersistentQueryMetadata persistentQuery = (PersistentQueryMetadata) q;
                return new RunningQuery(
                    q.getStatementString(),
                    ImmutableSet.of(persistentQuery.getSinkName().text()),
                    ImmutableSet.of(persistentQuery.getResultTopic().getKafkaTopicName()),
                    q.getQueryId(),
                    QueryStatusCount.fromStreamsStateCounts(
                        Collections.singletonMap(q.getState(), 1)),
                    q.getQueryType());
              }

              return new RunningQuery(
                  q.getStatementString(),
                  ImmutableSet.of(),
                  ImmutableSet.of(),
                  q.getQueryId(),
                  QueryStatusCount.fromStreamsStateCounts(
                      Collections.singletonMap(q.getState(), 1)),
                  q.getQueryType());
            }
        ));
  }

  private static Map<QueryId, RunningQuery> mergeSimple(
      final Map<QueryId, RunningQuery> allResults,
      final Pair<List<KsqlEntity>, Set<HostInfo>> remoteResults
  ) {
    final List<KsqlEntity> remoteQueries = remoteResults.getLeft();
    final List<RunningQuery> remoteRunningQueries = remoteQueries.stream()
        .map(Queries.class::cast)
        .map(Queries::getQueries)
        .flatMap(List::stream)
        .collect(Collectors.toList());

    for (RunningQuery q : remoteRunningQueries) {
      final QueryId queryId = q.getId();

      // If the query has already been discovered, update the QueryStatusCount object
      if (allResults.containsKey(queryId)) {
        for (Map.Entry<KsqlQueryStatus, Integer> entry :
            q.getStatusCount().getStatuses().entrySet()) {
          allResults
              .get(queryId)
              .getStatusCount()
              .updateStatusCount(entry.getKey(), entry.getValue());
        }
      } else {
        allResults.put(queryId, q);
      }
    }

    final Set<HostInfo> unresponsiveRemoteHosts = remoteResults.getRight();
    if (!unresponsiveRemoteHosts.isEmpty()) {
      for (RunningQuery runningQuery : allResults.values()) {
        runningQuery.getStatusCount()
            .updateStatusCount(KsqlQueryStatus.UNRESPONSIVE, unresponsiveRemoteHosts.size());
      }
    }
    return allResults;
  }

  private static Optional<KsqlEntity> executeExtended(
      final ConfiguredStatement<ListQueries> statement,
      final SessionProperties sessionProperties,
      final KsqlExecutionContext executionContext,
      final RemoteDataAugmenter remoteDataAugmenter
  ) {
    final Map<QueryId, QueryDescription> queryDescriptions = remoteDataAugmenter.augmentWithRemote(
        getLocalExtended(sessionProperties, executionContext),
        ListQueriesExecutor::mergeExtended
    );

    return Optional.of(new QueryDescriptionList(
        statement.getStatementText(),
        queryDescriptions.values()));
  }

  private static Map<QueryId, QueryDescription> getLocalExtended(
      final SessionProperties sessionProperties,
      final KsqlExecutionContext executionContext
  ) {
    return executionContext
        .getAllLiveQueries()
        .stream()
        .collect(Collectors.toMap(
            QueryMetadata::getQueryId,
            query -> QueryDescriptionFactory.forQueryMetadata(
                query,
                Collections.singletonMap(
                    new KsqlHostInfoEntity(sessionProperties.getKsqlHostInfo()),
                    KsqlConstants.fromStreamsState(
                        query.getState())
                ))));
  }

  private static Map<QueryId, QueryDescription> mergeExtended(
      final Map<QueryId, QueryDescription> allResults,
      final Pair<List<KsqlEntity>, Set<HostInfo>> remoteResults
  ) {
    final List<KsqlEntity> remoteQueries = remoteResults.getLeft();
    final List<QueryDescription> remoteQueryDescriptions = remoteQueries.stream()
        .map(QueryDescriptionList.class::cast)
        .map(QueryDescriptionList::getQueryDescriptions)
        .flatMap(List::stream)
        .collect(Collectors.toList());
    for (QueryDescription q : remoteQueryDescriptions) {
      final QueryId queryId = q.getId();

      // If the query has already been discovered, add to the ksqlQueryHostStatus mapping
      if (allResults.containsKey(queryId)) {
        for (Map.Entry<KsqlHostInfoEntity, KsqlQueryStatus> entry :
            q.getKsqlHostQueryStatus().entrySet()) {
          allResults
              .get(queryId)
              .updateKsqlHostQueryStatus(entry.getKey(), entry.getValue());
        }
      } else {
        allResults.put(queryId, q);
      }
    }

    final Set<HostInfo> unresponsiveRemoteHosts = remoteResults.getRight();
    for (HostInfo hostInfo : unresponsiveRemoteHosts) {
      for (QueryDescription queryDescription : allResults.values()) {
        queryDescription.updateKsqlHostQueryStatus(
            new KsqlHostInfoEntity(hostInfo.host(), hostInfo.port()),
            KsqlQueryStatus.UNRESPONSIVE);
      }
    }
    return allResults;
  }
}