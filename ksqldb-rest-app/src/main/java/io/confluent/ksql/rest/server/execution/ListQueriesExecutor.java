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

  public static StatementExecutorResponse execute(
      final ConfiguredStatement<ListQueries> statement,
      final SessionProperties sessionProperties,
      final KsqlExecutionContext executionContext,
      final ServiceContext serviceContext
  ) {
    final RemoteHostExecutor remoteHostExecutor = RemoteHostExecutor.create(
        statement,
        sessionProperties,
        executionContext,
        serviceContext.getKsqlClient()
    );
    return statement.getStatement().getShowExtended()
        ? executeExtended(statement, sessionProperties, executionContext, remoteHostExecutor)
        : executeSimple(statement, executionContext, remoteHostExecutor);
  }

  private static StatementExecutorResponse executeSimple(
      final ConfiguredStatement<ListQueries> statement,
      final KsqlExecutionContext executionContext,
      final RemoteHostExecutor remoteHostExecutor
  ) {
    final Map<QueryId, RunningQuery> runningQueries = mergeSimple(
        getLocalSimple(executionContext),
        remoteHostExecutor.fetchAllRemoteResults()
    );
    return StatementExecutorResponse.handled(Optional.of(new Queries(
        statement.getMaskedStatementText(),
        runningQueries.values())));
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
                    persistentQuery.getSinkName().isPresent()
                        ? ImmutableSet.of(persistentQuery.getSinkName().get().text())
                        : ImmutableSet.of(),
                    persistentQuery.getResultTopic().isPresent()
                        ? ImmutableSet.of(
                            persistentQuery.getResultTopic().get().getKafkaTopicName())
                        : ImmutableSet.of(),
                    q.getQueryId(),
                    new QueryStatusCount(Collections.singletonMap(q.getQueryStatus(), 1)),
                    q.getQueryType());
              }

              return new RunningQuery(
                  q.getStatementString(),
                  ImmutableSet.of(),
                  ImmutableSet.of(),
                  q.getQueryId(),
                  new QueryStatusCount(Collections.singletonMap(q.getQueryStatus(), 1)),
                  q.getQueryType());
            }
        ));
  }

  private static Map<QueryId, RunningQuery> mergeSimple(
      final Map<QueryId, RunningQuery> allResults,
      final Pair<Map<HostInfo, KsqlEntity>, Set<HostInfo>> remoteResults
  ) {
    final List<RunningQuery> remoteRunningQueries = remoteResults.getLeft()
        .values()
        .stream()
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

  private static StatementExecutorResponse executeExtended(
      final ConfiguredStatement<ListQueries> statement,
      final SessionProperties sessionProperties,
      final KsqlExecutionContext executionContext,
      final RemoteHostExecutor remoteHostExecutor
  ) {
    final Map<QueryId, QueryDescription> queryDescriptions = mergeExtended(
        getLocalExtended(sessionProperties, executionContext),
        remoteHostExecutor.fetchAllRemoteResults()
    );

    return StatementExecutorResponse.handled(Optional.of(new QueryDescriptionList(
        statement.getMaskedStatementText(),
        queryDescriptions.values())));
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
                    query.getQueryStatus()
                ))));
  }

  private static Map<QueryId, QueryDescription> mergeExtended(
      final Map<QueryId, QueryDescription> allResults,
      final Pair<Map<HostInfo, KsqlEntity>, Set<HostInfo>> remoteResults
  ) {
    final List<QueryDescription> remoteQueryDescriptions = remoteResults
        .getLeft()
        .values()
        .stream()
        .map(QueryDescriptionList.class::cast)
        .map(QueryDescriptionList::getQueryDescriptions)
        .flatMap(List::stream)
        .collect(Collectors.toList());
    for (QueryDescription q : remoteQueryDescriptions) {
      final QueryId queryId = q.getId();

      // If the query has already been discovered, add to the ksqlQueryHostStatus mapping
      // and the streams metadata task set
      if (allResults.containsKey(queryId)) {
        for (Map.Entry<KsqlHostInfoEntity, KsqlQueryStatus> entry :
            q.getKsqlHostQueryStatus().entrySet()) {
          allResults
              .get(queryId)
              .updateKsqlHostQueryStatus(entry.getKey(), entry.getValue());
        }
        
        allResults.get(queryId).updateTaskMetadata(q.getTasksMetadata());
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