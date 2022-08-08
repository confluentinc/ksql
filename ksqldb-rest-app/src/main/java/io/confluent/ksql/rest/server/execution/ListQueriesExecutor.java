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
import com.google.common.collect.ImmutableSet;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.KsqlExecutionContext;
import io.confluent.ksql.parser.tree.ListQueries;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.rest.SessionProperties;
import io.confluent.ksql.rest.client.RestResponse;
import io.confluent.ksql.rest.entity.KsqlEntity;
import io.confluent.ksql.rest.entity.KsqlEntityList;
import io.confluent.ksql.rest.entity.KsqlHostInfoEntity;
import io.confluent.ksql.rest.entity.Queries;
import io.confluent.ksql.rest.entity.QueryDescription;
import io.confluent.ksql.rest.entity.QueryDescriptionFactory;
import io.confluent.ksql.rest.entity.QueryDescriptionList;
import io.confluent.ksql.rest.entity.QueryStatusCount;
import io.confluent.ksql.rest.entity.RunningQuery;
import io.confluent.ksql.rest.server.ServerUtil;
import io.confluent.ksql.rest.util.DiscoverRemoteHostsUtil;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.services.SimpleKsqlClient;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.util.KsqlConstants;
import io.confluent.ksql.util.KsqlConstants.KsqlQueryStatus;
import io.confluent.ksql.util.KsqlRequestConfig;
import io.confluent.ksql.util.Pair;
import io.confluent.ksql.util.PersistentQueryMetadata;
import io.confluent.ksql.util.QueryMetadata;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.kafka.streams.state.HostInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressFBWarnings("SE_BAD_FIELD")
public final class ListQueriesExecutor {

  private static final int TIMEOUT_SECONDS = 10;
  private static final Logger LOG = LoggerFactory.getLogger(ListQueriesExecutor.class);

  private ListQueriesExecutor() {
  }

  public static Optional<KsqlEntity> execute(
      final ConfiguredStatement<ListQueries> statement,
      final SessionProperties sessionProperties,
      final KsqlExecutionContext executionContext,
      final ServiceContext serviceContext
  ) {
    final Pair<List<KsqlEntity>, Set<HostInfo>> remoteResults =
        scatterGather(statement, sessionProperties, executionContext, serviceContext);

    return statement.getStatement().getShowExtended()
        ? executeExtended(remoteResults, sessionProperties, statement, executionContext)
        : executeSimple(remoteResults, statement, executionContext);
  }

  private static Optional<KsqlEntity> executeSimple(
      final Pair<List<KsqlEntity>, Set<HostInfo>> remoteResults,
      final ConfiguredStatement<ListQueries> statement,
      final KsqlExecutionContext executionContext
  ) {
    final Map<QueryId, RunningQuery> runningQueries = getLocalSimple(executionContext);
    mergeSimple(remoteResults, runningQueries);

    return Optional.of(new Queries(
        statement.getMaskedStatementText(),
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

  private static void mergeSimple(
      final Pair<List<KsqlEntity>, Set<HostInfo>> remoteResults,
      final Map<QueryId, RunningQuery> allResults
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
  }
  
  private static Optional<KsqlEntity> executeExtended(
      final Pair<List<KsqlEntity>, Set<HostInfo>> remoteResults,
      final SessionProperties sessionProperties,
      final ConfiguredStatement<ListQueries> statement,
      final KsqlExecutionContext executionContext
  ) {
    final Map<QueryId, QueryDescription> queryDescriptions =
        getLocalExtended(sessionProperties, executionContext);

    mergeExtended(remoteResults, queryDescriptions);

    return Optional.of(new QueryDescriptionList(
        statement.getMaskedStatementText(),
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

  private static void mergeExtended(
      final Pair<List<KsqlEntity>, Set<HostInfo>> remoteResults,
      final Map<QueryId, QueryDescription> allResults
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
    for (HostInfo hostInfo: unresponsiveRemoteHosts) {
      for (QueryDescription queryDescription: allResults.values()) {
        queryDescription.updateKsqlHostQueryStatus(
            new KsqlHostInfoEntity(hostInfo.host(), hostInfo.port()),
            KsqlQueryStatus.UNRESPONSIVE);
      }
    }
  }

  private static Pair<List<KsqlEntity>, Set<HostInfo>> scatterGather(
      final ConfiguredStatement<ListQueries> statement,
      final SessionProperties sessionProperties,
      final KsqlExecutionContext executionContext,
      final ServiceContext serviceContext
  ) {
    if (sessionProperties.getInternalRequest()) {
      return new Pair<>(ImmutableList.of(), ImmutableSet.of());
    }

    final Set<HostInfo> remoteHosts = DiscoverRemoteHostsUtil.getRemoteHosts(
        executionContext.getPersistentQueries(),
        sessionProperties.getKsqlHostInfo()
    );

    if (remoteHosts.isEmpty()) {
      return new Pair<>(ImmutableList.of(), ImmutableSet.of());
    }

    final Set<HostInfo> unresponsiveHosts = new HashSet<>();
    final ExecutorService executorService = Executors.newFixedThreadPool(remoteHosts.size());

    try {
      final SimpleKsqlClient ksqlClient = serviceContext.getKsqlClient();

      final Map<HostInfo, CompletableFuture<RestResponse<KsqlEntityList>>> futureResponses =
          new HashMap<>();
      for (HostInfo host : remoteHosts) {
        final CompletableFuture<RestResponse<KsqlEntityList>> future = new CompletableFuture<>();
        executorService.execute(() -> {
          final RestResponse<KsqlEntityList> response = ksqlClient
              .makeKsqlRequest(
                  ServerUtil.buildRemoteUri(
                      sessionProperties.getLocalUrl(),
                      host.host(),
                      host.port()
                  ),
                  statement.getUnMaskedStatementText(),
                  Collections.singletonMap(KsqlRequestConfig.KSQL_REQUEST_INTERNAL_REQUEST, true));
          future.complete(response);
        });

        futureResponses.put(host, future);
      }
      
      final List<KsqlEntity> results = new ArrayList<>();
      for (final Map.Entry<HostInfo, CompletableFuture<RestResponse<KsqlEntityList>>> e
          : futureResponses.entrySet()) {
        try {
          final RestResponse<KsqlEntityList> response =
              e.getValue().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
          if (response.isErroneous()) {
            LOG.warn("Error response from host. host: {}, cause: {}",
                e.getKey(), response.getErrorMessage().getMessage());
            unresponsiveHosts.add(e.getKey());
          } else {
            results.add(response.getResponse().get(0));
          }
        } catch (final Exception cause) {
          LOG.warn("Failed to retrieve query info from host. host: {}, cause: {}",
              e.getKey(), cause.getMessage());
          unresponsiveHosts.add(e.getKey());
        }
      }

      return new Pair<>(results, unresponsiveHosts);
    } finally {
      executorService.shutdown();
    }
  }
}