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
import io.confluent.ksql.rest.entity.QueryStateCount;
import io.confluent.ksql.rest.entity.RunningQuery;
import io.confluent.ksql.rest.server.ServerUtil;
import io.confluent.ksql.rest.util.DiscoverRemoteHostsUtil;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.services.SimpleKsqlClient;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.util.KsqlRequestConfig;
import io.confluent.ksql.util.PersistentQueryMetadata;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.state.HostInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressFBWarnings("SE_BAD_FIELD")
public final class ListQueriesExecutor {

  private static int TIMEOUT_SECONDS = 10;
  private static final Logger LOG = LoggerFactory.getLogger(ListQueriesExecutor.class);

  private ListQueriesExecutor() { }

  public static Optional<KsqlEntity> execute(
      final ConfiguredStatement<ListQueries> statement,
      final SessionProperties sessionProperties,
      final KsqlExecutionContext executionContext,
      final ServiceContext serviceContext
  ) {
    final List<KsqlEntity> remoteResults =
        scatterGather(statement, sessionProperties, executionContext, serviceContext);

    return statement.getStatement().getShowExtended()
        ? executeExtended(remoteResults, sessionProperties, statement, executionContext)
        : executeSimple(remoteResults, statement, executionContext);
  }

  private static Optional<KsqlEntity> executeSimple(
      final List<KsqlEntity> remoteResults,
      final ConfiguredStatement<ListQueries> statement,
      final KsqlExecutionContext executionContext
  ) {
    final Map<QueryId, RunningQuery> runningQueries = getLocalSimple(executionContext);

    mergeSimple(remoteResults, runningQueries);

    return Optional.of(new Queries(
        statement.getStatementText(),
        runningQueries.values()));
  }

  private static Map<QueryId, RunningQuery> getLocalSimple(
      final KsqlExecutionContext executionContext
  ) {
    return executionContext
        .getPersistentQueries()
        .stream()
        .collect(Collectors.toMap(
            PersistentQueryMetadata::getQueryId,
            q -> new RunningQuery(
                q.getStatementString(),
                ImmutableSet.of(q.getSinkName().text()),
                ImmutableSet.of(q.getResultTopic().getKafkaTopicName()),
                q.getQueryId(),
                new QueryStateCount(
                    Collections.singletonMap(KafkaStreams.State.valueOf(q.getState()), 1)))
        ));
  }

  private static void mergeSimple(
      final List<KsqlEntity> remoteResults,
      final Map<QueryId, RunningQuery> allResults
  ) {
    final List<RunningQuery> remoteRunningQueries = remoteResults.stream()
        .map(Queries.class::cast)
        .map(Queries::getQueries)
        .flatMap(List::stream)
        .collect(Collectors.toList());
    
    for (RunningQuery q : remoteRunningQueries) {
      final QueryId queryId = q.getId();

      // If the query has already been discovered, update the QueryStateCount object
      if (allResults.containsKey(queryId)) {
        for (Map.Entry<KafkaStreams.State, Integer> entry :
            q.getStateCount().getStates().entrySet()) {
          allResults
              .get(queryId)
              .getStateCount()
              .updateStateCount(entry.getKey(), entry.getValue());
        }
      } else {
        allResults.put(queryId, q);
      }
    }
  }
  
  private static Optional<KsqlEntity> executeExtended(
      final List<KsqlEntity> remoteResults,
      final SessionProperties sessionProperties,
      final ConfiguredStatement<ListQueries> statement,
      final KsqlExecutionContext executionContext
  ) {
    final Map<QueryId, QueryDescription> queryDescriptions =
        getLocalExtended(sessionProperties, executionContext);

    mergeExtended(remoteResults, queryDescriptions);

    return Optional.of(new QueryDescriptionList(
        statement.getStatementText(),
        queryDescriptions.values()));
  }

  private static Map<QueryId, QueryDescription> getLocalExtended(
      final SessionProperties sessionProperties,
      final KsqlExecutionContext executionContext
  ) {
    return executionContext
        .getPersistentQueries()
        .stream()
        .collect(Collectors.toMap(
            PersistentQueryMetadata::getQueryId,
            query -> QueryDescriptionFactory.forQueryMetadata(
                query,
                Collections.singletonMap(
                    new KsqlHostInfoEntity(sessionProperties.getKsqlHostInfo()),
                    query.getState()))));
  }

  private static void mergeExtended(
      final List<KsqlEntity> remoteResults,
      final Map<QueryId, QueryDescription> allResults
  ) {
    final List<QueryDescription> remoteQueryDescriptions = remoteResults.stream()
        .map(QueryDescriptionList.class::cast)
        .map(QueryDescriptionList::getQueryDescriptions)
        .flatMap(List::stream)
        .collect(Collectors.toList());
    for (QueryDescription q : remoteQueryDescriptions) {
      final QueryId queryId = q.getId();

      // If the query has already been discovered, add to the ksqlQueryHostState mapping
      if (allResults.containsKey(queryId)) {
        for (Map.Entry<KsqlHostInfoEntity, String> entry :
            q.getKsqlHostQueryState().entrySet()) {
          allResults
              .get(queryId)
              .updateKsqlHostQueryState(entry.getKey(), entry.getValue());
        }
      } else {
        allResults.put(queryId, q);
      }
    }
  }

  private static List<KsqlEntity> scatterGather(
      final ConfiguredStatement<ListQueries> statement,
      final SessionProperties sessionProperties,
      final KsqlExecutionContext executionContext,
      final ServiceContext serviceContext
  ) {
    if (sessionProperties.getInternalRequest()) {
      return ImmutableList.of();
    }

    final Set<HostInfo> remoteHosts = DiscoverRemoteHostsUtil.getRemoteHosts(
        executionContext.getPersistentQueries(),
        sessionProperties.getKsqlHostInfo()
    );

    if (remoteHosts.isEmpty()) {
      return ImmutableList.of();
    }

    final ExecutorService executorService = Executors.newFixedThreadPool(remoteHosts.size());

    try {
      final SimpleKsqlClient ksqlClient = serviceContext.getKsqlClient();

      final Map<HostInfo, Future<RestResponse<KsqlEntityList>>> futureResponses = new HashMap<>();
      for (HostInfo host : remoteHosts) {
        final Future<RestResponse<KsqlEntityList>> future = executorService.submit(() -> ksqlClient
            .makeKsqlRequest(
                ServerUtil.buildRemoteUri(
                    sessionProperties.getLocalUrl(),
                    host.host(),
                    host.port()
                ),
                statement.getStatementText(),
                Collections.singletonMap(KsqlRequestConfig.KSQL_REQUEST_INTERNAL_REQUEST, true))
        );

        futureResponses.put(host, future);
      }
      
      final List<KsqlEntity> results = new ArrayList<>();
      for (final Map.Entry<HostInfo, Future<RestResponse<KsqlEntityList>>> e
          : futureResponses.entrySet()) {
        try {
          final RestResponse<KsqlEntityList> response =
              e.getValue().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
          if (response.isErroneous()) {
            LOG.warn("Error response from host. host: {}, cause: {}",
                e.getKey(), response.getErrorMessage().getMessage());
          } else {
            results.add(response.getResponse().get(0));
          }
        } catch (final Exception cause) {
          LOG.warn("Failed to retrieve query info from host. host: {}, cause: {}",
              e.getKey(), cause.getMessage());
        }
      }

      return results;
    } finally {
      executorService.shutdown();
    }
  }
}