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
import java.util.stream.Collectors;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.state.HostInfo;

@SuppressFBWarnings("SE_BAD_FIELD")
public final class ListQueriesExecutor {

  private ListQueriesExecutor() { }

  public static Optional<KsqlEntity> execute(
      final ConfiguredStatement<ListQueries> statement,
      final SessionProperties sessionProperties,
      final KsqlExecutionContext executionContext,
      final ServiceContext serviceContext
  ) {
    final ListQueries listQueries = statement.getStatement();
    if (listQueries.getShowExtended()) {
      return executeExtended(
          statement,
          sessionProperties,
          executionContext,
          serviceContext);
    }

    final Map<QueryId, RunningQuery> queryToRunningQuery = executionContext
        .getPersistentQueries()
        .stream()
        .collect(Collectors.toMap(
            PersistentQueryMetadata::getQueryId,
            q -> new RunningQuery(
                q.getStatementString(),
                ImmutableSet.of(q.getSinkName().text()),
                ImmutableSet.of(q.getResultTopic().getKafkaTopicName()),
                q.getQueryId(),
                Optional.empty(),
                new QueryStateCount(
                    Collections.singletonMap(KafkaStreams.State.valueOf(q.getState()), 1)))
        ));
    

    if (!sessionProperties.getInternalRequest()) {
      final Set<HostInfo> remoteHosts =
          DiscoverRemoteHostsUtil.getRemoteHosts(
              executionContext.getPersistentQueries(),
              sessionProperties.getKsqlHostInfo());
      
      if (!remoteHosts.isEmpty()) {
        final ExecutorService executorService = Executors.newFixedThreadPool(remoteHosts.size());
        final List<Future<List<RunningQuery>>> futureRunningQueries = new ArrayList<>();
        for (HostInfo host : remoteHosts) {
          futureRunningQueries.add(executorService.submit(() -> {
            final KsqlEntityList response = serviceContext.getKsqlClient()
                .makeKsqlRequest(
                    ServerUtil.buildRemoteUri(
                        sessionProperties.getLocalUrl(),
                        host.host(),
                        host.port()
                    ),
                    statement.getStatementText(),
                    Collections.singletonMap(KsqlRequestConfig.KSQL_REQUEST_INTERNAL_REQUEST, true))
                .getResponse();
            return ((Queries) response.get(0)).getQueries();
          }));
        }

        final ArrayList<RunningQuery> remoteRunningQueries =
            new ArrayList<>();
        futureRunningQueries.forEach(future -> {
          try {
            remoteRunningQueries.addAll(future.get());
          } catch (final Exception e) {
            // If the future fails from a server, that result won't be included in the output
          }
        });

        executorService.shutdown();
        for (RunningQuery q : remoteRunningQueries) {
          final QueryId queryId = q.getId();

          // If the query has already been discovered, update the KafkaStreamsStateCount object
          if (queryToRunningQuery.containsKey(queryId)) {
            for (Map.Entry<KafkaStreams.State, Integer> entry :
                q.getStateCount().getStates().entrySet()) {
              queryToRunningQuery
                  .get(queryId)
                  .getStateCount()
                  .updateStateCount(entry.getKey(), entry.getValue());
            }
          } else {
            queryToRunningQuery.put(queryId, q);
          }
        }
      }
    }

    return Optional.of(new Queries(
        statement.getStatementText(),
        new ArrayList<>(queryToRunningQuery.values().stream().map(runningQuery -> new RunningQuery(
            runningQuery.getQueryString(),
            runningQuery.getSinks(),
            runningQuery.getSinkKafkaTopics(),
            runningQuery.getId(),
            Optional.of(runningQuery.getStateCount().toString()),
            runningQuery.getStateCount()
        ))
        .collect(Collectors.toList()))));
  }

  private static Optional<KsqlEntity> executeExtended(
      final ConfiguredStatement<ListQueries> statement,
      final SessionProperties sessionProperties,
      final KsqlExecutionContext executionContext,
      final ServiceContext serviceContext
  ) {
    final Map<QueryId, QueryDescription> queryToQueryDescription = executionContext
        .getPersistentQueries()
        .stream()
        .collect(Collectors.toMap(
            PersistentQueryMetadata::getQueryId,
            query -> {
                final HashMap<KsqlHostInfoEntity, String> ksqlHostQueryState = new HashMap<>();
                ksqlHostQueryState.put(
                    new KsqlHostInfoEntity(sessionProperties.getKsqlHostInfo()),
                    query.getState());
                return QueryDescriptionFactory.forQueryMetadata(query, ksqlHostQueryState);
            }));

    if (!sessionProperties.getInternalRequest()) {
      final Set<HostInfo> remoteHosts =
          DiscoverRemoteHostsUtil.getRemoteHosts(
              executionContext.getPersistentQueries(),
              sessionProperties.getKsqlHostInfo());

      if (!remoteHosts.isEmpty()) {
        final ExecutorService executorService = Executors.newFixedThreadPool(remoteHosts.size());
        final List<Future<List<QueryDescription>>> futureQueryDescriptions = new ArrayList<>();
        for (HostInfo host : remoteHosts) {
          futureQueryDescriptions.add(executorService.submit(() -> {
            final KsqlEntityList response = serviceContext.getKsqlClient()
                .makeKsqlRequest(
                    ServerUtil.buildRemoteUri(
                        sessionProperties.getLocalUrl(),
                        host.host(),
                        host.port()
                    ),
                    statement.getStatementText(),
                    Collections.singletonMap(KsqlRequestConfig.KSQL_REQUEST_INTERNAL_REQUEST, true))
                .getResponse();
            return ((QueryDescriptionList) response.get(0)).getQueryDescriptions();
          }));
        }

        final ArrayList<QueryDescription> remoteQueryDescriptions =
            new ArrayList<>();
        futureQueryDescriptions.forEach(future -> {
          try {
            remoteQueryDescriptions.addAll(future.get());
          } catch (final Exception e) {
            // If the future fails from a server, that result won't be included in the output
          }
        });

        executorService.shutdown();
        for (QueryDescription q : remoteQueryDescriptions) {
          final QueryId queryId = q.getId();

          // If the query has already been discovered, add to the ksqlQueryHostState mapping
          if (queryToQueryDescription.containsKey(queryId)) {
            for (Map.Entry<KsqlHostInfoEntity, String> entry :
                q.getKsqlHostQueryState().entrySet()) {
              queryToQueryDescription
                  .get(queryId)
                  .getKsqlHostQueryState()
                  .put(entry.getKey(), entry.getValue());
            }
          } else {
            queryToQueryDescription.put(queryId, q);
          }
        }
      }
    }

    return Optional.of(new QueryDescriptionList(
        statement.getStatementText(),
        queryToQueryDescription.values()));
  }
}