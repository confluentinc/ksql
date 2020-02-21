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
import io.confluent.ksql.parser.tree.ListQueries;
import io.confluent.ksql.rest.SessionProperties;
import io.confluent.ksql.rest.entity.KsqlEntity;
import io.confluent.ksql.rest.entity.KsqlEntityList;
import io.confluent.ksql.rest.entity.KsqlHostInfoEntity;
import io.confluent.ksql.rest.entity.Queries;
import io.confluent.ksql.rest.entity.QueryDescription;
import io.confluent.ksql.rest.entity.QueryDescriptionFactory;
import io.confluent.ksql.rest.entity.QueryDescriptionList;
import io.confluent.ksql.rest.entity.RunningQuery;
import io.confluent.ksql.rest.server.ServerUtil;
import io.confluent.ksql.rest.util.DiscoverRemoteHostsUtil;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.statement.ConfiguredStatement;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.kafka.streams.state.HostInfo;

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
          listQueries,
          statement,
          sessionProperties,
          executionContext,
          serviceContext);
    }

    final List<RunningQuery> runningQueries = new ArrayList<>();
    runningQueries.addAll(executionContext.getPersistentQueries()
        .stream()
        .map(q -> new RunningQuery(
            q.getStatementString(),
            ImmutableSet.of(q.getSinkName().name()),
            ImmutableSet.of(q.getResultTopic().getKafkaTopicName()),
            q.getQueryId(),
            Optional.of(q.getState()),
            Optional.of(new KsqlHostInfoEntity(sessionProperties.getKsqlHostInfo()))
        ))
        .collect(Collectors.toList()));
    
    if (!sessionProperties.getIsInternalRequest()) {
      final Set<HostInfo> hosts =
          DiscoverRemoteHostsUtil.getRemoteHosts(
              executionContext.getPersistentQueries(),
              sessionProperties.getKsqlHostInfo());

      hosts.forEach(hostInfo -> {
        final KsqlEntityList response = serviceContext.getKsqlClient().makeInternalKsqlRequest(
            ServerUtil.buildRemoteUri(
                sessionProperties.getLocalUrl(),
                hostInfo.host(),
                hostInfo.port()
            ),
            "show queries;"
        ).getResponse();
        response.forEach(queries -> {
          runningQueries.addAll(((Queries) queries).getQueries());
        });
      });
    }

    return Optional.of(new io.confluent.ksql.rest.entity.Queries(
        statement.getStatementText(),
        runningQueries));
  }

  private static Optional<KsqlEntity> executeExtended(
      final ListQueries listQueries,
      final ConfiguredStatement<ListQueries> statement,
      final SessionProperties sessionProperties,
      final KsqlExecutionContext executionContext,
      final ServiceContext serviceContext
  ) {
    final List<QueryDescription> queryDescriptionList = new ArrayList<>();
    queryDescriptionList.addAll(
        executionContext.getPersistentQueries().stream()
        .map(query ->
            QueryDescriptionFactory.forQueryMetadata(
                query,
                Optional.of(new KsqlHostInfoEntity(sessionProperties.getKsqlHostInfo()))
            ))
        .collect(Collectors.toList()));

    if (!sessionProperties.getIsInternalRequest()) {
      final Set<HostInfo> hosts =
          DiscoverRemoteHostsUtil.getRemoteHosts(
              executionContext.getPersistentQueries(),
              sessionProperties.getKsqlHostInfo());

      hosts.forEach(hostInfo -> {
        final KsqlEntityList response = serviceContext.getKsqlClient().makeInternalKsqlRequest(
            ServerUtil.buildRemoteUri(
                sessionProperties.getLocalUrl(),
                hostInfo.host(),
                hostInfo.port()
            ),
            "show queries extended;"
        ).getResponse();

        response.forEach(queries -> {
          queryDescriptionList.addAll(((QueryDescriptionList) queries).getQueryDescriptions());
        });
      });
    }
    return Optional.of(new QueryDescriptionList(
        statement.getStatementText(),
        queryDescriptionList));
  }
}
