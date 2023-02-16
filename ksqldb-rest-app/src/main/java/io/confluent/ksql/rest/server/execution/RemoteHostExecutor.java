/*
 * Copyright 2021 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"; you may not use
 * this file except in compliance with the License. You may obtain a copy of the
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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.confluent.ksql.KsqlExecutionContext;
import io.confluent.ksql.rest.SessionProperties;
import io.confluent.ksql.rest.client.RestResponse;
import io.confluent.ksql.rest.entity.KsqlEntity;
import io.confluent.ksql.rest.entity.KsqlEntityList;
import io.confluent.ksql.rest.server.ServerUtil;
import io.confluent.ksql.rest.util.DiscoverRemoteHostsUtil;
import io.confluent.ksql.services.SimpleKsqlClient;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.util.KsqlRequestConfig;
import io.confluent.ksql.util.Pair;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.streams.state.HostInfo;


public final class RemoteHostExecutor {
  private static final int TIMEOUT_SECONDS = 10;
  private static final Logger LOG = LoggerFactory.getLogger(RemoteHostExecutor.class);

  private final ConfiguredStatement<?> statement;
  private final SessionProperties sessionProperties;
  private final KsqlExecutionContext executionContext;
  private final SimpleKsqlClient ksqlClient;

  private RemoteHostExecutor(
      final ConfiguredStatement<?> statement,
      final SessionProperties sessionProperties,
      final KsqlExecutionContext executionContext,
      final SimpleKsqlClient ksqlClient
  ) {
    this.statement = Objects.requireNonNull(statement);
    this.sessionProperties = Objects.requireNonNull(sessionProperties);
    this.executionContext = Objects.requireNonNull(executionContext);
    this.ksqlClient = Objects.requireNonNull(ksqlClient);
  }

  public static RemoteHostExecutor create(
      final ConfiguredStatement<?> statement,
      final SessionProperties sessionProperties,
      final KsqlExecutionContext executionContext,
      final SimpleKsqlClient ksqlClient
  ) {
    return new RemoteHostExecutor(
        statement, sessionProperties, executionContext, ksqlClient);
  }

  private RestResponse<KsqlEntityList> makeKsqlRequest(
      final HostInfo host,
      final String statementText
  ) {
    return ksqlClient.makeKsqlRequest(
        ServerUtil.buildRemoteUri(
            sessionProperties.getLocalUrl(),
            host.host(),
            host.port()
        ),
        statementText,
        Collections.singletonMap(KsqlRequestConfig.KSQL_REQUEST_INTERNAL_REQUEST, true)
    );
  }

  private CompletableFuture<RestResponse<KsqlEntityList>> fetchRemoteData(
      final String statementText,
      final HostInfo host,
      final Executor executor
  ) {
    return CompletableFuture.supplyAsync(() -> makeKsqlRequest(host, statementText), executor);
  }

  public Pair<Map<HostInfo, KsqlEntity>, Set<HostInfo>> fetchAllRemoteResults() {
    final Set<HostInfo> remoteHosts = DiscoverRemoteHostsUtil.getRemoteHosts(
        executionContext.getPersistentQueries(),
        sessionProperties.getKsqlHostInfo()
    );
    if (remoteHosts.isEmpty() || sessionProperties.getInternalRequest()) {
      return new Pair<>(ImmutableMap.of(), ImmutableSet.of());
    }

    final Set<HostInfo> unresponsiveHosts = new HashSet<>();
    final ExecutorService executorService = Executors.newFixedThreadPool(remoteHosts.size());

    try {
      final Map<HostInfo, CompletableFuture<RestResponse<KsqlEntityList>>> futureResponses =
          new HashMap<>();
      for (HostInfo host : remoteHosts) {
        futureResponses.put(host, fetchRemoteData(statement.getUnMaskedStatementText(), host,
            executorService));
      }

      final ImmutableMap.Builder<HostInfo, KsqlEntity> results = ImmutableMap.builder();
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
            results.put(e.getKey(), response.getResponse().get(0));
          }
        } catch (final Exception cause) {
          LOG.warn("Failed to retrieve info from host: {}, statement: {}, cause: {}",
              e.getKey(), statement.getMaskedStatementText(), cause);
          unresponsiveHosts.add(e.getKey());
        }
      }

      return new Pair<>(results.build(), unresponsiveHosts);
    } finally {
      executorService.shutdown();
    }
  }
}