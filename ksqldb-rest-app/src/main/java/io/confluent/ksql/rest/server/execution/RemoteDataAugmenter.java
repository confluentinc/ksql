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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.confluent.ksql.KsqlExecutionContext;
import io.confluent.ksql.rest.SessionProperties;
import io.confluent.ksql.rest.client.RestResponse;
import io.confluent.ksql.rest.entity.KsqlEntity;
import io.confluent.ksql.rest.entity.KsqlEntityList;
import io.confluent.ksql.rest.server.ServerUtil;
import io.confluent.ksql.rest.util.DiscoverRemoteHostsUtil;
import io.confluent.ksql.services.SimpleKsqlClient;
import io.confluent.ksql.util.KsqlRequestConfig;
import io.confluent.ksql.util.Pair;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import org.apache.kafka.streams.state.HostInfo;


public final class RemoteDataAugmenter {
  private static final int TIMEOUT_SECONDS = 10;
  private static final Logger LOG = LoggerFactory.getLogger(RemoteDataAugmenter.class);

  private final String statementText;
  private final SessionProperties sessionProperties;
  private final KsqlExecutionContext executionContext;
  private final SimpleKsqlClient ksqlClient;


  private RemoteDataAugmenter(
      final String statementText,
      final SessionProperties sessionProperties,
      final KsqlExecutionContext executionContext,
      final SimpleKsqlClient ksqlClient
  ) {
    this.statementText = Objects.requireNonNull(statementText);
    this.sessionProperties = Objects.requireNonNull(sessionProperties);
    this.executionContext = Objects.requireNonNull(executionContext);
    this.ksqlClient = Objects.requireNonNull(ksqlClient);
  }

  public static RemoteDataAugmenter create(
      final String statementText,
      final SessionProperties sessionProperties,
      final KsqlExecutionContext executionContext,
      final SimpleKsqlClient ksqlClient
  ) {
    return new RemoteDataAugmenter(
        statementText, sessionProperties, executionContext, ksqlClient);
  }


  public <R> R augmentWithRemote(
      final R localResult,
      final BiFunction<R, Pair<List<KsqlEntity>, Set<HostInfo>>, R> mergeFunc
  ) {
    Objects.requireNonNull(mergeFunc);
    if (sessionProperties.getInternalRequest()) {
      return mergeFunc.apply(localResult, new Pair<>(ImmutableList.of(), ImmutableSet.of()));
    }
    final Pair<List<KsqlEntity>, Set<HostInfo>> remoteResults = fetchAllRemoteResults();

    return mergeFunc.apply(localResult, remoteResults);
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

  private Pair<List<KsqlEntity>, Set<HostInfo>> fetchAllRemoteResults() {
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
      final Map<HostInfo, CompletableFuture<RestResponse<KsqlEntityList>>> futureResponses =
          new HashMap<>();
      for (HostInfo host : remoteHosts) {
        futureResponses.put(host, fetchRemoteData(statementText, host, executorService));
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
          LOG.warn("Failed to retrieve info from host: {}, statement: {}, cause: {}",
              e.getKey(), statementText, cause.getMessage());
          unresponsiveHosts.add(e.getKey());
        }
      }

      return new Pair<>(results, unresponsiveHosts);
    } finally {
      executorService.shutdown();
    }
  }
}