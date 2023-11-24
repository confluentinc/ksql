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

package io.confluent.ksql.rest.server.services;

import static io.confluent.ksql.rest.server.services.InternalKsqlClientFactory.createInternalClient;
import static java.util.Objects.requireNonNull;

import com.google.common.annotations.VisibleForTesting;
import io.confluent.ksql.reactive.BufferedPublisher;
import io.confluent.ksql.rest.client.KsqlClient;
import io.confluent.ksql.rest.client.KsqlTarget;
import io.confluent.ksql.rest.client.RestResponse;
import io.confluent.ksql.rest.client.StreamPublisher;
import io.confluent.ksql.rest.entity.ClusterStatusResponse;
import io.confluent.ksql.rest.entity.KsqlEntityList;
import io.confluent.ksql.rest.entity.KsqlHostInfoEntity;
import io.confluent.ksql.rest.entity.LagReportingMessage;
import io.confluent.ksql.rest.entity.StreamedRow;
import io.confluent.ksql.services.SimpleKsqlClient;
import io.confluent.ksql.util.KsqlHostInfo;
import io.vertx.core.Vertx;
import io.vertx.core.net.SocketAddress;
import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class DefaultKsqlClient implements SimpleKsqlClient {

  private static final Logger LOG = LoggerFactory.getLogger(DefaultKsqlClient.class);

  private final Optional<String> authHeader;
  private final KsqlClient sharedClient;
  private final boolean ownSharedClient;

  @VisibleForTesting
  DefaultKsqlClient(final Optional<String> authHeader,
      final Map<String, Object> clientProps,
      final BiFunction<Integer, String, SocketAddress> socketAddressFactory) {
    this(
        authHeader,
        createInternalClient(toClientProps(clientProps), socketAddressFactory, Vertx.vertx()),
        true
    );
  }

  DefaultKsqlClient(
      final Optional<String> authHeader,
      final KsqlClient sharedClient
  ) {
    this(authHeader, sharedClient, false);
  }

  DefaultKsqlClient(
      final Optional<String> authHeader,
      final KsqlClient sharedClient,
      final boolean ownSharedClient
  ) {
    this.authHeader = requireNonNull(authHeader, "authHeader");
    this.sharedClient = requireNonNull(sharedClient, "sharedClient");
    this.ownSharedClient = ownSharedClient;
  }

  @Override
  public RestResponse<KsqlEntityList> makeKsqlRequest(
      final URI serverEndPoint,
      final String sql,
      final Map<String, ?> requestProperties) {
    final KsqlTarget target = sharedClient
        .target(serverEndPoint);

    return getTarget(target)
        .postKsqlRequest(sql, requestProperties, Optional.empty());
  }

  @Override
  public RestResponse<List<StreamedRow>> makeQueryRequest(
      final URI serverEndPoint,
      final String sql,
      final Map<String, ?> configOverrides,
      final Map<String, ?> requestProperties
  ) {

    final KsqlTarget target = sharedClient
        .target(serverEndPoint)
        .properties(configOverrides);

    final RestResponse<List<StreamedRow>> resp = getTarget(target)
        .postQueryRequest(sql, requestProperties, Optional.empty());

    if (resp.isErroneous()) {
      return RestResponse.erroneous(resp.getStatusCode(), resp.getErrorMessage());
    }

    return RestResponse.successful(resp.getStatusCode(), resp.getResponse());
  }

  @Override
  public RestResponse<Integer> makeQueryRequest(
      final URI serverEndPoint,
      final String sql,
      final Map<String, ?> configOverrides,
      final Map<String, ?> requestProperties,
      final Consumer<List<StreamedRow>> rowConsumer,
      final CompletableFuture<Void> shouldCloseConnection
  ) {
    final KsqlTarget target = sharedClient
        .target(serverEndPoint)
        .properties(configOverrides);

    final RestResponse<Integer> resp = getTarget(target)
        .postQueryRequest(sql, requestProperties, Optional.empty(), rowConsumer,
            shouldCloseConnection);

    if (resp.isErroneous()) {
      return RestResponse.erroneous(resp.getStatusCode(), resp.getErrorMessage());
    }

    return RestResponse.successful(resp.getStatusCode(), resp.getResponse());
  }

  @Override
  public CompletableFuture<RestResponse<BufferedPublisher<StreamedRow>>> makeQueryRequestStreamed(
      final URI serverEndPoint,
      final String sql,
      final Map<String, ?> configOverrides,
      final Map<String, ?> requestProperties
  ) {
    final KsqlTarget target = sharedClient
        .targetHttp2(serverEndPoint)
        .properties(configOverrides);

    final CompletableFuture<RestResponse<StreamPublisher<StreamedRow>>> response =
        getTarget(target)
        .postQueryRequestStreamedAsync(sql, requestProperties);

    return response.thenApply(resp -> {
      if (resp.isErroneous()) {
        return RestResponse.erroneous(resp.getStatusCode(), resp.getErrorMessage());
      }

      return RestResponse.successful(resp.getStatusCode(), resp.getResponse());
    });
  }

  @Override
  public void makeAsyncHeartbeatRequest(
      final URI serverEndPoint,
      final KsqlHostInfo host,
      final long timestamp) {
    final KsqlTarget target = sharedClient
        .target(serverEndPoint);

    getTarget(target)
        .postAsyncHeartbeatRequest(new KsqlHostInfoEntity(host.host(), host.port()), timestamp)
        .exceptionally(t -> {
          // We send heartbeat requests quite frequently and to nodes that might be down.  We don't
          // want to fill the logs with spam, so we debug log exceptions.
          LOG.debug("Exception in async heartbeat request", t);
          return null;
        });
  }

  @Override
  public RestResponse<ClusterStatusResponse> makeClusterStatusRequest(final URI serverEndPoint) {
    final KsqlTarget target = sharedClient
        .target(serverEndPoint);

    return getTarget(target).getClusterStatus();
  }

  @Override
  public void makeAsyncLagReportRequest(
      final URI serverEndPoint,
      final LagReportingMessage lagReportingMessage
  ) {
    final KsqlTarget target = sharedClient
        .target(serverEndPoint);

    getTarget(target).postAsyncLagReportingRequest(lagReportingMessage)
        .exceptionally(t -> {
          LOG.debug("Exception in async lag reporting request", t);
          return null;
        });
  }

  @Override
  public void close() {
    if (ownSharedClient) {
      sharedClient.close();
    }
  }

  private KsqlTarget getTarget(final KsqlTarget target) {
    return authHeader
        .map(target::authorizationHeader)
        .orElse(target);
  }

  private static Map<String, String> toClientProps(final Map<String, Object> config) {
    final Map<String, String> clientProps = new HashMap<>();
    for (Map.Entry<String, Object> entry : config.entrySet()) {
      clientProps.put(entry.getKey(), entry.getValue().toString());
    }
    return clientProps;
  }
}
