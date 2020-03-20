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

import static java.util.Objects.requireNonNull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.properties.LocalProperties;
import io.confluent.ksql.rest.client.KsqlClient;
import io.confluent.ksql.rest.client.KsqlTarget;
import io.confluent.ksql.rest.client.RestResponse;
import io.confluent.ksql.rest.entity.ClusterStatusResponse;
import io.confluent.ksql.rest.entity.KsqlEntityList;
import io.confluent.ksql.rest.entity.KsqlHostInfoEntity;
import io.confluent.ksql.rest.entity.LagReportingMessage;
import io.confluent.ksql.rest.entity.StreamedRow;
import io.confluent.ksql.services.SimpleKsqlClient;
import io.confluent.ksql.util.KsqlHostInfo;
import io.vertx.core.http.HttpClientOptions;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class DefaultKsqlClient implements SimpleKsqlClient {

  private static final Logger LOG = LoggerFactory.getLogger(DefaultKsqlClient.class);

  private final Optional<String> authHeader;
  private final KsqlClient sharedClient;

  DefaultKsqlClient(final Optional<String> authHeader) {
    this(
        authHeader,
        new KsqlClient(
            ImmutableMap.of(),
            Optional.empty(),
            new LocalProperties(ImmutableMap.of()),
            createClientOptions()
        )
    );
  }

  @VisibleForTesting
  DefaultKsqlClient(
      final Optional<String> authHeader,
      final KsqlClient sharedClient
  ) {
    this.authHeader = requireNonNull(authHeader, "authHeader");
    this.sharedClient = requireNonNull(sharedClient, "sharedClient");
  }

  @Override
  public RestResponse<KsqlEntityList> makeKsqlRequest(
      final URI serverEndPoint,
      final String sql
  ) {
    final KsqlTarget target = sharedClient
        .target(serverEndPoint);

    return getTarget(target, authHeader).postKsqlRequest(sql, Optional.empty());
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

    final RestResponse<List<StreamedRow>> resp = getTarget(target, authHeader)
        .postQueryRequest(sql, requestProperties, Optional.empty());

    if (resp.isErroneous()) {
      return RestResponse.erroneous(resp.getStatusCode(), resp.getErrorMessage());
    }

    return RestResponse.successful(resp.getStatusCode(), resp.getResponse());
  }

  @Override
  public void makeAsyncHeartbeatRequest(
      final URI serverEndPoint,
      final KsqlHostInfo host,
      final long timestamp) {
    final KsqlTarget target = sharedClient
        .target(serverEndPoint);

    getTarget(target, authHeader)
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

    return getTarget(target, authHeader).getClusterStatus();
  }

  @Override
  public void makeAsyncLagReportRequest(
      final URI serverEndPoint,
      final LagReportingMessage lagReportingMessage
  ) {
    final KsqlTarget target = sharedClient
        .target(serverEndPoint);

    getTarget(target, authHeader).postAsyncLagReportingRequest(lagReportingMessage)
        .exceptionally(t -> {
          LOG.debug("Exception in async lag reporting request", t);
          return null;
        });
  }

  @Override
  public void close() {
    sharedClient.close();
  }

  private KsqlTarget getTarget(final KsqlTarget target, final Optional<String> authHeader) {
    return authHeader
        .map(target::authorizationHeader)
        .orElse(target);
  }

  private static HttpClientOptions createClientOptions() {
    return new HttpClientOptions().setMaxPoolSize(100);
  }

}
