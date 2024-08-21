/*
 * Copyright 2020 Confluent Inc.
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

package io.confluent.ksql.rest.server;

import io.confluent.ksql.reactive.BufferedPublisher;
import io.confluent.ksql.rest.client.RestResponse;
import io.confluent.ksql.rest.entity.ClusterStatusResponse;
import io.confluent.ksql.rest.entity.KsqlEntityList;
import io.confluent.ksql.rest.entity.LagReportingMessage;
import io.confluent.ksql.rest.entity.StreamedRow;
import io.confluent.ksql.services.SimpleKsqlClient;
import io.confluent.ksql.util.KsqlHostInfo;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Ksql client that has the ability to start failing outgoing requests dynamically.  This is useful
 * in simulating network partitions.
 */
public class FaultyKsqlClient implements SimpleKsqlClient {
  private static final Logger LOG = LoggerFactory.getLogger(FaultyKsqlClient.class);

  private final SimpleKsqlClient workingClient;
  private final Supplier<Boolean> isFaulty;

  public FaultyKsqlClient(
      final SimpleKsqlClient workingClient,
      final Supplier<Boolean> isFaulty
  ) {
    this.workingClient = workingClient;
    this.isFaulty = isFaulty;
  }

  private SimpleKsqlClient getClient() {
    if (isFaulty.get()) {
      throw new UnsupportedOperationException("KSQL client is disabled");
    }
    return workingClient;
  }

  @Override
  public RestResponse<KsqlEntityList> makeKsqlRequest(final URI serverEndPoint, final String sql,
      final Map<String, ?> requestProperties) {
    return getClient().makeKsqlRequest(serverEndPoint, sql, requestProperties);
  }

  @Override
  public RestResponse<List<StreamedRow>> makeQueryRequest(
      final URI serverEndPoint,
      final String sql,
      final Map<String, ?> configOverrides,
      final Map<String, ?> requestProperties
  ) {
    return getClient().makeQueryRequest(serverEndPoint, sql, configOverrides, requestProperties);
  }

  @Override
  public RestResponse<Integer> makeQueryRequest(
      final URI serverEndPoint,
      final String sql,
      final Map<String, ?> configOverrides,
      final Map<String, ?> requestProperties,
      final Consumer<List<StreamedRow>> rowConsumer,
      final CompletableFuture<Void> shouldCloseConnection) {
    return getClient().makeQueryRequest(serverEndPoint, sql, configOverrides, requestProperties,
        rowConsumer, shouldCloseConnection);
  }

  @Override
  public CompletableFuture<RestResponse<BufferedPublisher<StreamedRow>>> makeQueryRequestStreamed(
      URI serverEndPoint, String sql, Map<String, ?> configOverrides,
      Map<String, ?> requestProperties) {
    return getClient().makeQueryRequestStreamed(serverEndPoint, sql, configOverrides,
        requestProperties);
  }

  @Override
  public void makeAsyncHeartbeatRequest(final URI serverEndPoint, final KsqlHostInfo host,
      final long timestamp) {
    getClient().makeAsyncHeartbeatRequest(serverEndPoint, host, timestamp);
  }

  @Override
  public RestResponse<ClusterStatusResponse> makeClusterStatusRequest(final URI serverEndPoint) {
    return getClient().makeClusterStatusRequest(serverEndPoint);
  }

  @Override
  public void makeAsyncLagReportRequest(
      final URI serverEndPoint,
      final LagReportingMessage lagReportingMessage) {
    getClient().makeAsyncLagReportRequest(serverEndPoint, lagReportingMessage);
  }

  @Override
  public void close() {
    try {
      workingClient.close();
    } catch (Throwable t) {
      LOG.error("Error closing client", t);
    }
  }
}
