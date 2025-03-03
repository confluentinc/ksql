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

package io.confluent.ksql.services;

import io.confluent.ksql.reactive.BufferedPublisher;
import io.confluent.ksql.rest.client.RestResponse;
import io.confluent.ksql.rest.entity.ClusterStatusResponse;
import io.confluent.ksql.rest.entity.KsqlEntityList;
import io.confluent.ksql.rest.entity.LagReportingMessage;
import io.confluent.ksql.rest.entity.StreamedRow;
import io.confluent.ksql.util.KsqlHostInfo;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/**
 * A KSQL client implementation for use when communication with other nodes is not supported.
 */
public final class DisabledKsqlClient implements SimpleKsqlClient {

  public static DisabledKsqlClient instance() {
    return new DisabledKsqlClient();
  }

  private DisabledKsqlClient() {
  }
  
  @Override
  public RestResponse<KsqlEntityList> makeKsqlRequest(
      final URI serverEndPoint,
      final String sql,
      final Map<String, ?> requestProperties) {
    throw new UnsupportedOperationException("KSQL client is disabled");
  }

  @Override
  public RestResponse<List<StreamedRow>> makeQueryRequest(
      final URI serverEndPoint,
      final String sql,
      final Map<String, ?> configOverrides,
      final Map<String, ?> requestProperties
  ) {
    throw new UnsupportedOperationException("KSQL client is disabled");
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
    throw new UnsupportedOperationException("KSQL client is disabled");
  }

  @Override
  public CompletableFuture<RestResponse<BufferedPublisher<StreamedRow>>> makeQueryRequestStreamed(
      final URI serverEndPoint,
      final String sql,
      final Map<String, ?> configOverrides,
      final Map<String, ?> requestProperties
  ) {
    throw new UnsupportedOperationException("KSQL client is disabled");
  }

  @Override
  public void makeAsyncHeartbeatRequest(
      final URI serverEndPoint,
      final KsqlHostInfo host,
      final long timestamp
  ) {
    throw new UnsupportedOperationException("KSQL client is disabled");
  }

  @Override
  public RestResponse<ClusterStatusResponse> makeClusterStatusRequest(final URI serverEndPoint) {
    throw new UnsupportedOperationException("KSQL client is disabled");
  }

  @Override
  public void makeAsyncLagReportRequest(
      final URI serverEndPoint,
      final LagReportingMessage lagReportingMessage
  ) {
    throw new UnsupportedOperationException("KSQL client is disabled");
  }

  @Override
  public void close() {
  }
}

