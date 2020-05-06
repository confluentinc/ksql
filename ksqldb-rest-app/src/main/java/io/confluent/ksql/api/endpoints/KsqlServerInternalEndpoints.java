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

package io.confluent.ksql.api.endpoints;

import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;

import io.confluent.ksql.api.auth.ApiSecurityContext;
import io.confluent.ksql.api.spi.InternalEndpoints;
import io.confluent.ksql.rest.EndpointResponse;
import io.confluent.ksql.rest.entity.HeartbeatMessage;
import io.confluent.ksql.rest.entity.KsqlRequest;
import io.confluent.ksql.rest.entity.LagReportingMessage;
import io.confluent.ksql.rest.server.resources.ClusterStatusResource;
import io.confluent.ksql.rest.server.resources.HeartbeatResource;
import io.confluent.ksql.rest.server.resources.KsqlResource;
import io.confluent.ksql.rest.server.resources.LagReportingResource;
import io.confluent.ksql.rest.server.resources.streaming.StreamedQueryResource;
import io.vertx.core.WorkerExecutor;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

public class KsqlServerInternalEndpoints implements InternalEndpoints {

  private final Optional<HeartbeatResource> heartbeatResource;
  private final Optional<ClusterStatusResource> clusterStatusResource;
  private final Optional<LagReportingResource> lagReportingResource;
  private final KsqlResource ksqlResource;
  private final StreamedQueryResource streamedQueryResource;
  private final EndpointExecutor endpointExecutor;

  public KsqlServerInternalEndpoints(
      final KsqlSecurityContextProvider ksqlSecurityContextProvider,
      final Optional<HeartbeatResource> heartbeatResource,
      final Optional<ClusterStatusResource> clusterStatusResource,
      final Optional<LagReportingResource> lagReportingResource,
      final KsqlResource ksqlResource,
      final StreamedQueryResource streamedQueryResource
  ) {
    this.heartbeatResource = Objects.requireNonNull(heartbeatResource);
    this.clusterStatusResource = Objects.requireNonNull(clusterStatusResource);
    this.lagReportingResource = Objects.requireNonNull(lagReportingResource);
    this.ksqlResource = Objects.requireNonNull(ksqlResource);
    this.streamedQueryResource = Objects.requireNonNull(streamedQueryResource);
    this.endpointExecutor = new EndpointExecutor(
        Objects.requireNonNull(ksqlSecurityContextProvider));
  }


  @Override
  public CompletableFuture<EndpointResponse> executeHeartbeat(
      final HeartbeatMessage heartbeatMessage,
      final ApiSecurityContext apiSecurityContext) {
    return heartbeatResource.map(resource -> endpointExecutor.executeOldApiEndpoint(
        apiSecurityContext,
        ksqlSecurityContext -> resource.registerHeartbeat(heartbeatMessage)))
        .orElseGet(() -> CompletableFuture
            .completedFuture(EndpointResponse.failed(NOT_FOUND.code())));
  }

  @Override
  public CompletableFuture<EndpointResponse> executeClusterStatus(
      final ApiSecurityContext apiSecurityContext) {
    return clusterStatusResource.map(resource -> endpointExecutor.executeOldApiEndpoint(
        apiSecurityContext,
        ksqlSecurityContext -> resource.checkClusterStatus()))
        .orElseGet(() -> CompletableFuture
            .completedFuture(EndpointResponse.failed(NOT_FOUND.code())));
  }

  @Override
  public CompletableFuture<EndpointResponse> executeLagReport(
      final LagReportingMessage lagReportingMessage, final ApiSecurityContext apiSecurityContext) {
    return lagReportingResource.map(resource -> endpointExecutor.executeOldApiEndpoint(
        apiSecurityContext,
        ksqlSecurityContext -> resource.receiveHostLag(lagReportingMessage)))
        .orElseGet(() -> CompletableFuture
            .completedFuture(EndpointResponse.failed(NOT_FOUND.code())));
  }

  @Override
  public CompletableFuture<EndpointResponse> executeKsqlRequest(final KsqlRequest request,
      final WorkerExecutor workerExecutor,
      final ApiSecurityContext apiSecurityContext) {

    return endpointExecutor.executeOldApiEndpointOnWorker(apiSecurityContext,
        ksqlSecurityContext -> ksqlResource.handleKsqlStatements(
            ksqlSecurityContext,
            request), workerExecutor);
  }

  @Override
  public CompletableFuture<EndpointResponse> executeQueryRequest(final KsqlRequest request,
      final WorkerExecutor workerExecutor,
      final CompletableFuture<Void> connectionClosedFuture,
      final ApiSecurityContext apiSecurityContext) {
    return endpointExecutor.executeOldApiEndpointOnWorker(apiSecurityContext,
        ksqlSecurityContext -> streamedQueryResource.streamQuery(
            ksqlSecurityContext,
            request,
            connectionClosedFuture), workerExecutor);
  }
}
