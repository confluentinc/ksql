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

import io.confluent.ksql.api.auth.ApiSecurityContext;
import io.confluent.ksql.api.server.InsertResult;
import io.confluent.ksql.api.server.InsertsStreamSubscriber;
import io.confluent.ksql.api.spi.Endpoints;
import io.confluent.ksql.api.spi.QueryPublisher;
import io.confluent.ksql.engine.KsqlEngine;
import io.confluent.ksql.rest.EndpointResponse;
import io.confluent.ksql.rest.entity.ClusterTerminateRequest;
import io.confluent.ksql.rest.entity.KsqlRequest;
import io.confluent.ksql.rest.server.execution.PullQueryExecutor;
import io.confluent.ksql.rest.server.resources.HealthCheckResource;
import io.confluent.ksql.rest.server.resources.KsqlResource;
import io.confluent.ksql.rest.server.resources.ServerInfoResource;
import io.confluent.ksql.rest.server.resources.ServerMetadataResource;
import io.confluent.ksql.rest.server.resources.StatusResource;
import io.confluent.ksql.rest.server.resources.streaming.StreamedQueryResource;
import io.confluent.ksql.rest.server.resources.streaming.WSQueryEndpoint;
import io.confluent.ksql.security.KsqlSecurityContext;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.ReservedInternalTopics;
import io.vertx.core.Context;
import io.vertx.core.MultiMap;
import io.vertx.core.WorkerExecutor;
import io.vertx.core.http.ServerWebSocket;
import io.vertx.core.json.JsonObject;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import org.reactivestreams.Subscriber;

// CHECKSTYLE_RULES.OFF: ClassDataAbstractionCoupling
public class KsqlServerEndpoints implements Endpoints {
  // CHECKSTYLE_RULES.ON: ClassDataAbstractionCoupling

  private final KsqlEngine ksqlEngine;
  private final KsqlConfig ksqlConfig;
  private final PullQueryExecutor pullQueryExecutor;
  private final ReservedInternalTopics reservedInternalTopics;
  private final KsqlSecurityContextProvider ksqlSecurityContextProvider;
  private final KsqlResource ksqlResource;
  private final StreamedQueryResource streamedQueryResource;
  private final ServerInfoResource serverInfoResource;
  private final StatusResource statusResource;
  private final HealthCheckResource healthCheckResource;
  private final ServerMetadataResource serverMetadataResource;
  private final WSQueryEndpoint wsQueryEndpoint;
  private final EndpointExecutor endpointExecutor;

  // CHECKSTYLE_RULES.OFF: ParameterNumber
  public KsqlServerEndpoints(
      final KsqlEngine ksqlEngine,
      final KsqlConfig ksqlConfig,
      final PullQueryExecutor pullQueryExecutor,
      final KsqlSecurityContextProvider ksqlSecurityContextProvider,
      final KsqlResource ksqlResource,
      final StreamedQueryResource streamedQueryResource,
      final ServerInfoResource serverInfoResource,
      final StatusResource statusResource,
      final HealthCheckResource healthCheckResource,
      final ServerMetadataResource serverMetadataResource,
      final WSQueryEndpoint wsQueryEndpoint) {

    // CHECKSTYLE_RULES.ON: ParameterNumber
    this.ksqlEngine = Objects.requireNonNull(ksqlEngine);
    this.ksqlConfig = Objects.requireNonNull(ksqlConfig);
    this.pullQueryExecutor = Objects.requireNonNull(pullQueryExecutor);
    this.reservedInternalTopics = new ReservedInternalTopics(ksqlConfig);
    this.ksqlSecurityContextProvider = Objects.requireNonNull(ksqlSecurityContextProvider);
    this.ksqlResource = Objects.requireNonNull(ksqlResource);
    this.streamedQueryResource = Objects.requireNonNull(streamedQueryResource);
    this.serverInfoResource = Objects.requireNonNull(serverInfoResource);
    this.statusResource = Objects.requireNonNull(statusResource);
    this.healthCheckResource = Objects.requireNonNull(healthCheckResource);
    this.serverMetadataResource = Objects.requireNonNull(serverMetadataResource);
    this.wsQueryEndpoint = Objects.requireNonNull(wsQueryEndpoint);
    this.endpointExecutor = new EndpointExecutor(
        Objects.requireNonNull(ksqlSecurityContextProvider));
  }

  @Override
  public CompletableFuture<QueryPublisher> createQueryPublisher(final String sql,
      final JsonObject properties,
      final Context context,
      final WorkerExecutor workerExecutor,
      final ApiSecurityContext apiSecurityContext) {
    return endpointExecutor.executeOnWorker(
        () -> new QueryEndpoint(ksqlEngine, ksqlConfig, pullQueryExecutor)
            .createQueryPublisher(sql, properties, context, workerExecutor,
                ksqlSecurityContextProvider.provide(apiSecurityContext).getServiceContext()),
        workerExecutor);
  }

  @Override
  public CompletableFuture<InsertsStreamSubscriber> createInsertsSubscriber(final String target,
      final JsonObject properties,
      final Subscriber<InsertResult> acksSubscriber, final Context context,
      final WorkerExecutor workerExecutor,
      final ApiSecurityContext apiSecurityContext) {
    return endpointExecutor.executeOnWorker(
        () -> new InsertsStreamEndpoint(ksqlEngine, ksqlConfig, reservedInternalTopics)
            .createInsertsSubscriber(target, properties, acksSubscriber, context, workerExecutor,
                ksqlSecurityContextProvider.provide(apiSecurityContext).getServiceContext()),
        workerExecutor);
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

  @Override
  public CompletableFuture<EndpointResponse> executeTerminate(
      final ClusterTerminateRequest request,
      final WorkerExecutor workerExecutor,
      final ApiSecurityContext apiSecurityContext) {
    return endpointExecutor.executeOldApiEndpointOnWorker(apiSecurityContext,
        ksqlSecurityContext -> ksqlResource.terminateCluster(
            ksqlSecurityContext,
            request), workerExecutor);
  }

  @Override
  public CompletableFuture<EndpointResponse> executeInfo(
      final ApiSecurityContext apiSecurityContext) {
    return endpointExecutor.executeOldApiEndpoint(apiSecurityContext,
        ksqlSecurityContext -> serverInfoResource.get());
  }

  @Override
  public CompletableFuture<EndpointResponse> executeStatus(final String type, final String entity,
      final String action, final ApiSecurityContext apiSecurityContext) {
    return endpointExecutor.executeOldApiEndpoint(apiSecurityContext,
        ksqlSecurityContext -> statusResource.getStatus(type, entity, action));
  }

  @Override
  public CompletableFuture<EndpointResponse> executeAllStatuses(
      final ApiSecurityContext apiSecurityContext) {
    return endpointExecutor.executeOldApiEndpoint(apiSecurityContext,
        ksqlSecurityContext -> statusResource.getAllStatuses());
  }

  @Override
  public CompletableFuture<EndpointResponse> executeCheckHealth(
      final ApiSecurityContext apiSecurityContext) {
    return endpointExecutor.executeOldApiEndpoint(apiSecurityContext,
        ksqlSecurityContext -> healthCheckResource.checkHealth());
  }

  @Override
  public CompletableFuture<EndpointResponse> executeServerMetadata(
      final ApiSecurityContext apiSecurityContext) {
    return endpointExecutor.executeOldApiEndpoint(apiSecurityContext,
        ksqlSecurityContext -> serverMetadataResource.getServerMetadata());
  }

  @Override
  public CompletableFuture<EndpointResponse> executeServerMetadataClusterId(
      final ApiSecurityContext apiSecurityContext) {
    return endpointExecutor.executeOldApiEndpoint(apiSecurityContext,
        ksqlSecurityContext -> serverMetadataResource.getServerClusterId());
  }

  @Override
  public void executeWebsocketStream(final ServerWebSocket webSocket, final MultiMap requestParams,
      final WorkerExecutor workerExecutor,
      final ApiSecurityContext apiSecurityContext) {

    endpointExecutor.executeOnWorker(() -> {
      final KsqlSecurityContext ksqlSecurityContext = ksqlSecurityContextProvider
          .provide(apiSecurityContext);
      try {
        wsQueryEndpoint
            .executeStreamQuery(webSocket, requestParams, ksqlSecurityContext);
      } finally {
        ksqlSecurityContext.getServiceContext().close();
      }
      return null;
    }, workerExecutor);
  }
}
