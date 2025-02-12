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

import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.api.auth.ApiSecurityContext;
import io.confluent.ksql.api.impl.InsertsStreamEndpoint;
import io.confluent.ksql.api.impl.KsqlSecurityContextProvider;
import io.confluent.ksql.api.impl.QueryEndpoint;
import io.confluent.ksql.api.server.InsertResult;
import io.confluent.ksql.api.server.InsertsStreamSubscriber;
import io.confluent.ksql.api.server.MetricsCallbackHolder;
import io.confluent.ksql.api.spi.Endpoints;
import io.confluent.ksql.api.spi.QueryPublisher;
import io.confluent.ksql.engine.KsqlEngine;
import io.confluent.ksql.internal.PullQueryExecutorMetrics;
import io.confluent.ksql.rest.EndpointResponse;
import io.confluent.ksql.rest.entity.ClusterTerminateRequest;
import io.confluent.ksql.rest.entity.HeartbeatMessage;
import io.confluent.ksql.rest.entity.KsqlMediaType;
import io.confluent.ksql.rest.entity.KsqlRequest;
import io.confluent.ksql.rest.entity.LagReportingMessage;
import io.confluent.ksql.rest.server.query.QueryExecutor;
import io.confluent.ksql.rest.server.resources.ClusterStatusResource;
import io.confluent.ksql.rest.server.resources.HealthCheckResource;
import io.confluent.ksql.rest.server.resources.HeartbeatResource;
import io.confluent.ksql.rest.server.resources.KsqlResource;
import io.confluent.ksql.rest.server.resources.LagReportingResource;
import io.confluent.ksql.rest.server.resources.ServerInfoResource;
import io.confluent.ksql.rest.server.resources.ServerMetadataResource;
import io.confluent.ksql.rest.server.resources.StatusResource;
import io.confluent.ksql.rest.server.resources.streaming.StreamedQueryResource;
import io.confluent.ksql.rest.server.resources.streaming.WSQueryEndpoint;
import io.confluent.ksql.rest.util.AuthenticationUtil;
import io.confluent.ksql.security.KsqlAuthTokenProvider;
import io.confluent.ksql.security.KsqlSecurityContext;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.ReservedInternalTopics;
import io.confluent.ksql.util.VertxCompletableFuture;
import io.vertx.core.Context;
import io.vertx.core.MultiMap;
import io.vertx.core.WorkerExecutor;
import io.vertx.core.http.ServerWebSocket;
import io.vertx.core.json.JsonObject;
import java.time.Clock;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.function.Supplier;
import org.reactivestreams.Subscriber;

// CHECKSTYLE_RULES.OFF: ClassDataAbstractionCoupling
public class KsqlServerEndpoints implements Endpoints {
  // CHECKSTYLE_RULES.ON: ClassDataAbstractionCoupling

  private final KsqlEngine ksqlEngine;
  private final KsqlConfig ksqlConfig;
  private final ReservedInternalTopics reservedInternalTopics;
  private final KsqlSecurityContextProvider ksqlSecurityContextProvider;
  private final KsqlResource ksqlResource;
  private final StreamedQueryResource streamedQueryResource;
  private final ServerInfoResource serverInfoResource;
  private final Optional<HeartbeatResource> heartbeatResource;
  private final Optional<ClusterStatusResource> clusterStatusResource;
  private final StatusResource statusResource;
  private final Optional<LagReportingResource> lagReportingResource;
  private final HealthCheckResource healthCheckResource;
  private final ServerMetadataResource serverMetadataResource;
  private final WSQueryEndpoint wsQueryEndpoint;
  private final Optional<PullQueryExecutorMetrics> pullQueryMetrics;
  private final QueryExecutor queryExecutor;
  private final Optional<KsqlAuthTokenProvider> authTokenProvider;

  // CHECKSTYLE_RULES.OFF: ParameterNumber
  @SuppressFBWarnings(value = "EI_EXPOSE_REP2")
  public KsqlServerEndpoints(
      final KsqlEngine ksqlEngine,
      final KsqlConfig ksqlConfig,
      final KsqlSecurityContextProvider ksqlSecurityContextProvider,
      final KsqlResource ksqlResource,
      final StreamedQueryResource streamedQueryResource,
      final ServerInfoResource serverInfoResource,
      final Optional<HeartbeatResource> heartbeatResource,
      final Optional<ClusterStatusResource> clusterStatusResource,
      final StatusResource statusResource,
      final Optional<LagReportingResource> lagReportingResource,
      final HealthCheckResource healthCheckResource,
      final ServerMetadataResource serverMetadataResource,
      final WSQueryEndpoint wsQueryEndpoint,
      final Optional<PullQueryExecutorMetrics> pullQueryMetrics,
      final QueryExecutor queryExecutor,
      final Optional<KsqlAuthTokenProvider> authTokenProvider
  ) {

    // CHECKSTYLE_RULES.ON: ParameterNumber
    this.ksqlEngine = Objects.requireNonNull(ksqlEngine);
    this.ksqlConfig = Objects.requireNonNull(ksqlConfig);
    this.reservedInternalTopics = new ReservedInternalTopics(ksqlConfig);
    this.ksqlSecurityContextProvider = Objects.requireNonNull(ksqlSecurityContextProvider);
    this.ksqlResource = Objects.requireNonNull(ksqlResource);
    this.streamedQueryResource = Objects.requireNonNull(streamedQueryResource);
    this.serverInfoResource = Objects.requireNonNull(serverInfoResource);
    this.heartbeatResource = Objects.requireNonNull(heartbeatResource);
    this.clusterStatusResource = Objects.requireNonNull(clusterStatusResource);
    this.statusResource = Objects.requireNonNull(statusResource);
    this.lagReportingResource = Objects.requireNonNull(lagReportingResource);
    this.healthCheckResource = Objects.requireNonNull(healthCheckResource);
    this.serverMetadataResource = Objects.requireNonNull(serverMetadataResource);
    this.wsQueryEndpoint = Objects.requireNonNull(wsQueryEndpoint);
    this.pullQueryMetrics = Objects.requireNonNull(pullQueryMetrics);
    this.queryExecutor = queryExecutor;
    this.authTokenProvider = authTokenProvider;
  }

  @Override
  public CompletableFuture<QueryPublisher> createQueryPublisher(final String sql,
      final Map<String, Object> properties,
      final Map<String, Object> sessionVariables,
      final Map<String, Object> requestProperties,
      final Context context,
      final WorkerExecutor workerExecutor,
      final ApiSecurityContext apiSecurityContext,
      final MetricsCallbackHolder metricsCallbackHolder,
      final Optional<Boolean> isInternalRequest) {
    final KsqlSecurityContext ksqlSecurityContext = ksqlSecurityContextProvider
        .provide(apiSecurityContext);
    return executeOnWorker(() -> {
      try {
        return new QueryEndpoint(ksqlEngine, ksqlConfig, pullQueryMetrics, queryExecutor)
            .createQueryPublisher(
                sql,
                properties,
                sessionVariables,
                requestProperties,
                context,
                workerExecutor,
                ksqlSecurityContext.getServiceContext(),
                metricsCallbackHolder,
                isInternalRequest);
      } finally {
        ksqlSecurityContext.getServiceContext().close();
      }
    },
    workerExecutor);
  }

  @Override
  public CompletableFuture<InsertsStreamSubscriber> createInsertsSubscriber(final String target,
      final JsonObject properties,
      final Subscriber<InsertResult> acksSubscriber, final Context context,
      final WorkerExecutor workerExecutor,
      final ApiSecurityContext apiSecurityContext) {
    return executeOnWorker(
        () -> new InsertsStreamEndpoint(ksqlEngine, ksqlConfig, reservedInternalTopics)
            .createInsertsSubscriber(target, properties, acksSubscriber, context, workerExecutor,
                ksqlSecurityContextProvider.provide(apiSecurityContext).getServiceContext()),
        workerExecutor);
  }

  @Override
  public CompletableFuture<EndpointResponse> executeKsqlRequest(final KsqlRequest request,
      final WorkerExecutor workerExecutor,
      final ApiSecurityContext apiSecurityContext) {

    return executeOldApiEndpointOnWorker(apiSecurityContext,
        ksqlSecurityContext -> ksqlResource.handleKsqlStatements(
            ksqlSecurityContext,
            request), workerExecutor);
  }

  @Override
  public CompletableFuture<EndpointResponse> executeQueryRequest(
      final KsqlRequest request,
      final WorkerExecutor workerExecutor,
      final CompletableFuture<Void> connectionClosedFuture,
      final ApiSecurityContext apiSecurityContext,
      final Optional<Boolean> isInternalRequest,
      final KsqlMediaType mediaType,
      final MetricsCallbackHolder metricsCallbackHolder,
      final Context context
  ) {
    return executeOldApiEndpointOnWorker(apiSecurityContext,
        ksqlSecurityContext -> streamedQueryResource.streamQuery(
            ksqlSecurityContext,
            request,
            connectionClosedFuture,
            isInternalRequest,
            metricsCallbackHolder,
            context
        ), workerExecutor);
  }

  @Override
  public CompletableFuture<EndpointResponse> executeTerminate(
      final ClusterTerminateRequest request,
      final WorkerExecutor workerExecutor,
      final ApiSecurityContext apiSecurityContext) {
    return executeOldApiEndpointOnWorker(apiSecurityContext,
        ksqlSecurityContext -> ksqlResource.terminateCluster(
            ksqlSecurityContext,
            request), workerExecutor);
  }

  @Override
  public CompletableFuture<EndpointResponse> executeInfo(
      final ApiSecurityContext apiSecurityContext) {
    return executeOldApiEndpoint(apiSecurityContext,
        ksqlSecurityContext -> serverInfoResource.get());
  }

  @Override
  public CompletableFuture<EndpointResponse> executeHeartbeat(
      final HeartbeatMessage heartbeatMessage,
      final ApiSecurityContext apiSecurityContext) {
    return heartbeatResource.map(resource -> executeOldApiEndpoint(apiSecurityContext,
        ksqlSecurityContext -> resource.registerHeartbeat(heartbeatMessage)))
        .orElseGet(() -> CompletableFuture
            .completedFuture(EndpointResponse.failed(NOT_FOUND.code())));
  }

  @Override
  public CompletableFuture<EndpointResponse> executeClusterStatus(
      final ApiSecurityContext apiSecurityContext) {
    return clusterStatusResource.map(resource -> executeOldApiEndpoint(apiSecurityContext,
        ksqlSecurityContext -> resource.checkClusterStatus()))
        .orElseGet(() -> CompletableFuture
            .completedFuture(EndpointResponse.failed(NOT_FOUND.code())));
  }

  @Override
  public CompletableFuture<EndpointResponse> executeStatus(final String type, final String entity,
      final String action, final ApiSecurityContext apiSecurityContext) {
    return executeOldApiEndpoint(apiSecurityContext,
        ksqlSecurityContext -> statusResource.getStatus(type, entity, action));
  }

  @Override
  public CompletableFuture<EndpointResponse> executeIsValidProperty(final String property,
      final WorkerExecutor workerExecutor,
      final ApiSecurityContext apiSecurityContext) {
    return executeOldApiEndpointOnWorker(apiSecurityContext,
        ksqlSecurityContext -> ksqlResource.isValidProperty(
            property), workerExecutor);
  }

  @Override
  public CompletableFuture<EndpointResponse> executeAllStatuses(
      final ApiSecurityContext apiSecurityContext) {
    return executeOldApiEndpoint(apiSecurityContext,
        ksqlSecurityContext -> statusResource.getAllStatuses());
  }

  @Override
  public CompletableFuture<EndpointResponse> executeLagReport(
      final LagReportingMessage lagReportingMessage, final ApiSecurityContext apiSecurityContext) {
    return lagReportingResource.map(resource -> executeOldApiEndpoint(apiSecurityContext,
        ksqlSecurityContext -> resource.receiveHostLag(lagReportingMessage)))
        .orElseGet(() -> CompletableFuture
            .completedFuture(EndpointResponse.failed(NOT_FOUND.code())));
  }

  @Override
  public CompletableFuture<EndpointResponse> executeCheckHealth(
      final ApiSecurityContext apiSecurityContext) {
    return executeOldApiEndpoint(apiSecurityContext,
        ksqlSecurityContext -> healthCheckResource.checkHealth());
  }

  @Override
  public CompletableFuture<EndpointResponse> executeServerMetadata(
      final ApiSecurityContext apiSecurityContext) {
    return executeOldApiEndpoint(apiSecurityContext,
        ksqlSecurityContext -> serverMetadataResource.getServerMetadata());
  }

  @Override
  public CompletableFuture<EndpointResponse> executeServerMetadataClusterId(
      final ApiSecurityContext apiSecurityContext) {
    return executeOldApiEndpoint(apiSecurityContext,
        ksqlSecurityContext -> serverMetadataResource.getServerClusterId());
  }

  @Override
  public void executeWebsocketStream(final ServerWebSocket webSocket, final MultiMap requestParams,
      final WorkerExecutor workerExecutor,
      final ApiSecurityContext apiSecurityContext,
      final Context context) {
    executeOnWorker(() -> {
      final KsqlSecurityContext ksqlSecurityContext = ksqlSecurityContextProvider
          .provide(apiSecurityContext);
      try {
        wsQueryEndpoint.executeStreamQuery(
            webSocket,
            requestParams,
            ksqlSecurityContext,
            context,
            new AuthenticationUtil(Clock.systemUTC())
                .getTokenTimeout(apiSecurityContext.getAuthHeader(), ksqlConfig, authTokenProvider)
        );
      } finally {
        ksqlSecurityContext.getServiceContext().close();
      }
      return null;
    }, workerExecutor);
  }

  private <R> CompletableFuture<R> executeOnWorker(final Supplier<R> supplier,
      final WorkerExecutor workerExecutor) {
    final VertxCompletableFuture<R> vcf = new VertxCompletableFuture<>();
    workerExecutor.executeBlocking(promise -> promise.complete(supplier.get()), false, vcf);
    return vcf;
  }

  private CompletableFuture<EndpointResponse> executeOldApiEndpointOnWorker(
      final ApiSecurityContext apiSecurityContext,
      final Function<KsqlSecurityContext, EndpointResponse> functionCall,
      final WorkerExecutor workerExecutor) {

    final KsqlSecurityContext ksqlSecurityContext = ksqlSecurityContextProvider
        .provide(apiSecurityContext);

    return executeOnWorker(() -> {
      try {
        return functionCall.apply(ksqlSecurityContext);
      } finally {
        ksqlSecurityContext.getServiceContext().close();
      }
    }, workerExecutor);
  }

  private CompletableFuture<EndpointResponse> executeOldApiEndpoint(
      final ApiSecurityContext apiSecurityContext,
      final Function<KsqlSecurityContext, EndpointResponse> functionCall) {
    final KsqlSecurityContext ksqlSecurityContext = ksqlSecurityContextProvider
        .provide(apiSecurityContext);

    try {
      return CompletableFuture.completedFuture(functionCall.apply(ksqlSecurityContext));
    } finally {
      ksqlSecurityContext.getServiceContext().close();
    }
  }

}
