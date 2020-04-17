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
import io.confluent.ksql.api.spi.EndpointResponse;
import io.confluent.ksql.api.spi.Endpoints;
import io.confluent.ksql.api.spi.QueryPublisher;
import io.confluent.ksql.engine.KsqlEngine;
import io.confluent.ksql.rest.entity.ClusterTerminateRequest;
import io.confluent.ksql.rest.entity.HeartbeatMessage;
import io.confluent.ksql.rest.entity.KsqlRequest;
import io.confluent.ksql.rest.entity.LagReportingMessage;
import io.confluent.ksql.rest.server.execution.PullQueryExecutor;
import io.confluent.ksql.rest.server.resources.ClusterStatusResource;
import io.confluent.ksql.rest.server.resources.HealthCheckResource;
import io.confluent.ksql.rest.server.resources.HeartbeatResource;
import io.confluent.ksql.rest.server.resources.KsqlResource;
import io.confluent.ksql.rest.server.resources.LagReportingResource;
import io.confluent.ksql.rest.server.resources.ServerInfoResource;
import io.confluent.ksql.rest.server.resources.StatusResource;
import io.confluent.ksql.rest.server.resources.streaming.StreamedQueryResource;
import io.confluent.ksql.security.KsqlSecurityContext;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.ReservedInternalTopics;
import io.confluent.ksql.util.VertxCompletableFuture;
import io.vertx.core.Context;
import io.vertx.core.WorkerExecutor;
import io.vertx.core.json.JsonObject;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.http.HttpStatus;
import org.reactivestreams.Subscriber;

// CHECKSTYLE_RULES.OFF: ClassDataAbstractionCoupling
public class KsqlServerEndpoints implements Endpoints {
  // CHECKSTYLE_RULES.ON: ClassDataAbstractionCoupling

  private final KsqlEngine ksqlEngine;
  private final KsqlConfig ksqlConfig;
  private final PullQueryExecutor pullQueryExecutor;
  private final ReservedInternalTopics reservedInternalTopics;
  private final KsqlSecurityContextProvider ksqlSecurityContextProvider;
  private final KsqlStatementsEndpoint ksqlStatementsEndpoint;
  private final TerminateEndpoint terminateEndpoint;
  private final OldQueryEndpoint streamedQueryEndpoint;
  private final ServerInfoResource serverInfoResource;
  private final Optional<HeartbeatResource> heartbeatResource;
  private final Optional<ClusterStatusResource> clusterStatusResource;
  private final StatusResource statusResource;
  private final Optional<LagReportingResource> lagReportingResource;
  private final HealthCheckResource healthCheckResource;

  // CHECKSTYLE_RULES.OFF: ParameterNumber
  public KsqlServerEndpoints(
      final KsqlEngine ksqlEngine,
      final KsqlConfig ksqlConfig,
      final PullQueryExecutor pullQueryExecutor,
      final KsqlSecurityContextProvider ksqlSecurityContextProvider,
      final KsqlResource ksqlResource,
      final StreamedQueryResource streamedQueryResource,
      final ServerInfoResource serverInfoResource,
      final Optional<HeartbeatResource> heartbeatResource,
      final Optional<ClusterStatusResource> clusterStatusResource,
      final StatusResource statusResource,
      final Optional<LagReportingResource> lagReportingResource,
      final HealthCheckResource healthCheckResource) {

    // CHECKSTYLE_RULES.ON: ParameterNumber
    this.ksqlEngine = Objects.requireNonNull(ksqlEngine);
    this.ksqlConfig = Objects.requireNonNull(ksqlConfig);
    this.pullQueryExecutor = Objects.requireNonNull(pullQueryExecutor);
    this.reservedInternalTopics = new ReservedInternalTopics(ksqlConfig);
    this.ksqlSecurityContextProvider = Objects.requireNonNull(ksqlSecurityContextProvider);
    this.ksqlStatementsEndpoint = new KsqlStatementsEndpoint(ksqlResource);
    this.terminateEndpoint = new TerminateEndpoint(ksqlResource);
    this.streamedQueryEndpoint = new OldQueryEndpoint(streamedQueryResource);
    this.serverInfoResource = Objects.requireNonNull(serverInfoResource);
    this.heartbeatResource = Objects.requireNonNull(heartbeatResource);
    this.clusterStatusResource = Objects.requireNonNull(clusterStatusResource);
    this.statusResource = Objects.requireNonNull(statusResource);
    this.lagReportingResource = Objects.requireNonNull(lagReportingResource);
    this.healthCheckResource = Objects.requireNonNull(healthCheckResource);
  }

  @Override
  public CompletableFuture<QueryPublisher> createQueryPublisher(final String sql,
      final JsonObject properties,
      final Context context,
      final WorkerExecutor workerExecutor,
      final ApiSecurityContext apiSecurityContext) {
    return executeOnWorker(
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

    return executeOldApiEndpoint(apiSecurityContext,
        ksqlSecurityContext -> ksqlStatementsEndpoint.executeStatements(
            ksqlSecurityContext,
            request), workerExecutor);
  }

  @Override
  public CompletableFuture<EndpointResponse> executeQueryRequest(final KsqlRequest request,
      final WorkerExecutor workerExecutor,
      final CompletableFuture<Void> connectionClosedFuture,
      final ApiSecurityContext apiSecurityContext) {

    return executeOldApiEndpoint(apiSecurityContext,
        ksqlSecurityContext -> streamedQueryEndpoint.executeQuery(
            ksqlSecurityContext,
            request,
            connectionClosedFuture), workerExecutor);
  }

  @Override
  public CompletableFuture<EndpointResponse> executeTerminate(
      final ClusterTerminateRequest request,
      final WorkerExecutor workerExecutor,
      final ApiSecurityContext apiSecurityContext) {
    return executeOldApiEndpoint(apiSecurityContext,
        ksqlSecurityContext -> terminateEndpoint.executeTerminate(
            ksqlSecurityContext,
            request), workerExecutor);
  }

  @Override
  public CompletableFuture<EndpointResponse> executeInfo(
      final ApiSecurityContext apiSecurityContext) {
    return CompletableFuture
        .completedFuture(EndpointResponse.create(serverInfoResource.get()));
  }

  @Override
  public CompletableFuture<EndpointResponse> executeHeartbeat(
      final HeartbeatMessage heartbeatMessage,
      final ApiSecurityContext apiSecurityContext) {
    return heartbeatResource.map(resource -> CompletableFuture
        .completedFuture(
            EndpointResponse.create(resource.registerHeartbeat(heartbeatMessage))))
        .orElseGet(() -> CompletableFuture
            .completedFuture(EndpointResponse.create(HttpStatus.SC_NOT_FOUND, "Not found", null)));
  }

  @Override
  public CompletableFuture<EndpointResponse> executeClusterStatus(
      final ApiSecurityContext apiSecurityContext) {
    return clusterStatusResource.map(resource -> CompletableFuture
        .completedFuture(
            EndpointResponse.create(resource.checkClusterStatus())))
        .orElseGet(() -> CompletableFuture
            .completedFuture(EndpointResponse.create(HttpStatus.SC_NOT_FOUND, "Not found", null)));
  }

  @Override
  public CompletableFuture<EndpointResponse> executeStatus(final String type, final String entity,
      final String action, final ApiSecurityContext apiSecurityContext) {
    return CompletableFuture
        .completedFuture(EndpointResponse.create(statusResource.getStatus(type, entity, action)));
  }

  @Override
  public CompletableFuture<EndpointResponse> executeAllStatuses(
      final ApiSecurityContext apiSecurityContext) {
    return CompletableFuture
        .completedFuture(EndpointResponse.create(statusResource.getAllStatuses()));
  }

  @Override
  public CompletableFuture<EndpointResponse> executeLagReport(
      final LagReportingMessage lagReportingMessage, final ApiSecurityContext apiSecurityContext) {
    return lagReportingResource.map(resource -> CompletableFuture
        .completedFuture(
            EndpointResponse.create(resource.receiveHostLag(lagReportingMessage))))
        .orElseGet(() -> CompletableFuture
            .completedFuture(EndpointResponse.create(HttpStatus.SC_NOT_FOUND, "Not found", null)));
  }

  @Override
  public CompletableFuture<EndpointResponse> executeCheckHealth(
      final ApiSecurityContext apiSecurityContext) {
    return CompletableFuture
        .completedFuture(EndpointResponse.create(healthCheckResource.checkHealth()));
  }

  private <R> CompletableFuture<R> executeOnWorker(final Supplier<R> supplier,
      final WorkerExecutor workerExecutor) {
    final VertxCompletableFuture<R> vcf = new VertxCompletableFuture<>();
    workerExecutor.executeBlocking(promise -> promise.complete(supplier.get()), false, vcf);
    return vcf;
  }

  private CompletableFuture<EndpointResponse> executeOldApiEndpoint(
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

}
