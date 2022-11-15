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

package io.confluent.ksql.api;

import com.google.common.collect.ImmutableList;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.api.auth.ApiSecurityContext;
import io.confluent.ksql.api.server.InsertResult;
import io.confluent.ksql.api.server.InsertsStreamSubscriber;
import io.confluent.ksql.api.server.MetricsCallbackHolder;
import io.confluent.ksql.api.spi.Endpoints;
import io.confluent.ksql.api.spi.QueryPublisher;
import io.confluent.ksql.api.utils.RowGenerator;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.reactive.BufferedPublisher;
import io.confluent.ksql.rest.EndpointResponse;
import io.confluent.ksql.rest.entity.ClusterTerminateRequest;
import io.confluent.ksql.rest.entity.HeartbeatMessage;
import io.confluent.ksql.rest.entity.KsqlEntity;
import io.confluent.ksql.rest.entity.KsqlEntityList;
import io.confluent.ksql.rest.entity.KsqlMediaType;
import io.confluent.ksql.rest.entity.KsqlRequest;
import io.confluent.ksql.rest.entity.LagReportingMessage;
import io.confluent.ksql.rest.entity.ServerClusterId;
import io.confluent.ksql.rest.entity.ServerInfo;
import io.confluent.ksql.util.AppInfo;
import io.vertx.core.Context;
import io.vertx.core.MultiMap;
import io.vertx.core.Vertx;
import io.vertx.core.WorkerExecutor;
import io.vertx.core.http.ServerWebSocket;
import io.vertx.core.json.JsonObject;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import org.reactivestreams.Subscriber;

public class TestEndpoints implements Endpoints {

  private Supplier<RowGenerator> rowGeneratorFactory;
  private TestInsertsSubscriber insertsSubscriber;
  private KsqlEntityList ksqlEndpointResponse;
  private String lastSql;
  private JsonObject lastProperties;
  private JsonObject lastSessionVariables;
  private String lastTarget;
  private final Set<TestQueryPublisher> queryPublishers = new HashSet<>();
  private int acksBeforePublisherError = -1;
  private int rowsBeforePublisherError = -1;
  private RuntimeException createQueryPublisherException;
  private RuntimeException createInsertsSubscriberException;
  private RuntimeException executeKsqlRequestException;
  private ApiSecurityContext lastApiSecurityContext;
  private int queryCount = 0;

  @Override
  public synchronized CompletableFuture<QueryPublisher> createQueryPublisher(
      final String sql,
      final Map<String, Object> properties,
      final Map<String, Object> sessionVariables,
      final Map<String, Object> requestProperties,
      final Context context,
      final WorkerExecutor workerExecutor,
      final ApiSecurityContext apiSecurityContext,
      final MetricsCallbackHolder metricsCallbackHolder,
      final Optional<Boolean> isInternalRequest) {
    CompletableFuture<QueryPublisher> completableFuture = new CompletableFuture<>();
    if (createQueryPublisherException != null) {
      createQueryPublisherException.fillInStackTrace();
      completableFuture.completeExceptionally(createQueryPublisherException);
    } else {
      this.lastSql = sql;
      this.lastProperties = new JsonObject(properties);
      this.lastSessionVariables = new JsonObject(sessionVariables);
      this.lastApiSecurityContext = apiSecurityContext;
      final boolean push = sql.toLowerCase().contains("emit changes");
      final int limit = extractLimit(sql);
      final TestQueryPublisher queryPublisher = new TestQueryPublisher(context,
          rowGeneratorFactory.get(),
          rowsBeforePublisherError,
          push,
          limit,
          new QueryId("queryId" + (queryCount > 0 ? queryCount : "")));
      queryPublishers.add(queryPublisher);
      completableFuture.complete(queryPublisher);
      queryCount++;
    }
    return completableFuture;
  }

  @Override
  public synchronized CompletableFuture<InsertsStreamSubscriber> createInsertsSubscriber(
      final String target,
      final JsonObject properties,
      final Subscriber<InsertResult> acksSubscriber,
      final Context context,
      final WorkerExecutor workerExecutor,
      final ApiSecurityContext apiSecurityContext) {
    CompletableFuture<InsertsStreamSubscriber> completableFuture = new CompletableFuture<>();
    if (createInsertsSubscriberException != null) {
      createInsertsSubscriberException.fillInStackTrace();
      completableFuture.completeExceptionally(createInsertsSubscriberException);
    } else {
      this.lastTarget = target;
      this.lastProperties = properties.copy();
      this.lastApiSecurityContext = apiSecurityContext;
      BufferedPublisher<InsertResult> acksPublisher = new BufferedPublisher<>(
          Vertx.currentContext());
      acksPublisher.subscribe(acksSubscriber);
      this.insertsSubscriber = new TestInsertsSubscriber(Vertx.currentContext(), acksPublisher,
          acksBeforePublisherError);
      completableFuture.complete(insertsSubscriber);
    }
    return completableFuture;
  }

  @Override
  public synchronized CompletableFuture<EndpointResponse> executeKsqlRequest(
      final KsqlRequest request,
      final WorkerExecutor workerExecutor,
      final ApiSecurityContext apiSecurityContext) {
    this.lastSql = request.getUnmaskedKsql();
    this.lastProperties = new JsonObject(request.getConfigOverrides());
    this.lastSessionVariables = new JsonObject(request.getSessionVariables());
    this.lastApiSecurityContext = apiSecurityContext;
    CompletableFuture<EndpointResponse> cf = new CompletableFuture<>();

    if (executeKsqlRequestException != null) {
      executeKsqlRequestException.fillInStackTrace();
      cf.completeExceptionally(executeKsqlRequestException);
    } else {
      cf.complete(EndpointResponse.ok(ksqlEndpointResponse));
    }

    return cf;
  }

  @Override
  public CompletableFuture<EndpointResponse> executeTerminate(final ClusterTerminateRequest request,
      final WorkerExecutor workerExecutor, final ApiSecurityContext apiSecurityContext) {
    return null;
  }

  @Override
  public CompletableFuture<EndpointResponse> executeQueryRequest(KsqlRequest request,
      WorkerExecutor workerExecutor, CompletableFuture<Void> connectionClosedFuture,
      ApiSecurityContext apiSecurityContext, Optional<Boolean> isInternalRequest,
      KsqlMediaType mediaType, final MetricsCallbackHolder metricsCallbackHolder, Context context) {
    return null;
  }

  @Override
  public synchronized CompletableFuture<EndpointResponse> executeInfo(ApiSecurityContext apiSecurityContext) {
    this.lastApiSecurityContext = apiSecurityContext;
    final ServerInfo entity = new ServerInfo(
        AppInfo.getVersion(), "kafka-cluster-id", "ksql-service-id", "server-status");
    return CompletableFuture.completedFuture(EndpointResponse.ok(entity));
  }

  @Override
  public CompletableFuture<EndpointResponse> executeHeartbeat(HeartbeatMessage heartbeatMessage,
      ApiSecurityContext apiSecurityContext) {
    return null;
  }

  @Override
  public CompletableFuture<EndpointResponse> executeClusterStatus(
      ApiSecurityContext apiSecurityContext) {
    return null;
  }

  @Override
  public CompletableFuture<EndpointResponse> executeStatus(String type, String entity,
      String action, ApiSecurityContext apiSecurityContext) {
    return null;
  }

  @Override
  public CompletableFuture<EndpointResponse> executeIsValidProperty(String property,
      WorkerExecutor workerExecutor, ApiSecurityContext apiSecurityContext) {
    return null;
  }

  @Override
  public CompletableFuture<EndpointResponse> executeAllStatuses(
      ApiSecurityContext apiSecurityContext) {
    return null;
  }

  @Override
  public CompletableFuture<EndpointResponse> executeLagReport(
      LagReportingMessage lagReportingMessage, ApiSecurityContext apiSecurityContext) {
    return null;
  }

  @Override
  public CompletableFuture<EndpointResponse> executeCheckHealth(
      ApiSecurityContext apiSecurityContext) {
    return null;
  }

  @Override
  public CompletableFuture<EndpointResponse> executeServerMetadata(
      ApiSecurityContext apiSecurityContext) {
    return null;
  }

  @Override
  public synchronized CompletableFuture<EndpointResponse> executeServerMetadataClusterId(
      ApiSecurityContext apiSecurityContext) {
    this.lastApiSecurityContext = apiSecurityContext;
    final ServerClusterId entity = ServerClusterId.of("kafka-cluster-id", "ksql-service-id");
    return CompletableFuture.completedFuture(EndpointResponse.ok(entity));
  }

  @Override
  public void executeWebsocketStream(ServerWebSocket webSocket, MultiMap requstParams,
      WorkerExecutor workerExecutor, ApiSecurityContext apiSecurityContext, Context context) {

  }

  public synchronized void setRowGeneratorFactory(
      final Supplier<RowGenerator> rowGeneratorFactory) {
    this.rowGeneratorFactory = rowGeneratorFactory;
  }

  public synchronized TestInsertsSubscriber getInsertsSubscriber() {
    return insertsSubscriber;
  }

  public synchronized void setKsqlEndpointResponse(final List<KsqlEntity> entities) {
    this.ksqlEndpointResponse = new KsqlEntityList(ImmutableList.copyOf(entities));
  }

  public synchronized String getLastSql() {
    return lastSql;
  }

  public synchronized JsonObject getLastProperties() {
    return lastProperties.copy();
  }

  public synchronized JsonObject getLastSessionVariables() {
    return lastSessionVariables.copy();
  }

  public synchronized Set<TestQueryPublisher> getQueryPublishers() {
    return Collections.unmodifiableSet(queryPublishers);
  }

  public synchronized String getLastTarget() {
    return lastTarget;
  }

  public synchronized ApiSecurityContext getLastApiSecurityContext() {
    return lastApiSecurityContext;
  }

  public synchronized void setAcksBeforePublisherError(final int acksBeforePublisherError) {
    this.acksBeforePublisherError = acksBeforePublisherError;
  }

  public synchronized void setRowsBeforePublisherError(final int rowsBeforePublisherError) {
    this.rowsBeforePublisherError = rowsBeforePublisherError;
  }

  @SuppressFBWarnings(value = "EI_EXPOSE_REP2")
  public synchronized void setCreateQueryPublisherException(final RuntimeException exception) {
    this.createQueryPublisherException = exception;
  }

  @SuppressFBWarnings(value = "EI_EXPOSE_REP2")
  public synchronized void setCreateInsertsSubscriberException(final RuntimeException exception) {
    this.createInsertsSubscriberException = exception;
  }

  @SuppressFBWarnings(value = "EI_EXPOSE_REP2")
  public synchronized void setExecuteKsqlRequestException(final RuntimeException exception) {
    this.executeKsqlRequestException = exception;
  }

  private static int extractLimit(final String sql) {
    final int ind = sql.toLowerCase().indexOf("limit");
    if (ind == -1) {
      return -1;
    }

    // extract the string between "limit" and the following semicolon
    final String limit = sql.substring(ind + 5, ind + sql.substring(ind).indexOf(";")).trim();
    return Integer.parseInt(limit);
  }
}

