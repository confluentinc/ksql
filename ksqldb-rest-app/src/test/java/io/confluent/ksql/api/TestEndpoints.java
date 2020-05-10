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

import io.confluent.ksql.api.auth.ApiSecurityContext;
import io.confluent.ksql.api.server.InsertResult;
import io.confluent.ksql.api.server.InsertsStreamSubscriber;
import io.confluent.ksql.api.spi.Endpoints;
import io.confluent.ksql.api.spi.QueryPublisher;
import io.confluent.ksql.api.utils.RowGenerator;
import io.confluent.ksql.reactive.BufferedPublisher;
import io.confluent.ksql.rest.EndpointResponse;
import io.confluent.ksql.rest.entity.ClusterTerminateRequest;
import io.confluent.ksql.rest.entity.HeartbeatMessage;
import io.confluent.ksql.rest.entity.KsqlRequest;
import io.confluent.ksql.rest.entity.LagReportingMessage;
import io.confluent.ksql.rest.entity.StreamsList;
import io.vertx.core.Context;
import io.vertx.core.MultiMap;
import io.vertx.core.Vertx;
import io.vertx.core.WorkerExecutor;
import io.vertx.core.http.ServerWebSocket;
import io.vertx.core.json.JsonObject;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import org.reactivestreams.Subscriber;

public class TestEndpoints implements Endpoints {

  private Supplier<RowGenerator> rowGeneratorFactory;
  private TestInsertsSubscriber insertsSubscriber;
  private String lastSql;
  private JsonObject lastProperties;
  private String lastTarget;
  private Set<TestQueryPublisher> queryPublishers = new HashSet<>();
  private int acksBeforePublisherError = -1;
  private int rowsBeforePublisherError = -1;
  private RuntimeException createQueryPublisherException;
  private ApiSecurityContext lastApiSecurityContext;

  @Override
  public synchronized CompletableFuture<QueryPublisher> createQueryPublisher(final String sql,
      final JsonObject properties, final Context context, final WorkerExecutor workerExecutor,
      final ApiSecurityContext apiSecurityContext) {
    CompletableFuture<QueryPublisher> completableFuture = new CompletableFuture<>();
    if (createQueryPublisherException != null) {
      createQueryPublisherException.fillInStackTrace();
      completableFuture.completeExceptionally(createQueryPublisherException);
    } else {
      this.lastSql = sql;
      this.lastProperties = properties;
      this.lastApiSecurityContext = apiSecurityContext;
      final boolean push = sql.toLowerCase().contains("emit changes");
      final int limit = extractLimit(sql);
      final TestQueryPublisher queryPublisher = new TestQueryPublisher(context,
          rowGeneratorFactory.get(),
          rowsBeforePublisherError,
          push,
          limit);
      queryPublishers.add(queryPublisher);
      completableFuture.complete(queryPublisher);
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
    this.lastTarget = target;
    this.lastProperties = properties;
    this.lastApiSecurityContext = apiSecurityContext;
    BufferedPublisher<InsertResult> acksPublisher = new BufferedPublisher<>(Vertx.currentContext());
    acksPublisher.subscribe(acksSubscriber);
    this.insertsSubscriber = new TestInsertsSubscriber(Vertx.currentContext(), acksPublisher,
        acksBeforePublisherError);
    return CompletableFuture.completedFuture(insertsSubscriber);
  }

  @Override
  public synchronized CompletableFuture<EndpointResponse> executeKsqlRequest(
      final KsqlRequest request,
      final WorkerExecutor workerExecutor,
      final ApiSecurityContext apiSecurityContext) {
    this.lastSql = request.getKsql();
    this.lastProperties = new JsonObject(request.getRequestProperties());
    this.lastApiSecurityContext = apiSecurityContext;
    if (request.getKsql().toLowerCase().equals("show streams;")) {
      final StreamsList entity = new StreamsList(request.getKsql(), Collections.emptyList());
      return CompletableFuture.completedFuture(EndpointResponse.ok(entity));
    } else {
      return null;
    }
  }

  @Override
  public CompletableFuture<EndpointResponse> executeTerminate(final ClusterTerminateRequest request,
      final WorkerExecutor workerExecutor, final ApiSecurityContext apiSecurityContext) {
    return null;
  }

  @Override
  public CompletableFuture<EndpointResponse> executeQueryRequest(KsqlRequest request,
      WorkerExecutor workerExecutor, CompletableFuture<Void> connectionClosedFuture,
      ApiSecurityContext apiSecurityContext) {
    return null;
  }

  @Override
  public CompletableFuture<EndpointResponse> executeInfo(ApiSecurityContext apiSecurityContext) {
    return null;
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
  public CompletableFuture<EndpointResponse> executeServerMetadataClusterId(
      ApiSecurityContext apiSecurityContext) {
    return null;
  }

  @Override
  public void executeWebsocketStream(ServerWebSocket webSocket, MultiMap requstParams,
      WorkerExecutor workerExecutor, ApiSecurityContext apiSecurityContext) {

  }

  public synchronized void setRowGeneratorFactory(
      final Supplier<RowGenerator> rowGeneratorFactory) {
    this.rowGeneratorFactory = rowGeneratorFactory;
  }

  public synchronized TestInsertsSubscriber getInsertsSubscriber() {
    return insertsSubscriber;
  }

  public synchronized String getLastSql() {
    return lastSql;
  }

  public synchronized JsonObject getLastProperties() {
    return lastProperties;
  }

  public synchronized Set<TestQueryPublisher> getQueryPublishers() {
    return queryPublishers;
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

  public synchronized void setCreateQueryPublisherException(final RuntimeException exception) {
    this.createQueryPublisherException = exception;
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

