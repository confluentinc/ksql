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

package io.confluent.ksql.api.spi;

import io.confluent.ksql.api.auth.ApiSecurityContext;
import io.confluent.ksql.api.server.InsertResult;
import io.confluent.ksql.api.server.InsertsStreamSubscriber;
import io.confluent.ksql.api.server.MetricsCallbackHolder;
import io.confluent.ksql.rest.EndpointResponse;
import io.confluent.ksql.rest.entity.ClusterTerminateRequest;
import io.confluent.ksql.rest.entity.HeartbeatMessage;
import io.confluent.ksql.rest.entity.KsqlMediaType;
import io.confluent.ksql.rest.entity.KsqlRequest;
import io.confluent.ksql.rest.entity.LagReportingMessage;
import io.vertx.core.Context;
import io.vertx.core.MultiMap;
import io.vertx.core.WorkerExecutor;
import io.vertx.core.http.ServerWebSocket;
import io.vertx.core.json.JsonObject;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

/**
 * In order to keep a clean separation between the plumbing of the API server and actual back-end
 * implementation of the endpoints we define this interface to encapsulate the actual endpoint
 * implementation.
 */
public interface Endpoints {

  /**
   * Create a publisher that will publish the results of the query. This will be subscribed to from
   * the API server and the subscriber will write the results to the HTTP response
   *
   * @param sql            The sql of the query
   * @param properties     Optional properties for the query
   * @param context        The Vert.x context
   * @param workerExecutor The worker executor to use for blocking operations
   * @return A CompletableFuture representing the future result of the operation
   */
  CompletableFuture<Publisher<?>> createQueryPublisher(String sql,
      Map<String, Object> properties,
      Map<String, Object> sessionVariables, Map<String, Object> requestProperties,
      Context context, WorkerExecutor workerExecutor,
      ApiSecurityContext apiSecurityContext, MetricsCallbackHolder metricsCallbackHolder,
      Optional<Boolean> isInternalRequest);

  /**
   * Create a subscriber which will receive a stream of inserts from the API server and process
   * them. This method takes an optional acksSubsciber - if specified this is used to receive a
   * stream of acks from the back-end representing completion of the processing of the inserts
   *
   * @param target         The target to insert into
   * @param properties     Optional properties
   * @param acksSubscriber Optional subscriber of acks
   * @param context        The Vert.x context
   * @return A CompletableFuture representing the future result of the operation
   */
  CompletableFuture<InsertsStreamSubscriber> createInsertsSubscriber(String target,
      JsonObject properties,
      Subscriber<InsertResult> acksSubscriber, Context context, WorkerExecutor workerExecutor,
      ApiSecurityContext apiSecurityContext);

  /*
  The ported old API endpoints now follow
   */

  CompletableFuture<EndpointResponse> executeKsqlRequest(KsqlRequest request,
      WorkerExecutor workerExecutor, ApiSecurityContext apiSecurityContext);

  CompletableFuture<EndpointResponse> executeTerminate(ClusterTerminateRequest request,
      WorkerExecutor workerExecutor, ApiSecurityContext apiSecurityContext);

  CompletableFuture<EndpointResponse> executeQueryRequest(
      KsqlRequest request, WorkerExecutor workerExecutor,
      CompletableFuture<Void> connectionClosedFuture, ApiSecurityContext apiSecurityContext,
      Optional<Boolean> isInternalRequest,
      KsqlMediaType mediaType,
      MetricsCallbackHolder metricsCallbackHolder,
      Context context);

  CompletableFuture<EndpointResponse> executeInfo(ApiSecurityContext apiSecurityContext);

  CompletableFuture<EndpointResponse> executeHeartbeat(HeartbeatMessage heartbeatMessage,
      ApiSecurityContext apiSecurityContext);

  CompletableFuture<EndpointResponse> executeClusterStatus(ApiSecurityContext apiSecurityContext);

  CompletableFuture<EndpointResponse> executeStatus(String type, String entity, String action,
      ApiSecurityContext apiSecurityContext);

  CompletableFuture<EndpointResponse> executeIsValidProperty(String property,
      WorkerExecutor workerExecutor, ApiSecurityContext apiSecurityContext);

  CompletableFuture<EndpointResponse> executeAllStatuses(ApiSecurityContext apiSecurityContext);

  CompletableFuture<EndpointResponse> executeLagReport(LagReportingMessage lagReportingMessage,
      ApiSecurityContext apiSecurityContext);

  CompletableFuture<EndpointResponse> executeCheckHealth(ApiSecurityContext apiSecurityContext);

  CompletableFuture<EndpointResponse> executeServerMetadata(ApiSecurityContext apiSecurityContext);

  CompletableFuture<EndpointResponse> executeServerMetadataClusterId(
      ApiSecurityContext apiSecurityContext);

  // This is the legacy websocket based query streaming API
  void executeWebsocketStream(ServerWebSocket webSocket, MultiMap requstParams,
      WorkerExecutor workerExecutor, ApiSecurityContext apiSecurityContext,
      Context context);

  CompletableFuture<EndpointResponse> executeTest(
      String test, ApiSecurityContext apiSecurityContext);

}
