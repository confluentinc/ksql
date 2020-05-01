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
import io.confluent.ksql.rest.EndpointResponse;
import io.confluent.ksql.rest.entity.HeartbeatMessage;
import io.confluent.ksql.rest.entity.KsqlRequest;
import io.confluent.ksql.rest.entity.LagReportingMessage;
import io.vertx.core.WorkerExecutor;
import java.util.concurrent.CompletableFuture;

/**
 * In order to keep a clean separation between the plumbing of the API server and actual back-end
 * implementation of the endpoints we define this interface to encapsulate the actual internal
 * endpoint implementation.
 */
public interface InternalEndpoints {

  /*
  The ported old API endpoints follow
 */
  CompletableFuture<EndpointResponse> executeHeartbeat(HeartbeatMessage heartbeatMessage,
      ApiSecurityContext apiSecurityContext);

  CompletableFuture<EndpointResponse> executeClusterStatus(ApiSecurityContext apiSecurityContext);

  CompletableFuture<EndpointResponse> executeLagReport(LagReportingMessage lagReportingMessage,
      ApiSecurityContext apiSecurityContext);

  CompletableFuture<EndpointResponse> executeKsqlRequest(KsqlRequest request,
      WorkerExecutor workerExecutor, ApiSecurityContext apiSecurityContext);

  CompletableFuture<EndpointResponse> executeQueryRequest(KsqlRequest request,
      WorkerExecutor workerExecutor, CompletableFuture<Void> connectionClosedFuture,
      ApiSecurityContext apiSecurityContext);
}
