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
import io.confluent.ksql.rest.entity.KsqlRequest;
import io.confluent.ksql.rest.server.execution.PullQueryExecutor;
import io.confluent.ksql.rest.server.resources.KsqlResource;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.ReservedInternalTopics;
import io.confluent.ksql.util.VertxCompletableFuture;
import io.vertx.core.Context;
import io.vertx.core.WorkerExecutor;
import io.vertx.core.json.JsonObject;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import org.reactivestreams.Subscriber;

// CHECKSTYLE_RULES.OFF: ClassDataAbstractionCoupling
public class KsqlServerEndpoints implements Endpoints {
  // CHECKSTYLE_RULES.ON: ClassDataAbstractionCoupling

  private final KsqlEngine ksqlEngine;
  private final KsqlConfig ksqlConfig;
  private final PullQueryExecutor pullQueryExecutor;
  private final ReservedInternalTopics reservedInternalTopics;
  private final KsqlSecurityContextProvider ksqlSecurityContextProvider;
  private final KsqlStatementsEndpoint ksqlEndpoint;

  public KsqlServerEndpoints(
      final KsqlEngine ksqlEngine,
      final KsqlConfig ksqlConfig,
      final PullQueryExecutor pullQueryExecutor,
      final KsqlSecurityContextProvider ksqlSecurityContextProvider,
      final KsqlResource ksqlResource) {
    this.ksqlEngine = Objects.requireNonNull(ksqlEngine);
    this.ksqlConfig = Objects.requireNonNull(ksqlConfig);
    this.pullQueryExecutor = Objects.requireNonNull(pullQueryExecutor);
    this.reservedInternalTopics = new ReservedInternalTopics(ksqlConfig);
    this.ksqlSecurityContextProvider = Objects.requireNonNull(ksqlSecurityContextProvider);
    this.ksqlEndpoint = new KsqlStatementsEndpoint(ksqlResource);
  }

  @Override
  public CompletableFuture<QueryPublisher> createQueryPublisher(final String sql,
      final JsonObject properties,
      final Context context,
      final WorkerExecutor workerExecutor,
      final ApiSecurityContext apiSecurityContext) {
    return executeOnWorker(
        () -> new QueryStreamEndpoint(ksqlEngine, ksqlConfig, pullQueryExecutor)
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
    return executeOnWorker(
        () -> ksqlEndpoint
            .executeStatements(
                ksqlSecurityContextProvider.provide(apiSecurityContext),
                request),
        workerExecutor);
  }

  private <R> CompletableFuture<R> executeOnWorker(final Supplier<R> supplier,
      final WorkerExecutor workerExecutor) {
    final VertxCompletableFuture<R> vcf = new VertxCompletableFuture<>();
    workerExecutor.executeBlocking(promise -> promise.complete(supplier.get()), vcf);
    return vcf;
  }

}
