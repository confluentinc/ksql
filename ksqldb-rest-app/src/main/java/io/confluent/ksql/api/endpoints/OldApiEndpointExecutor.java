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
import io.confluent.ksql.rest.EndpointResponse;
import io.confluent.ksql.security.KsqlSecurityContext;
import io.confluent.ksql.util.VertxCompletableFuture;
import io.vertx.core.WorkerExecutor;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.function.Supplier;

public class OldApiEndpointExecutor {

  private final KsqlSecurityContextProvider ksqlSecurityContextProvider;

  public OldApiEndpointExecutor(final KsqlSecurityContextProvider ksqlSecurityContextProvider) {
    this.ksqlSecurityContextProvider = ksqlSecurityContextProvider;
  }


  <R> CompletableFuture<R> executeOnWorker(final Supplier<R> supplier,
      final WorkerExecutor workerExecutor) {
    final VertxCompletableFuture<R> vcf = new VertxCompletableFuture<>();
    workerExecutor.executeBlocking(promise -> promise.complete(supplier.get()), false, vcf);
    return vcf;
  }

  CompletableFuture<EndpointResponse> executeOldApiEndpointOnWorker(
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

  CompletableFuture<EndpointResponse> executeOldApiEndpoint(
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
