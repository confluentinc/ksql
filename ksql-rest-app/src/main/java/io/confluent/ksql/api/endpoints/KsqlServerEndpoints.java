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

import io.confluent.ksql.api.server.ApiSecurityContext;
import io.confluent.ksql.api.server.InsertResult;
import io.confluent.ksql.api.server.InsertsStreamSubscriber;
import io.confluent.ksql.api.spi.Endpoints;
import io.confluent.ksql.api.spi.QueryPublisher;
import io.confluent.ksql.engine.KsqlEngine;
import io.confluent.ksql.rest.server.execution.PullQueryExecutor;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.ReservedInternalTopics;
import io.vertx.core.Context;
import io.vertx.core.WorkerExecutor;
import io.vertx.core.json.JsonObject;
import java.util.Objects;
import org.reactivestreams.Subscriber;

public class KsqlServerEndpoints implements Endpoints {

  private final KsqlEngine ksqlEngine;
  private final KsqlConfig ksqlConfig;
  private final PullQueryExecutor pullQueryExecutor;
  private final ReservedInternalTopics reservedInternalTopics;
  private final KsqlSecurityContextProvider ksqlSecurityContextProvider;

  public KsqlServerEndpoints(
      final KsqlEngine ksqlEngine,
      final KsqlConfig ksqlConfig,
      final PullQueryExecutor pullQueryExecutor,
      final KsqlSecurityContextProvider ksqlSecurityContextProvider) {
    this.ksqlEngine = Objects.requireNonNull(ksqlEngine);
    this.ksqlConfig = Objects.requireNonNull(ksqlConfig);
    this.pullQueryExecutor = Objects.requireNonNull(pullQueryExecutor);
    this.reservedInternalTopics = new ReservedInternalTopics(ksqlConfig);
    this.ksqlSecurityContextProvider = Objects.requireNonNull(ksqlSecurityContextProvider);
  }

  @Override
  public QueryPublisher createQueryPublisher(final String sql, final JsonObject properties,
      final Context context,
      final WorkerExecutor workerExecutor,
      final ApiSecurityContext apiSecurityContext) {
    return new QueryStreamEndpoint(ksqlEngine, ksqlConfig, pullQueryExecutor)
        .createQueryPublisher(sql, properties, context, workerExecutor,
            createServiceContext(apiSecurityContext));
  }

  @Override
  public InsertsStreamSubscriber createInsertsSubscriber(final String target,
      final JsonObject properties,
      final Subscriber<InsertResult> acksSubscriber, final Context context,
      final WorkerExecutor workerExecutor,
      final ApiSecurityContext apiSecurityContext) {
    return new InsertsStreamEndpoint(ksqlEngine, ksqlConfig, reservedInternalTopics)
        .createInsertsSubscriber(target, properties, acksSubscriber, context, workerExecutor,
            createServiceContext(apiSecurityContext));
  }

  private ServiceContext createServiceContext(final ApiSecurityContext apiSecurityContext) {
    return ksqlSecurityContextProvider.provide(apiSecurityContext).getServiceContext();
  }

}
