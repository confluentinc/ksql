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

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.api.server.InsertResult;
import io.confluent.ksql.api.server.InsertsStreamSubscriber;
import io.confluent.ksql.api.spi.Endpoints;
import io.confluent.ksql.api.spi.QueryPublisher;
import io.confluent.ksql.engine.KsqlEngine;
import io.confluent.ksql.rest.server.execution.PullQueryExecutor;
import io.confluent.ksql.rest.server.services.RestServiceContextFactory.UserServiceContextFactory;
import io.confluent.ksql.schema.registry.KsqlSchemaRegistryClientFactory;
import io.confluent.ksql.security.KsqlSecurityExtension;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.ReservedInternalTopics;
import io.vertx.core.Context;
import io.vertx.core.WorkerExecutor;
import io.vertx.core.json.JsonObject;
import java.security.Principal;
import java.util.Collections;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;
import org.apache.kafka.streams.KafkaClientSupplier;
import org.apache.kafka.streams.processor.internals.DefaultKafkaClientSupplier;
import org.reactivestreams.Subscriber;

public class KsqlServerEndpoints implements Endpoints {

  private final KsqlEngine ksqlEngine;
  private final KsqlConfig ksqlConfig;
  private final KsqlSecurityExtension securityExtension;
  private final UserServiceContextFactory serviceContextFactory;
  private final PullQueryExecutor pullQueryExecutor;
  private final ReservedInternalTopics reservedInternalTopics;

  public KsqlServerEndpoints(
      final KsqlEngine ksqlEngine,
      final KsqlConfig ksqlConfig,
      final KsqlSecurityExtension securityExtension,
      final UserServiceContextFactory serviceContextFactory,
      final PullQueryExecutor pullQueryExecutor) {
    this.ksqlEngine = Objects.requireNonNull(ksqlEngine);
    this.ksqlConfig = Objects.requireNonNull(ksqlConfig);
    this.securityExtension = Objects.requireNonNull(securityExtension);
    this.serviceContextFactory = Objects.requireNonNull(serviceContextFactory);
    this.pullQueryExecutor = Objects.requireNonNull(pullQueryExecutor);
    this.reservedInternalTopics = new ReservedInternalTopics(ksqlConfig);
  }

  @Override
  public QueryPublisher createQueryPublisher(final String sql, final JsonObject properties,
      final Context context,
      final WorkerExecutor workerExecutor) {
    return new QueryStreamEndpoint(ksqlEngine, ksqlConfig, pullQueryExecutor)
        .createQueryPublisher(sql, properties, context, workerExecutor,
            createServiceContext());
  }

  @Override
  public InsertsStreamSubscriber createInsertsSubscriber(final String target,
      final JsonObject properties,
      final Subscriber<InsertResult> acksSubscriber, final Context context,
      final WorkerExecutor workerExecutor) {
    return new InsertsStreamEndpoint(ksqlEngine, ksqlConfig, reservedInternalTopics)
        .createInsertsSubscriber(target, properties, acksSubscriber, context, workerExecutor,
            createServiceContext());
  }

  private ServiceContext createServiceContext() {
    // Creates a ServiceContext using the user's credentials, so the WS query topics are
    // accessed with the user permission context (defaults to KSQL service context)

    if (!securityExtension.getUserContextProvider().isPresent()) {
      return createServiceContext(new DefaultKafkaClientSupplier(),
          new KsqlSchemaRegistryClientFactory(ksqlConfig, Collections.emptyMap())::get);
    }

    final Principal principal = new DummyPrincipal();

    return securityExtension.getUserContextProvider()
        .map(provider ->
            createServiceContext(
                provider.getKafkaClientSupplier(principal),
                provider.getSchemaRegistryClientFactory(principal)
            ))
        .get();
  }

  private ServiceContext createServiceContext(
      final KafkaClientSupplier kafkaClientSupplier,
      final Supplier<SchemaRegistryClient> srClientFactory
  ) {
    return serviceContextFactory.create(ksqlConfig,
        Optional.empty(),
        kafkaClientSupplier, srClientFactory);
  }

  private static class DummyPrincipal implements Principal {

    @Override
    public String getName() {
      return "NO_PRINCIPAL";
    }
  }

}
