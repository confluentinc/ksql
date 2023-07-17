/*
 * Copyright 2022 Confluent Inc.
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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.rest.client.KsqlClient;
import io.confluent.ksql.rest.entity.KsqlErrorMessage;
import io.confluent.ksql.rest.server.services.InternalKsqlClientFactory;
import io.confluent.ksql.rest.server.services.RestServiceContextFactory;
import io.confluent.ksql.rest.server.state.ServerState;
import io.confluent.ksql.schema.registry.KsqlSchemaRegistryClientFactory;
import io.confluent.ksql.services.ConnectClientFactory;
import io.confluent.ksql.services.DefaultConnectClientFactory;
import io.confluent.ksql.services.KafkaTopicClient;
import io.confluent.ksql.services.KafkaTopicClientImpl;
import io.confluent.ksql.services.LazyServiceContext;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.RetryUtil;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.net.SocketAddress;
import java.io.Closeable;
import java.net.URI;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.function.Supplier;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.streams.processor.internals.DefaultKafkaClientSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// CHECKSTYLE_RULES.OFF: ClassDataAbstractionCoupling
public class PreconditionChecker implements Executable {
  // CHECKSTYLE_RULES.ON: ClassDataAbstractionCoupling

  private static final Logger LOG = LoggerFactory.getLogger(PreconditionChecker.class);

  final KsqlRestConfig restConfig;
  final Supplier<Map<String, String>> propertiesLoader;
  final Supplier<Clients> clientsSupplier;
  final Vertx vertx;
  final List<KsqlServerPrecondition> preconditions;
  final PreconditionServer server;
  final ServerState serverState;
  private final CompletableFuture<Void> terminatedFuture = new CompletableFuture<>();

  public PreconditionChecker(
      final Supplier<Map<String, String>> propertiesLoader,
      final ServerState serverState
  ) {
    this.propertiesLoader = Objects.requireNonNull(propertiesLoader, "propertiesLoader");
    final Map<String, String> properties = propertiesLoader.get();
    this.restConfig = new KsqlRestConfig(properties);
    this.serverState = Objects.requireNonNull(serverState, "serverState");
    this.clientsSupplier = () -> buildClients(this.propertiesLoader);
    this.preconditions = restConfig.getConfiguredInstances(
        KsqlRestConfig.KSQL_SERVER_PRECONDITIONS,
        KsqlServerPrecondition.class
    );
    this.vertx = Vertx.vertx(
        new VertxOptions()
            .setMaxWorkerExecuteTimeUnit(TimeUnit.MILLISECONDS)
            .setMaxWorkerExecuteTime(Long.MAX_VALUE));
    this.server = new PreconditionServer(
        vertx,
        restConfig,
        serverState
    );
  }

  @VisibleForTesting
  PreconditionChecker(
      final Supplier<Map<String, String>> propertiesLoader,
      final KsqlRestConfig restConfig,
      final Supplier<Clients> clientsSupplier,
      final Vertx vertx,
      final List<KsqlServerPrecondition> preconditions,
      final PreconditionServer server,
      final ServerState state
  ) {
    this.propertiesLoader = Objects.requireNonNull(propertiesLoader, "propertiesLoader");
    this.clientsSupplier = Objects.requireNonNull(clientsSupplier, "clientsSupplier");
    this.restConfig = Objects.requireNonNull(restConfig, "restConfig");
    this.vertx = Objects.requireNonNull(vertx, "vertx");
    this.preconditions = Objects.requireNonNull(preconditions, "preconditions");
    this.server = Objects.requireNonNull(server, "server");
    this.serverState = Objects.requireNonNull(state, "state");
  }

  private boolean shouldCheckPreconditions() {
    try (Clients clients = clientsSupplier.get()) {
      return preconditions.stream()
          .map(p -> p.checkPrecondition(
              propertiesLoader.get(),
              clients.serviceContext,
              clients.topicClient))
          .peek(
              r -> r.ifPresent(rr -> LOG.info("Precondition failed: {}", rr))
          )
          .anyMatch(Optional::isPresent);
    }
  }

  /**
   * Maybe start a precondition server. A precondition server is start if any of the configured
   * preconditions fail. Otherwise, no server is started. The precondition server responds to all
   * requests with 503, other than the liveness and readiness probes.
   */
  @Override
  public void startAsync() {
    if (!shouldCheckPreconditions()) {
      LOG.info("All preconditions passed, skipping precondition server start");
      return;
    }
    LOG.info("Some preconditions not passed, starting precondition server");
    server.start();
  }

  @Override
  public void notifyTerminated() {
    terminatedFuture.complete(null);
  }

  @Override
  public void shutdown() {
    if (server.started()) {
      server.stop();
    }
    vertx.close();
  }

  /**
   * Wait until either all preconditions evaluate successfully, or the process is asked
   * to exit (by calling notifyTermianted)
   */
  @Override
  public void awaitTerminated() {
    final List<Predicate<Exception>> predicates = ImmutableList.of(
        e -> !(e instanceof KsqlFailedPrecondition)
    );
    RetryUtil.retryWithBackoff(
        Integer.MAX_VALUE,
        1000,
        Math.toIntExact(restConfig.getLong(
            KsqlRestConfig.KSQL_PRECONDITION_CHECKER_BACK_OFF_TIME_MS)),
        this::checkPreconditions,
        terminatedFuture::isDone,
        predicates
    );
  }

  public List<URI> getListeners() {
    return server.getListeners();
  }

  /**
   * Checks all preconditions. This is called first to decide whether or not the precondition
   * server needs to run. Then, it's called indefinitely until all preconditions pass.
   */
  private void checkPreconditions() {
    LOG.info("Checking preconditions...");
    for (final KsqlServerPrecondition precondition : preconditions) {
      final Optional<KsqlErrorMessage> error;
      try (Clients clients = clientsSupplier.get()) {
        error = precondition.checkPrecondition(
            propertiesLoader.get(),
            clients.serviceContext,
            clients.topicClient
        );
      }
      if (error.isPresent()) {
        LOG.info("Precondition failed: {}", error.get());
        serverState.setInitializingReason(error.get());
        throw new KsqlFailedPrecondition(error.get().toString());
      }
    }
  }

  @VisibleForTesting
  static class KsqlFailedPrecondition extends RuntimeException {
    KsqlFailedPrecondition(final String error) {
      super(error);
    }
  }

  private static Admin createCommandTopicAdminClient(
      final KsqlRestConfig ksqlRestConfig,
      final KsqlConfig ksqlConfig
  ) {
    final Map<String, Object> adminClientConfigs =
        new HashMap<>(ksqlConfig.getKsqlAdminClientConfigProps());
    adminClientConfigs.putAll(ksqlRestConfig.getCommandProducerProperties());
    return new DefaultKafkaClientSupplier().getAdmin(adminClientConfigs);
  }

  private static Clients buildClients(
      final Supplier<Map<String, String>> propertiesLoader
  ) {
    final Map<String, String> properties = propertiesLoader.get();
    final Vertx vertx = Vertx.vertx(
        new VertxOptions()
            .setMaxWorkerExecuteTimeUnit(TimeUnit.MILLISECONDS)
            .setMaxWorkerExecuteTime(Long.MAX_VALUE));
    final KsqlConfig ksqlConfig = new KsqlConfig(properties);
    final KsqlClient sharedClient = InternalKsqlClientFactory.createInternalClient(
        ksqlConfig.originalsStrings(),
        SocketAddress::inetSocketAddress,
        vertx
    );
    final Supplier<SchemaRegistryClient> schemaRegistryClientFactory =
        new KsqlSchemaRegistryClientFactory(ksqlConfig, Collections.emptyMap())::get;
    final ConnectClientFactory connectClientFactory = new DefaultConnectClientFactory(ksqlConfig);
    final ServiceContext serviceContext
        = new LazyServiceContext(() -> RestServiceContextFactory.create(
            ksqlConfig,
            Optional.empty(),
            schemaRegistryClientFactory,
            connectClientFactory,
            sharedClient,
            Collections.emptyList(),
            Optional.empty()
    ));
    final Admin admin
        = createCommandTopicAdminClient(new KsqlRestConfig(properties), new KsqlConfig(properties));
    return new Clients(
        serviceContext,
        vertx,
        admin,
        new KafkaTopicClientImpl(() -> admin)
    );
  }

  static final class Clients implements Closeable {
    private final ServiceContext serviceContext;
    private final Vertx vertx;
    private final Admin admin;
    private final KafkaTopicClient topicClient;

    Clients(
        final ServiceContext serviceContext,
        final Vertx vertx,
        final Admin admin,
        final KafkaTopicClient topicClient
    ) {
      this.serviceContext = serviceContext;
      this.vertx = vertx;
      this.admin = admin;
      this.topicClient = topicClient;
    }

    @Override
    public void close() {
      serviceContext.close();
      vertx.close();
      admin.close();
    }
  }
}
