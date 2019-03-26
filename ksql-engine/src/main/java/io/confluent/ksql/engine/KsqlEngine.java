/*
 * Copyright 2019 Confluent Inc.
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

package io.confluent.ksql.engine;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.KsqlExecutionContext;
import io.confluent.ksql.ddl.commands.DdlCommandExec;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.function.MutableFunctionRegistry;
import io.confluent.ksql.internal.KsqlEngineMetrics;
import io.confluent.ksql.logging.processing.ProcessingLogContext;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.MetaStoreImpl;
import io.confluent.ksql.metastore.MutableMetaStore;
import io.confluent.ksql.metrics.StreamsErrorCollector;
import io.confluent.ksql.parser.KsqlParser.ParsedStatement;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.parser.tree.ExecutableDdlStatement;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.parser.tree.QueryContainer;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.schema.registry.SchemaRegistryUtil;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.PersistentQueryMetadata;
import io.confluent.ksql.util.QueryIdGenerator;
import io.confluent.ksql.util.QueryMetadata;
import java.io.Closeable;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KsqlEngine implements KsqlExecutionContext, Closeable {

  private static final Logger log = LoggerFactory.getLogger(KsqlEngine.class);

  private final AtomicBoolean acceptingStatements = new AtomicBoolean(true);
  private final Set<QueryMetadata> allLiveQueries = ConcurrentHashMap.newKeySet();
  private final KsqlEngineMetrics engineMetrics;
  private final ScheduledExecutorService aggregateMetricsCollector;
  private final String serviceId;
  private final ServiceContext serviceContext;
  private final EngineContext primaryContext;

  public KsqlEngine(
      final ServiceContext serviceContext,
      final ProcessingLogContext processingLogContext,
      final MutableFunctionRegistry functionRegistry,
      final String serviceId
  ) {
    this(
        serviceContext,
        processingLogContext,
        serviceId,
        new MetaStoreImpl(functionRegistry),
        KsqlEngineMetrics::new);
  }

  KsqlEngine(
      final ServiceContext serviceContext,
      final ProcessingLogContext processingLogContext,
      final String serviceId,
      final MutableMetaStore metaStore,
      final Function<KsqlEngine, KsqlEngineMetrics> engineMetricsFactory
  ) {
    this.primaryContext = EngineContext.create(
        serviceContext,
        processingLogContext,
        metaStore,
        new QueryIdGenerator(),
        this::unregisterQuery);
    this.serviceContext = Objects.requireNonNull(serviceContext, "serviceContext");
    this.serviceId = Objects.requireNonNull(serviceId, "serviceId");
    this.engineMetrics = engineMetricsFactory.apply(this);
    this.aggregateMetricsCollector = Executors.newSingleThreadScheduledExecutor();
    this.aggregateMetricsCollector.scheduleAtFixedRate(
        () -> {
          try {
            this.engineMetrics.updateMetrics();
          } catch (final Exception e) {
            log.info("Error updating engine metrics", e);
          }
        },
        1000,
        1000,
        TimeUnit.MILLISECONDS
    );
  }

  public int numberOfLiveQueries() {
    return allLiveQueries.size();
  }

  @Override
  public Optional<PersistentQueryMetadata> getPersistentQuery(final QueryId queryId) {
    return primaryContext.getPersistentQuery(queryId);
  }

  public List<PersistentQueryMetadata> getPersistentQueries() {
    return ImmutableList.copyOf(primaryContext.getPersistentQueries().values());
  }

  public boolean hasActiveQueries() {
    return !primaryContext.getPersistentQueries().isEmpty();
  }

  @Override
  public MetaStore getMetaStore() {
    return primaryContext.getMetaStore();
  }

  @Override
  public FunctionRegistry getFunctionRegistry() {
    return primaryContext.getMetaStore().getFunctionRegistry();
  }

  @Override
  public ServiceContext getServiceContext() {
    return serviceContext;
  }

  public DdlCommandExec getDdlCommandExec() {
    return primaryContext.getDdlCommandExec();
  }

  public String getServiceId() {
    return serviceId;
  }

  public void stopAcceptingStatements() {
    acceptingStatements.set(false);
  }

  public boolean isAcceptingStatements() {
    return acceptingStatements.get();
  }

  @Override
  public KsqlExecutionContext createSandbox() {
    return new SandboxedExecutionContext(primaryContext);
  }

  @Override
  public List<ParsedStatement> parse(final String sql) {
    return primaryContext.parse(sql);
  }

  @Override
  public PreparedStatement<?> prepare(final ParsedStatement stmt) {
    return primaryContext.prepare(stmt);
  }

  @Override
  public ExecuteResult execute(
      final PreparedStatement<?> statement,
      final KsqlConfig ksqlConfig,
      final Map<String, Object> overriddenProperties
  ) {
    final ExecuteResult result = EngineExecutor
        .create(primaryContext, ksqlConfig, overriddenProperties)
        .execute(statement);

    result.getQuery().ifPresent(this::registerQuery);

    return result;
  }

  @Override
  public void close() {
    allLiveQueries.forEach(QueryMetadata::close);
    engineMetrics.close();
    aggregateMetricsCollector.shutdown();
  }

  /**
   * Determines if a statement is executable by the engine.
   *
   * @param statement the statement to test.
   * @return {@code true} if the engine can execute the statement, {@code false} otherwise
   */
  public static boolean isExecutableStatement(final PreparedStatement<?> statement) {
    return statement.getStatement() instanceof ExecutableDdlStatement
        || statement.getStatement() instanceof QueryContainer
        || statement.getStatement() instanceof Query;
  }

  private void registerQuery(final QueryMetadata query) {
    allLiveQueries.add(query);
    engineMetrics.registerQuery(query);
  }

  private void unregisterQuery(final QueryMetadata query) {
    final String applicationId = query.getQueryApplicationId();

    if (!query.getState().equalsIgnoreCase("NOT_RUNNING")) {
      throw new IllegalStateException("query not stopped."
          + " id " + applicationId + ", state: " + query.getState());
    }

    if (!allLiveQueries.remove(query)) {
      return;
    }

    if (query.hasEverBeenStarted()) {
      SchemaRegistryUtil
          .cleanUpInternalTopicAvroSchemas(applicationId, serviceContext.getSchemaRegistryClient());
      serviceContext.getTopicClient().deleteInternalTopics(applicationId);
    }

    StreamsErrorCollector.notifyApplicationClose(applicationId);
  }
}
