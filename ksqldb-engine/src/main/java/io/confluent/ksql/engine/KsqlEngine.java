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
import io.confluent.ksql.ServiceInfo;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.internal.KsqlEngineMetrics;
import io.confluent.ksql.logging.processing.ProcessingLogContext;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.MetaStoreImpl;
import io.confluent.ksql.metastore.MutableMetaStore;
import io.confluent.ksql.parser.KsqlParser.ParsedStatement;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.parser.tree.ExecutableDdlStatement;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.parser.tree.QueryContainer;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.planner.plan.ConfiguredKsqlPlan;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.query.id.QueryIdGenerator;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.KsqlStatementException;
import io.confluent.ksql.util.PersistentQueryMetadata;
import io.confluent.ksql.util.QueryMetadata;
import io.confluent.ksql.util.TransientQueryMetadata;
import java.io.Closeable;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KsqlEngine implements KsqlExecutionContext, Closeable {

  private static final Logger log = LoggerFactory.getLogger(KsqlEngine.class);

  private final KsqlEngineMetrics engineMetrics;
  private final ScheduledExecutorService aggregateMetricsCollector;
  private final String serviceId;
  private final EngineContext primaryContext;

  public KsqlEngine(
      final ServiceContext serviceContext,
      final ProcessingLogContext processingLogContext,
      final FunctionRegistry functionRegistry,
      final ServiceInfo serviceInfo,
      final QueryIdGenerator queryIdGenerator
  ) {
    this(
        serviceContext,
        processingLogContext,
        serviceInfo.serviceId(),
        new MetaStoreImpl(functionRegistry),
        (engine) -> new KsqlEngineMetrics(
            serviceInfo.metricsPrefix(),
            engine,
            serviceInfo.customMetricsTags(),
            serviceInfo.metricsExtension()
        ),
        queryIdGenerator);
  }

  public KsqlEngine(
      final ServiceContext serviceContext,
      final ProcessingLogContext processingLogContext,
      final String serviceId,
      final MutableMetaStore metaStore,
      final Function<KsqlEngine, KsqlEngineMetrics> engineMetricsFactory,
      final QueryIdGenerator queryIdGenerator
  ) {
    this.primaryContext = EngineContext.create(
        serviceContext,
        processingLogContext,
        metaStore,
        queryIdGenerator
    );
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
    return primaryContext.getAllLiveQueries().size();
  }

  @Override
  public Optional<PersistentQueryMetadata> getPersistentQuery(final QueryId queryId) {
    return primaryContext.getPersistentQuery(queryId);
  }

  @Override
  public List<PersistentQueryMetadata> getPersistentQueries() {
    return ImmutableList.copyOf(primaryContext.getPersistentQueries().values());
  }

  @Override
  public List<QueryMetadata> getAllLiveQueries() {
    return primaryContext.getAllLiveQueries();
  }

  public boolean hasActiveQueries() {
    return !primaryContext.getPersistentQueries().isEmpty();
  }

  @Override
  public MetaStore getMetaStore() {
    return primaryContext.getMetaStore();
  }

  @Override
  public ServiceContext getServiceContext() {
    return primaryContext.getServiceContext();
  }

  @Override
  public ProcessingLogContext getProcessingLogContext() {
    return primaryContext.getProcessingLogContext();
  }

  public String getServiceId() {
    return serviceId;
  }

  @Override
  public KsqlExecutionContext createSandbox(final ServiceContext serviceContext) {
    return new SandboxedExecutionContext(primaryContext, serviceContext);
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
  public KsqlPlan plan(
      final ServiceContext serviceContext,
      final ConfiguredStatement<?> statement
  ) {
    return EngineExecutor
        .create(
            primaryContext, serviceContext, statement.getConfig(), statement.getConfigOverrides())
        .plan(statement);
  }

  @Override
  public ExecuteResult execute(final ServiceContext serviceContext, final ConfiguredKsqlPlan plan) {
    final ExecuteResult result = EngineExecutor
        .create(primaryContext, serviceContext, plan.getConfig(), plan.getOverrides())
        .execute(plan.getPlan());
    result.getQuery().ifPresent(this::registerQuery);
    return result;
  }

  @Override
  public ExecuteResult execute(
      final ServiceContext serviceContext,
      final ConfiguredStatement<?> statement
  ) {
    return execute(
        serviceContext,
        ConfiguredKsqlPlan.of(
            plan(serviceContext, statement),
            statement.getConfigOverrides(),
            statement.getConfig()
        )
    );
  }

  @Override
  public TransientQueryMetadata executeQuery(
      final ServiceContext serviceContext,
      final ConfiguredStatement<Query> statement
  ) {
    try {
      final TransientQueryMetadata query = EngineExecutor
          .create(
              primaryContext,
              serviceContext,
              statement.getConfig(),
              statement.getConfigOverrides())
          .executeQuery(statement);
      registerQuery(query);
      primaryContext.registerQuery(query);
      return query;
    } catch (final KsqlStatementException e) {
      throw e;
    } catch (final KsqlException e) {
      // add the statement text to the KsqlException
      throw new KsqlStatementException(e.getMessage(), statement.getStatementText(), e.getCause());
    }
  }

  /**
   * @param closeQueries whether or not to clean up the local state for any running queries
   */
  public void close(final boolean closeQueries) {
    primaryContext.getAllLiveQueries()
      .forEach(closeQueries ? QueryMetadata::close : QueryMetadata::stop);

    engineMetrics.close();
    aggregateMetricsCollector.shutdown();
  }

  @Override
  public void close() {
    close(false);
  }

  /**
   * Determines if a statement is executable by the engine.
   *
   * @param statement the statement to test.
   * @return {@code true} if the engine can execute the statement, {@code false} otherwise
   */
  public static boolean isExecutableStatement(final Statement statement) {
    return statement instanceof ExecutableDdlStatement
        || statement instanceof QueryContainer
        || statement instanceof Query;
  }

  private void registerQuery(final QueryMetadata query) {
    engineMetrics.registerQuery(query);
  }

}
