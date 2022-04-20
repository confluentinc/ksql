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
import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.KsqlExecutionContext;
import io.confluent.ksql.analyzer.ImmutableAnalysis;
import io.confluent.ksql.execution.pull.HARouting;
import io.confluent.ksql.execution.pull.PullQueryResult;
import io.confluent.ksql.execution.scalablepush.PushRouting;
import io.confluent.ksql.execution.scalablepush.PushRoutingOptions;
import io.confluent.ksql.execution.streams.RoutingOptions;
import io.confluent.ksql.internal.PullQueryExecutorMetrics;
import io.confluent.ksql.internal.ScalablePushQueryMetrics;
import io.confluent.ksql.logging.processing.NoopProcessingLogContext;
import io.confluent.ksql.logging.processing.ProcessingLogContext;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metrics.MetricCollectors;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.parser.KsqlParser.ParsedStatement;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.planner.QueryPlannerOptions;
import io.confluent.ksql.planner.plan.ConfiguredKsqlPlan;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.util.ConsistencyOffsetVector;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.PersistentQueryMetadata;
import io.confluent.ksql.util.QueryMetadata;
import io.confluent.ksql.util.Sandbox;
import io.confluent.ksql.util.ScalablePushQueryMetadata;
import io.confluent.ksql.util.StreamPullQueryMetadata;
import io.confluent.ksql.util.TransientQueryMetadata;
import io.vertx.core.Context;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * An execution context that can execute statements without changing the core engine's state
 * or the state of external services.
 */
@Sandbox
final class SandboxedExecutionContext implements KsqlExecutionContext {

  private final EngineContext engineContext;
  private final MetricCollectors metricCollectors;

  SandboxedExecutionContext(
      final EngineContext sourceContext,
      final ServiceContext serviceContext,
      final MetricCollectors metricCollectors
  ) {
    this.metricCollectors = metricCollectors;
    this.engineContext = sourceContext.createSandbox(serviceContext);
  }

  @Override
  public KsqlConfig getKsqlConfig() {
    return this.engineContext.getKsqlConfig();
  }

  @Override
  public MetricCollectors metricCollectors() {
    return metricCollectors;
  }

  @Override
  public void alterSystemProperty(final String propertyName, final String propertyValue) {
    final Map<String, String> overrides = ImmutableMap.of(propertyName, propertyValue);
    this.engineContext.alterSystemProperty(overrides);
  }

  @Override
  public MetaStore getMetaStore() {
    return engineContext.getMetaStore();
  }

  @Override
  public ServiceContext getServiceContext() {
    return engineContext.getServiceContext();
  }

  @Override
  public ProcessingLogContext getProcessingLogContext() {
    return NoopProcessingLogContext.INSTANCE;
  }

  @Override
  public KsqlExecutionContext createSandbox(final ServiceContext serviceContext) {
    return new SandboxedExecutionContext(engineContext, serviceContext, metricCollectors);
  }

  @Override
  public Optional<PersistentQueryMetadata> getPersistentQuery(final QueryId queryId) {
    return engineContext.getQueryRegistry().getPersistentQuery(queryId);
  }

  @Override
  public Optional<QueryMetadata> getQuery(final QueryId queryId) {
    return engineContext.getQueryRegistry().getQuery(queryId);
  }

  @Override
  public List<PersistentQueryMetadata> getPersistentQueries() {
    return ImmutableList.copyOf(engineContext.getQueryRegistry().getPersistentQueries().values());
  }

  @Override
  public Set<QueryId> getQueriesWithSink(final SourceName sourceName) {
    return engineContext.getQueryRegistry().getQueriesWithSink(sourceName);
  }

  @Override
  public List<QueryMetadata> getAllLiveQueries() {
    return engineContext.getQueryRegistry().getAllLiveQueries();
  }

  @Override
  public List<ParsedStatement> parse(final String sql) {
    return engineContext.parse(sql);
  }

  @Override
  public PreparedStatement<?> prepare(
      final ParsedStatement stmt,
      final Map<String, String> variablesMap
  ) {
    return engineContext.prepare(stmt, variablesMap);
  }

  @Override
  public KsqlPlan plan(
      final ServiceContext serviceContext,
      final ConfiguredStatement<?> statement
  ) {
    return EngineExecutor.create(
        engineContext,
        serviceContext,
        statement.getSessionConfig()
    ).plan(statement);
  }

  @Override
  public ExecuteResult execute(
      final ServiceContext serviceContext,
      final ConfiguredKsqlPlan ksqlPlan,
      final boolean restoreInProgress
  ) {
    try {
      final ExecuteResult result = EngineExecutor.create(
          engineContext,
          serviceContext,
          ksqlPlan.getConfig()
      ).execute(ksqlPlan.getPlan(), restoreInProgress);

      // Having a streams running in a sandboxed environment is not necessary
      if (!getKsqlConfig().getBoolean(KsqlConfig.KSQL_SHARED_RUNTIME_ENABLED)) {
        result.getQuery().map(QueryMetadata::getKafkaStreams).ifPresent(streams -> streams.close());
      }
      return result;
    } finally {
      if (getKsqlConfig().getBoolean(KsqlConfig.KSQL_SHARED_RUNTIME_ENABLED)) {
        engineContext.getQueryRegistry().closeRuntimes();
      }
    }
  }

  @Override
  public ExecuteResult execute(
      final ServiceContext serviceContext,
      final ConfiguredStatement<?> statement
  ) {
    return execute(
        serviceContext,
        ConfiguredKsqlPlan.of(plan(serviceContext, statement), statement.getSessionConfig())
    );
  }

  @Override
  public TransientQueryMetadata executeTransientQuery(
      final ServiceContext serviceContext,
      final ConfiguredStatement<Query> statement,
      final boolean excludeTombstones
  ) {
    return EngineExecutor.create(
        engineContext,
        serviceContext,
        statement.getSessionConfig()
    ).executeTransientQuery(statement, excludeTombstones);
  }

  @Override
  public PullQueryResult executeTablePullQuery(
      final ImmutableAnalysis analysis,
      final ServiceContext serviceContext,
      final ConfiguredStatement<Query> statement,
      final HARouting routing,
      final RoutingOptions routingOptions,
      final QueryPlannerOptions queryPlannerOptions,
      final Optional<PullQueryExecutorMetrics> pullQueryMetrics,
      final boolean startImmediately,
      final Optional<ConsistencyOffsetVector> consistencyOffsetVector
  ) {
    return EngineExecutor.create(
        engineContext,
        serviceContext,
        statement.getSessionConfig()
    ).executeTablePullQuery(
        analysis,
        statement,
        routing,
        routingOptions,
        queryPlannerOptions,
        pullQueryMetrics,
        startImmediately,
        consistencyOffsetVector
    );
  }

  @Override
  public ScalablePushQueryMetadata executeScalablePushQuery(
      final ImmutableAnalysis analysis,
      final ServiceContext serviceContext,
      final ConfiguredStatement<Query> statement,
      final PushRouting pushRouting,
      final PushRoutingOptions pushRoutingOptions,
      final QueryPlannerOptions queryPlannerOptions,
      final Context context,
      final Optional<ScalablePushQueryMetrics> scalablePushQueryMetrics
  ) {
    return EngineExecutor.create(
        engineContext,
        serviceContext,
        statement.getSessionConfig()
    ).executeScalablePushQuery(
        analysis,
        statement,
        pushRouting,
        pushRoutingOptions,
        queryPlannerOptions,
        context,
        scalablePushQueryMetrics
    );
  }

  @Override
  public void updateStreamsPropertiesAndRestartRuntime() {
    throw new UnsupportedOperationException();

  }

  @Override
  public ImmutableAnalysis analyzeQueryWithNoOutputTopic(
      final Query query,
      final String queryText,
      final Map<String, Object> configOverrides) {
    throw new UnsupportedOperationException();
  }

  @Override
  public StreamPullQueryMetadata createStreamPullQuery(
      final ServiceContext serviceContext,
      final ImmutableAnalysis analysis,
      final ConfiguredStatement<Query> statementOrig,
      final boolean excludeTombstones) {
    throw new UnsupportedOperationException();
  }
}
