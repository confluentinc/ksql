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
import io.confluent.ksql.execution.streams.RoutingOptions;
import io.confluent.ksql.internal.PullQueryExecutorMetrics;
import io.confluent.ksql.logging.processing.NoopProcessingLogContext;
import io.confluent.ksql.logging.processing.ProcessingLogContext;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.parser.KsqlParser.ParsedStatement;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.physical.pull.HARouting;
import io.confluent.ksql.physical.pull.PullQueryResult;
import io.confluent.ksql.physical.scalablepush.PushRouting;
import io.confluent.ksql.physical.scalablepush.PushRoutingOptions;
import io.confluent.ksql.planner.QueryPlannerOptions;
import io.confluent.ksql.planner.plan.ConfiguredKsqlPlan;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.util.PersistentQueryMetadata;
import io.confluent.ksql.util.QueryMetadata;
import io.confluent.ksql.util.Sandbox;
import io.confluent.ksql.util.ScalablePushQueryMetadata;
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

  SandboxedExecutionContext(
      final EngineContext sourceContext,
      final ServiceContext serviceContext
  ) {
    this.engineContext = sourceContext.createSandbox(serviceContext);
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
    return new SandboxedExecutionContext(engineContext, serviceContext);
  }

  @Override
  public Optional<PersistentQueryMetadata> getPersistentQuery(final QueryId queryId) {
    return engineContext.getQueryRegistry().getPersistentQuery(queryId);
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
      final ConfiguredKsqlPlan ksqlPlan
  ) {
    final ExecuteResult result = EngineExecutor.create(
        engineContext,
        serviceContext,
        ksqlPlan.getConfig()
    ).execute(ksqlPlan.getPlan());
    result.getQuery().ifPresent(query -> query.getKafkaStreams().close());
    return result;
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
  public TransientQueryMetadata executeQuery(
      final ServiceContext serviceContext,
      final ConfiguredStatement<Query> statement,
      final boolean excludeTombstones
  ) {
    return EngineExecutor.create(
        engineContext,
        serviceContext,
        statement.getSessionConfig()
    ).executeQuery(statement, excludeTombstones);
  }

  @Override
  public PullQueryResult executePullQuery(
      final ServiceContext serviceContext,
      final ConfiguredStatement<Query> statement,
      final HARouting routing,
      final RoutingOptions routingOptions,
      final QueryPlannerOptions queryPlannerOptions,
      final Optional<PullQueryExecutorMetrics> pullQueryMetrics,
      final boolean startImmediately
  ) {
    return EngineExecutor.create(
        engineContext,
        serviceContext,
        statement.getSessionConfig()
    ).executePullQuery(
        statement,
        routing,
        routingOptions,
        queryPlannerOptions,
        pullQueryMetrics,
        startImmediately
    );
  }

  @Override
  public ScalablePushQueryMetadata executeScalablePushQuery(
      final ServiceContext serviceContext,
      final ConfiguredStatement<Query> statement,
      final PushRouting pushRouting,
      final PushRoutingOptions pushRoutingOptions,
      final QueryPlannerOptions queryPlannerOptions,
      final Context context
  ) {
    return EngineExecutor.create(
        engineContext,
        serviceContext,
        statement.getSessionConfig()
    ).executeScalablePushQuery(
        statement,
        pushRouting,
        pushRoutingOptions,
        queryPlannerOptions,
        context
    );
  }
}
