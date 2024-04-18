/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.ksql;

import io.confluent.ksql.analyzer.ImmutableAnalysis;
import io.confluent.ksql.engine.KsqlEngine;
import io.confluent.ksql.engine.KsqlPlan;
import io.confluent.ksql.execution.pull.HARouting;
import io.confluent.ksql.execution.pull.PullQueryResult;
import io.confluent.ksql.execution.scalablepush.PushRouting;
import io.confluent.ksql.execution.scalablepush.PushRoutingOptions;
import io.confluent.ksql.execution.streams.RoutingOptions;
import io.confluent.ksql.internal.PullQueryExecutorMetrics;
import io.confluent.ksql.internal.ScalablePushQueryMetrics;
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
import io.confluent.ksql.util.ScalablePushQueryMetadata;
import io.confluent.ksql.util.TransientQueryMetadata;
import io.vertx.core.Context;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * The context in which statements can be executed.
 */
public interface KsqlExecutionContext {

  /**
   * Create an execution context in which statements can be run without affecting the state
   * of the system.
   *
   * @return a sand boxed execution context.
   */
  KsqlExecutionContext createSandbox(ServiceContext serviceContext);

  /**
   * @return read-only access to the context's {@link MetaStore}.
   */
  MetaStore getMetaStore();

  /**
   * @return read-only access to the context's {@link KsqlConfig}
   */
  KsqlConfig getKsqlConfig();

  MetricCollectors metricCollectors();

  /**
   * Alters the system property to the specified value.
   *
   * @param propertyName the system property that we want to change.
   * @param propertyValue the value we want to change the property to.
   */
  void alterSystemProperty(String propertyName, String propertyValue);

  /**
   * @return the service context used for this execution context
   */
  ServiceContext getServiceContext();

  /**
   * @return the processing log context used to track errors during processing.
   */
  ProcessingLogContext getProcessingLogContext();

  /**
   * Retrieve the details of a persistent query.
   *
   * @param queryId the id of the query to retrieve.
   * @return the query's details or else {@code Optional.empty()} if no found.
   */
  Optional<PersistentQueryMetadata> getPersistentQuery(QueryId queryId);

  /**
   * Retrieve the details of a query.
   *
   * @param queryId the id of the query to retrieve.
   * @return the query's details or else {@code Optional.empty()} if no found.
   */
  Optional<QueryMetadata> getQuery(QueryId queryId);

  /**
   * Retrieves the list of all running persistent queries.
   *
   * @return the list of all persistent queries
   * @see #getPersistentQuery(QueryId)
   */
  List<PersistentQueryMetadata> getPersistentQueries();

  /**
   * Retrieves the list of all queries writing to this {@code SourceName}.
   *
   * @param sourceName the sourceName of the queries to retrieve.
   * @return the list of queries.
   */
  Set<QueryId> getQueriesWithSink(SourceName sourceName);

  /**
   * Retrieves the list of all running queries.
   *
   * @return the list of all queries
   */
  List<QueryMetadata> getAllLiveQueries();

  /**
   * Parse the statement(s) in supplied {@code sql}.
   *
   * <p>Note: the state of the execution context will not be changed.
   *
   * @param sql the statements to parse.
   * @return the list of prepared statements.
   */
  List<ParsedStatement> parse(String sql);

  /**
   * Prepare the supplied statement for execution.
   *
   * <p>This provides some level of validation as well, e.g. ensuring sources and topics exist
   * in the metastore, etc.
   *
   * <p>If variables are used in the statement, they will be replaced with the values found in
   * {@code variablesMap}.
   *
   * @param stmt the parsed statement.
   * @param variablesMap a list of values for SQL variable substitution
   * @return the prepared statement.
   */
  PreparedStatement<?> prepare(ParsedStatement stmt, Map<String, String> variablesMap);

  default PreparedStatement<?> prepare(ParsedStatement stmt) {
    return prepare(stmt, Collections.emptyMap());
  }

  /**
   * Executes a query using the supplied service context.
   * @return the query metadata
   */
  TransientQueryMetadata executeTransientQuery(
      ServiceContext serviceContext,
      ConfiguredStatement<Query> statement,
      boolean excludeTombstones
  );

  /**
   * Executes a pull query by first creating a logical plan and then translating it to a physical
   * plan. The physical plan is then traversed for every row in the state store.
   * @param serviceContext The service context to execute the query in
   * @param statement The pull query
   * @param routingOptions Configuration parameters used for routing requests
   * @param pullQueryMetrics JMX metrics
   * @param startImmediately Whether to start the pull query immediately.  If not, the caller must
   *                         call PullQueryResult.start to start the query.
   * @return the rows that are the result of the query evaluation.
   */
  PullQueryResult executeTablePullQuery(
      ImmutableAnalysis analysis,
      ServiceContext serviceContext,
      ConfiguredStatement<Query> statement,
      HARouting routing,
      RoutingOptions routingOptions,
      QueryPlannerOptions queryPlannerOptions,
      Optional<PullQueryExecutorMetrics> pullQueryMetrics,
      boolean startImmediately,
      Optional<ConsistencyOffsetVector> consistencyOffsetVector
  );

  /**
   * Executes a scalable push query by first creating a logical plan and then translating it to a
   * physical plan. The physical plan is then traversed for every row that's passing through the
   * topology's output.
   * @param serviceContext The service context to execute the query in
   * @param statement The scalable push query
   * @param pushRouting The push routing object
   * @param pushRoutingOptions The options for routing
   * @param context The Vertx context of the request
   * @param scalablePushQueryMetrics JMX metrics
   * @return A ScalablePushQueryMetadata object
   */
  ScalablePushQueryMetadata executeScalablePushQuery(
      ImmutableAnalysis analysis,
      ServiceContext serviceContext,
      ConfiguredStatement<Query> statement,
      PushRouting pushRouting,
      PushRoutingOptions pushRoutingOptions,
      QueryPlannerOptions queryPlannerOptions,
      Context context,
      Optional<ScalablePushQueryMetrics> scalablePushQueryMetrics
  );

  /**
   * Computes a plan for executing a DDL/DML statement using the supplied service context.
   */
  KsqlPlan plan(ServiceContext serviceContext, ConfiguredStatement<?> statement);

  /**
   * Executes a KSQL plan using the supplied service context.
   */
  default ExecuteResult execute(ServiceContext serviceContext, ConfiguredKsqlPlan plan) {
    return execute(serviceContext, plan, false);
  }

  /**
   * Executes a KSQL plan using the supplied service context.
   */
  ExecuteResult execute(ServiceContext serviceContext, ConfiguredKsqlPlan plan,
                        boolean restoreInProgress);

  /**
   * Execute the supplied statement, updating the meta store and registering any query.
   *
   * <p>The statement must be executable. See {@link KsqlEngine#isExecutableStatement}.
   *
   * <p>If the statement contains a query, then it will be tracked, but not started.
   *
   * <p>The statement is executed using the specific {@code ServiceContext}
   *
   * @param serviceContext The ServiceContext of the user executing the statement.
   * @param statement The SQL to execute.
   * @return The execution result.
   */
  ExecuteResult execute(ServiceContext serviceContext, ConfiguredStatement<?> statement);

  /**
   * Holds the union of possible results from an {@link #execute} call.
   *
   * <p>Only one field will be populated.
   */
  final class ExecuteResult {

    private final Optional<QueryMetadata> query;
    private final Optional<String> commandResult;

    public static ExecuteResult of(final QueryMetadata query) {
      return new ExecuteResult(Optional.of(query), Optional.empty());
    }

    public static ExecuteResult of(final String commandResult) {
      return new ExecuteResult(Optional.empty(), Optional.of(commandResult));
    }

    public Optional<QueryMetadata> getQuery() {
      return query;
    }

    public Optional<String> getCommandResult() {
      return commandResult;
    }

    private ExecuteResult(
        final Optional<QueryMetadata> query,
        final Optional<String> commandResult
    ) {
      this.query = Objects.requireNonNull(query, "query");
      this.commandResult = Objects.requireNonNull(commandResult, "commandResult");
    }
  }
}
