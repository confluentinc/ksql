/*
 * Copyright 2021 Confluent Inc.
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

package io.confluent.ksql.query;

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.config.SessionConfig;
import io.confluent.ksql.execution.plan.ExecutionStep;
import io.confluent.ksql.logging.processing.ProcessingLogContext;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.model.DataSource;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.serde.WindowInfo;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlConstants;
import io.confluent.ksql.util.PersistentQueryMetadata;
import io.confluent.ksql.util.QueryMetadata;
import io.confluent.ksql.util.TransientQueryMetadata;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.function.BiPredicate;
import org.apache.kafka.common.TopicPartition;

/**
 * Interface for building and managing queries.
 * <p>
 * Callers can build transient and persistent queries. Once built, the queries are owned by the
 * implementation of this interface. The caller creating the query receives a handle to the created
 * query. The handle is a child class of {@link io.confluent.ksql.util.QueryMetadata}.
 * </p>
 * <p>
 * Callers can also get a handle to to a query via the query ID, and list all queries managed by
 * a registry.
 * </p>
 * <p>
 * QueryRegistry also supports tracking the sink data source for each query. In particular, callers
 * can ask for the query that was started by the C(S|T)AS statement that created a source, and can
 * get a list of all queries writing into a source.
 * </p>
 * <p>
 * Finally, the implementation should support subscribing to events about queries by implementing
 * the {@link io.confluent.ksql.engine.QueryEventListener} interface.
 * </p>
 */
public interface QueryRegistry {

  /**
   * Create a transient query
   */
  // CHECKSTYLE_RULES.OFF: ParameterNumberCheck
  TransientQueryMetadata createTransientQuery(
      SessionConfig config,
      ServiceContext serviceContext,
      ProcessingLogContext processingLogContext,
      MetaStore metaStore,
      String statementText,
      QueryId queryId,
      Set<SourceName> sources,
      ExecutionStep<?> physicalPlan,
      String planSummary,
      LogicalSchema schema,
      OptionalInt limit,
      Optional<WindowInfo> windowInfo,
      boolean excludeTombstones
  );
  // CHECKSTYLE_RULES.ON: ParameterNumberCheck

  /**
   * Create a transient query
   */
  // CHECKSTYLE_RULES.OFF: ParameterNumberCheck
  TransientQueryMetadata createStreamPullQuery(
      SessionConfig config,
      ServiceContext serviceContext,
      ProcessingLogContext processingLogContext,
      MetaStore metaStore,
      String statementText,
      QueryId queryId,
      Set<SourceName> sources,
      ExecutionStep<?> physicalPlan,
      String planSummary,
      LogicalSchema schema,
      OptionalInt limit,
      Optional<WindowInfo> windowInfo,
      boolean excludeTombstones,
      ImmutableMap<TopicPartition, Long> endOffsets
  );
  // CHECKSTYLE_RULES.ON: ParameterNumberCheck

  /**
   * Create a persistent query, and possibly replace an existing query if one exists with the same
   * ID. Replacement will fail if migration from the current to the new physical plan is not
   * supported.
   */
  // CHECKSTYLE_RULES.OFF: ParameterNumberCheck
  PersistentQueryMetadata createOrReplacePersistentQuery(
      SessionConfig config,
      ServiceContext serviceContext,
      ProcessingLogContext processingLogContext,
      MetaStore metaStore,
      String statementText,
      QueryId queryId,
      Optional<DataSource> sinkDataSource,
      Set<DataSource> sources,
      ExecutionStep<?> physicalPlan,
      String planSummary,
      KsqlConstants.PersistentQueryType persistentQueryType,
      Optional<String> sharedRuntimeId
  );
  // CHECKSTYLE_RULES.ON: ParameterNumberCheck

  /**
   * Get a persistent query by ID
   * @param queryId The ID of the persistent query to get
   * @return The query with ID queryID, if such a query is registered.
   */
  Optional<PersistentQueryMetadata> getPersistentQuery(QueryId queryId);

  /**
   * Get any query by ID
   * @param queryId The ID of the query to get
   * @return The query with ID queryID, if such a query is registered.
   */
  Optional<QueryMetadata> getQuery(QueryId queryId);

  /**
   * Get all persistent queries
   * @return All persistent queries, keyed by ID
   */
  Map<QueryId, PersistentQueryMetadata> getPersistentQueries();

  /**
   * Get all queries with a given sink
   * @param sourceName The name of the source to list queries for
   * @return A set of query IDs that contains the IDs of queries writing into sourceName
   */
  Set<QueryId> getQueriesWithSink(SourceName sourceName);

  /**
   * Get all queries registered with the registry
   * @return List of all queries that are registered (both transient and persistent).
   */
  List<QueryMetadata> getAllLiveQueries();

  /**
   * Get the query started by the C(S|T)AS statement that created a given source
   * @param sourceName the name of the source to get the creating query for
   * @return the query started by the C(S|T)AS that created sourceName
   */
  Optional<QueryMetadata> getCreateAsQuery(SourceName sourceName);

  /**
   * Updates streams properties and restarts the streams runtimes
   */
  void updateStreamsPropertiesAndRestartRuntime(KsqlConfig config, ProcessingLogContext logContext);

  /**
   * Get all insert queries that write into or read from a given source.
   * @param sourceName The source name to fetch queries for.
   * @param filterQueries A predicate to apply to filter the returned list of queries
   * @return All insert queries that match the predicate and write into the given source.
   */
  Set<QueryId> getInsertQueries(
      SourceName sourceName,
      BiPredicate<SourceName, PersistentQueryMetadata> filterQueries
  );

  /**
   * Create a sandbox copy of the query registry. The sandbox can be used for validation, but the
   * contained queries cannot be started, stopped, or closed.
   * @return A sandbox instance of the current registry.
   */
  QueryRegistry createSandbox();

  /**
   * Close the query registry and free up its resources.
   * @param closePersistent if true, the underlying queries will be closed, meaning all state will
   *                        be destroyed. If false, the persistent queries owned by the registry
   *                        will be stopped by calling stop(). Transient queries are always closed.
   */
  void close(boolean closePersistent);

  /**
   * Close all shared runtimes in this registry
   */
  void closeRuntimes();
}
