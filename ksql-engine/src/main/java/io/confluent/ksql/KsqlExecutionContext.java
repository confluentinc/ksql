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

import io.confluent.ksql.engine.KsqlEngine;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.parser.KsqlParser.ParsedStatement;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.PersistentQueryMetadata;
import io.confluent.ksql.util.QueryMetadata;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

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
  KsqlExecutionContext createSandbox();

  /**
   * @return read-only access to the context's {@link MetaStore}.
   */
  MetaStore getMetaStore();

  /**
   * @return access to the context's {@link FunctionRegistry}.
   */
  FunctionRegistry getFunctionRegistry();

  /**
   * @return the service context used for this execution context
   */
  ServiceContext getServiceContext();

  /**
   * Retrieve the details of a persistent query.
   *
   * @param queryId the id of the query to retrieve.
   * @return the query's details or else {@code Optional.empty()} if no found.
   */
  Optional<PersistentQueryMetadata> getPersistentQuery(QueryId queryId);

  /**
   * Retrieves the list of all running persistent queries.
   *
   * @return the list of all persistent queries
   * @see #getPersistentQuery(QueryId)
   */
  List<PersistentQueryMetadata> getPersistentQueries();

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
   * @param stmt the parsed statement.
   * @return the prepared statement.
   */
  PreparedStatement<?> prepare(ParsedStatement stmt);

  /**
   * Execute the supplied statement, updating the meta store and registering any query.
   *
   * <p>The statement must be executable. See {@link KsqlEngine#isExecutableStatement}.
   *
   * <p>If the statement contains a query, then it will be tracked, but not started.
   *
   * @param statement The SQL to execute.
   * @param ksqlConfig the config.
   * @param overriddenProperties The user-requested property overrides.
   * @return The execution result.
   */
  ExecuteResult execute(
      PreparedStatement<?> statement,
      KsqlConfig ksqlConfig,
      Map<String, Object> overriddenProperties);

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
