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

package io.confluent.ksql.rest.server.computation;

import io.confluent.ksql.KsqlExecutionContext;
import io.confluent.ksql.engine.KsqlPlan;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.parser.tree.TerminateQuery;
import io.confluent.ksql.planner.plan.ConfiguredKsqlPlan;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.rest.util.TerminateCluster;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlStatementException;
import io.confluent.ksql.util.PersistentQueryMetadata;
import java.util.Objects;
import java.util.Optional;

/**
 * Creates commands that have been validated to successfully execute against
 * the given engine snapshot. Validated commands are safe to enqueue onto the
 * command queue.
 */
public final class ValidatedCommandFactory {
  private final KsqlConfig config;

  public ValidatedCommandFactory(final KsqlConfig config) {
    this.config = Objects.requireNonNull(config, "config");
  }

  /**
   * Create a validated command.
   * @param statement The KSQL statement to create the command for.
   * @param context The KSQL engine snapshot to validate the command against.
   * @return A validated command, which is safe to enqueue onto the command topic.
   */
  public Command create(
      final ConfiguredStatement<? extends Statement> statement,
      final KsqlExecutionContext context) {
    if (statement.getStatementText().equals(TerminateCluster.TERMINATE_CLUSTER_STATEMENT_TEXT)) {
      return Command.of(statement);
    } else if (statement.getStatement() instanceof TerminateQuery) {
      return createForTerminateQuery(statement, context);
    }
    return createForPlannedQuery(statement, context);
  }

  private Command createForTerminateQuery(
      final ConfiguredStatement<? extends Statement> statement,
      final KsqlExecutionContext context
  ) {
    final TerminateQuery terminateQuery = (TerminateQuery) statement.getStatement();
    final Optional<QueryId> queryId = terminateQuery.getQueryId();

    if (!queryId.isPresent()) {
      context.getPersistentQueries().forEach(PersistentQueryMetadata::close);
      return Command.of(statement);
    }

    context.getPersistentQuery(queryId.get())
        .orElseThrow(() -> new KsqlStatementException(
            "Unknown queryId: " + queryId.get(),
            statement.getStatementText()))
        .close();
    return Command.of(statement);
  }

  private Command createForPlannedQuery(
      final ConfiguredStatement<? extends Statement> statement,
      final KsqlExecutionContext context
  ) {
    final KsqlPlan plan = context.plan(context.getServiceContext(), statement);
    context.execute(
        context.getServiceContext(),
        ConfiguredKsqlPlan.of(
            plan,
            statement.getOverrides(),
            statement.getConfig()
        )
    );
    if (!config.getBoolean(KsqlConfig.KSQL_EXECUTION_PLANS_ENABLE)) {
      return Command.of(statement);
    }
    return Command.of(
        ConfiguredKsqlPlan.of(plan, statement.getOverrides(), statement.getConfig()));
  }
}
