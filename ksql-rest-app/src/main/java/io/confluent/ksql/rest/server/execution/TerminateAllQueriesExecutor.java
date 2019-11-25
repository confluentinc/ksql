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

package io.confluent.ksql.rest.server.execution;

import io.confluent.ksql.KsqlExecutionContext;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.parser.tree.TerminateAllQueries;
import io.confluent.ksql.parser.tree.TerminateQuery;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.rest.entity.KsqlEntity;
import io.confluent.ksql.rest.server.computation.DistributingExecutor;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.util.PersistentQueryMetadata;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Executes `TERMINATE ALL` by distributing `TERMINATE queryId` for each existing query id.
 */
final class TerminateAllQueriesExecutor implements StatementExecutor<TerminateAllQueries> {

  private final DistributingExecutor distributor;

  TerminateAllQueriesExecutor(
      final DistributingExecutor distributor
  ) {
    this.distributor = Objects.requireNonNull(distributor, "distributor");
  }

  public List<? extends KsqlEntity> execute(
      final ConfiguredStatement<TerminateAllQueries> statement,
      final Map<String, Object> mutableScopedProperties,
      final KsqlExecutionContext executionContext,
      final ServiceContext serviceContext
  ) {
    return executionContext.getPersistentQueries().stream()
        .map(PersistentQueryMetadata::getQueryId)
        .map(id -> buildTerminateStatement(id, statement))
        .flatMap(terminateStmt -> distributor.execute(
            terminateStmt,
            mutableScopedProperties,
            executionContext,
            serviceContext
            ).stream()
        )
        .collect(Collectors.toList());
  }

  private static ConfiguredStatement<Statement> buildTerminateStatement(
      final QueryId queryId,
      final ConfiguredStatement<TerminateAllQueries> parent
  ) {
    final TerminateQuery terminateQuery = new TerminateQuery(Optional.empty(), queryId);

    final PreparedStatement<Statement> prepared = PreparedStatement
        .of("TERMINATE " + queryId.getId() + ";", terminateQuery);

    return ConfiguredStatement.of(prepared, parent.getOverrides(), parent.getConfig());
  }
}
