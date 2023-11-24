/*
 * Copyright 2021 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"; you may not use
 * this file except in compliance with the License. You may obtain a copy of the
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
import io.confluent.ksql.parser.tree.TerminateQuery;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.rest.SessionProperties;
import io.confluent.ksql.rest.entity.TerminateQueryEntity;
import io.confluent.ksql.rest.util.TerminateCluster;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.util.KsqlException;
import java.util.Optional;

public final class TerminateQueryExecutor {

  private TerminateQueryExecutor() {
  }

  public static StatementExecutorResponse execute(
      final ConfiguredStatement<TerminateQuery> statement,
      final SessionProperties sessionProperties,
      final KsqlExecutionContext executionContext,
      final ServiceContext serviceContext
  ) {
    final TerminateQuery terminateQuery = statement.getStatement();
    // do default behaviour for TERMINATE ALL
    if (!terminateQuery.getQueryId().isPresent()) {
      return StatementExecutorResponse.notHandled();
    }
    final QueryId queryId = terminateQuery.getQueryId().get();
    final RemoteHostExecutor remoteHostExecutor = RemoteHostExecutor.create(
        statement,
        sessionProperties,
        executionContext,
        serviceContext.getKsqlClient()
    );

    if (executionContext.getPersistentQuery(queryId).isPresent()
        || statement.getUnMaskedStatementText().equals(
            TerminateCluster.TERMINATE_CLUSTER_STATEMENT_TEXT)) {
      // do default behaviour for terminating persistent queries
      return StatementExecutorResponse.notHandled();
    } else {
      // Check are we running this push query locally, if yes then terminate, otherwise
      // propagate terminate query to other nodes
      if (executionContext.getQuery(queryId).isPresent()) {
        executionContext.getQuery(queryId).get().close();
      } else {
        final boolean wasTerminatedRemotely = remoteHostExecutor.fetchAllRemoteResults().getLeft()
            .values()
            .stream()
            .map(TerminateQueryEntity.class::cast)
            .map(TerminateQueryEntity::getWasTerminated)
            .anyMatch(b -> b.equals(true));
        if (!wasTerminatedRemotely) {
          throw new KsqlException(String.format(
              "Failed to terminate query with query ID: '%s'",
              queryId));
        }
      }
      return StatementExecutorResponse.handled(Optional.of(
          new TerminateQueryEntity(statement.getMaskedStatementText(), queryId.toString(), true)
      ));
    }
  }
}
