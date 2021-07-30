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
import io.confluent.ksql.rest.entity.KsqlEntity;
import io.confluent.ksql.rest.entity.TerminateQueryEntity;
import io.confluent.ksql.rest.server.computation.DistributingExecutor;
import io.confluent.ksql.security.KsqlSecurityContext;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.statement.ConfiguredStatement;
import java.util.Optional;

public final class TerminateQueryExecutor {

  private TerminateQueryExecutor() { }

  public static Optional<KsqlEntity> execute(
      final ConfiguredStatement<TerminateQuery> statement,
      final SessionProperties sessionProperties,
      final KsqlExecutionContext executionContext,
      final ServiceContext serviceContext,
      final DistributingExecutor distributingExecutor,
      final KsqlSecurityContext securityContext
  ) {
    final TerminateQuery terminateQuery = statement.getStatement();
    final Optional<QueryId> queryId = terminateQuery.getQueryId();
    final RemoteHostExecutor remoteHostExecutor = RemoteHostExecutor.create(
        statement,
        sessionProperties,
        executionContext,
        serviceContext.getKsqlClient()
    );

    if (executionContext.getPersistentQuery(queryId.get()).isPresent()) {
      // do default behaviour for terminating persistent queries
      return distributingExecutor.execute(statement, executionContext, securityContext);
    } else {
      // Check are we running this push query locally, if yes then terminate, otherwise
      // propagate terminate query to other nodes
      if (executionContext.getQuery(queryId.get()).isPresent()) {
        executionContext.getQuery(queryId.get()).get().close();
        return Optional.of(
            new TerminateQueryEntity(statement.getStatementText(), queryId.get().toString(), true)
        );
      } else {
        final boolean wasTerminatedLocally = remoteHostExecutor.fetchAllRemoteResults().getLeft()
            .values()
            .stream()
            .map(TerminateQueryEntity.class::cast)
            .map(TerminateQueryEntity::getWasTerminatedLocally)
            .anyMatch(b -> b.equals(true));
        return Optional.of(new TerminateQueryEntity(
            statement.getStatementText(),
            queryId.get().toString(),
            wasTerminatedLocally));
      }
    }
//    return Optional.empty();
  }
}

