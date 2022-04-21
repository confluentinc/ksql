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
import io.confluent.ksql.parser.tree.ResumeQuery;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.rest.SessionProperties;
import io.confluent.ksql.rest.entity.ResumeQueryEntity;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.util.KsqlException;
import java.util.Optional;

public final class ResumeQueryExecutor {

  private ResumeQueryExecutor() {
  }

  public static StatementExecutorResponse execute(
      final ConfiguredStatement<ResumeQuery> statement,
      final SessionProperties sessionProperties,
      final KsqlExecutionContext executionContext,
      final ServiceContext serviceContext
  ) {
    final ResumeQuery resumeQuery = statement.getStatement();
    System.out.println("Handling " + resumeQuery);

    // do default behaviour for RESUME ALL
    // JNH: How does this do the default behavior for RESUME (TERMINATE) ALL?
    if (!resumeQuery.getQueryId().isPresent()) {
      System.out.println("RESUME ALL?");
      return StatementExecutorResponse.notHandled();
    }

    // RESUME locally
    final QueryId queryId = resumeQuery.getQueryId().get();
    System.out.println("Resuming Query locally: " + queryId);
    boolean resumedLocally = false;
    if (executionContext.getQuery(queryId).isPresent()) {
      resumedLocally = true;
      // JNH - call:resume
      executionContext.getQuery(queryId).get().resume();
    }

    // JNH: See how the RemoteHostExecutor works.
    // RESUME Remotely
    final RemoteHostExecutor remoteHostExecutor = RemoteHostExecutor.create(
        statement,
        sessionProperties,
        executionContext,
        serviceContext.getKsqlClient()
    );

    System.out.println("Resuming Query remotely: " + queryId);
    final boolean wasResumedRemotely = remoteHostExecutor.fetchAllRemoteResults().getLeft()
        .values()
        .stream()
        .map(ResumeQueryEntity.class::cast)
        .map(ResumeQueryEntity::getWasResumed)
        .anyMatch(b -> b.equals(true));
    if (!resumedLocally && !wasResumedRemotely) {
      throw new KsqlException(String.format(
          "Failed to resume query with query ID: '%s'",
          queryId));
    }

    return StatementExecutorResponse.handled(Optional.of(
        new ResumeQueryEntity(statement.getStatementText(), queryId.toString(), true)
    ));

  }
}
