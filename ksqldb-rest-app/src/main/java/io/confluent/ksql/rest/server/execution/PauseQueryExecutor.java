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
import io.confluent.ksql.parser.tree.PauseQuery;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.rest.SessionProperties;
import io.confluent.ksql.rest.entity.KsqlEntity;
import io.confluent.ksql.rest.entity.PauseQueryEntity;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.Pair;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.kafka.streams.state.HostInfo;

public final class PauseQueryExecutor {

  private PauseQueryExecutor() {
  }

  public static StatementExecutorResponse execute(
      final ConfiguredStatement<PauseQuery> statement,
      final SessionProperties sessionProperties,
      final KsqlExecutionContext executionContext,
      final ServiceContext serviceContext
  ) {
    final PauseQuery pauseQuery = statement.getStatement();
    System.out.println("Handling " + pauseQuery);

    // do default behaviour for PAUSE ALL
    // JNH: How does this do the default behavior for PAUSE (TERMINATE) ALL?
    if (!pauseQuery.getQueryId().isPresent()) {
      System.out.println("PAUSE ALL?");
      return StatementExecutorResponse.notHandled();
    } else {
      return StatementExecutorResponse.notHandled();
    }

    // PAUSE locally
//    final QueryId queryId = pauseQuery.getQueryId().get();
//    System.out.println("Pausing Query locally: " + queryId);
//    boolean pausedLocally = false;
//    if (executionContext.getQuery(queryId).isPresent()) {
//      pausedLocally = true;
//      // JNH - call:pause
//      executionContext.getQuery(queryId).get().pause();
//    }
//
//    // JNH: See how the RemoteHostExecutor works.
//    // PAUSE Remotely
//    final RemoteHostExecutor remoteHostExecutor = RemoteHostExecutor.create(
//        statement,
//        sessionProperties,
//        executionContext,
//        serviceContext.getKsqlClient()
//    );
//
//    System.out.println("Pausing Query remotely: " + queryId);
//    final Pair<Map<HostInfo, KsqlEntity>, Set<HostInfo>> res =
//        remoteHostExecutor.fetchAllRemoteResults();
//    if (res.left.isEmpty()) {
//      System.out.println("No remote servers were paused");
//    }
//    final boolean wasPausedRemotely = remoteHostExecutor.fetchAllRemoteResults().getLeft()
//        .values()
//        .stream()
//        .map(PauseQueryEntity.class::cast)
//        .map(PauseQueryEntity::getWasPaused)
//        .anyMatch(b -> b.equals(true));
//    if (!pausedLocally && !wasPausedRemotely) {
//      throw new KsqlException(String.format(
//          "Failed to pause query with query ID: '%s'",
//          queryId));
//    }
//
//    return StatementExecutorResponse.handled(Optional.of(
//        new PauseQueryEntity(statement.getStatementText(), queryId.toString(), true)
//    ));

  }
}
