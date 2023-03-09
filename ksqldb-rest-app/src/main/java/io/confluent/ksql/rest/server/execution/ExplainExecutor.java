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
import io.confluent.ksql.parser.tree.Explain;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.parser.tree.QueryContainer;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.rest.SessionProperties;
import io.confluent.ksql.rest.entity.KsqlHostInfoEntity;
import io.confluent.ksql.rest.entity.QueryDescription;
import io.confluent.ksql.rest.entity.QueryDescriptionEntity;
import io.confluent.ksql.rest.entity.QueryDescriptionFactory;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.util.KsqlConstants;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.KsqlStatementException;
import io.confluent.ksql.util.PersistentQueryMetadata;
import io.confluent.ksql.util.QueryMetadata;
import java.util.Collections;
import java.util.Optional;

/**
 * Explains the execution of either an existing persistent query or a statement that has not yet
 * been issued.
 */
public final class ExplainExecutor {

  private ExplainExecutor() {
  }

  public static StatementExecutorResponse execute(
      final ConfiguredStatement<Explain> statement,
      final SessionProperties sessionProperties,
      final KsqlExecutionContext executionContext,
      final ServiceContext serviceContext
  ) {
    return StatementExecutorResponse.handled(Optional
        .of(ExplainExecutor.explain(
            serviceContext,
            statement,
            executionContext,
            sessionProperties)));
  }

  /**
   * This method explains a prepared statement's execution plan, but does <i>not</i>
   * have any side-effects.
   *
   * @return explains the given statement contextualized by the parameters
   */
  private static QueryDescriptionEntity explain(
      final ServiceContext serviceContext,
      final ConfiguredStatement<Explain> statement,
      final KsqlExecutionContext executionContext,
      final SessionProperties sessionProperties
  ) {
    final Optional<String> queryId = statement.getStatement().getQueryId();

    try {
      final QueryDescription queryDescription = queryId
          .map(s -> explainQuery(s, executionContext, sessionProperties))
          .orElseGet(() -> explainStatement(
              statement, executionContext, serviceContext));

      return new QueryDescriptionEntity(statement.getMaskedStatementText(), queryDescription);
    } catch (final KsqlException e) {
      throw new KsqlStatementException(e.getMessage(), statement.getMaskedStatementText(), e);
    }
  }

  private static QueryDescription explainStatement(
      final ConfiguredStatement<Explain> explain,
      final KsqlExecutionContext executionContext,
      final ServiceContext serviceContext
  ) {
    final Statement statement = explain.getStatement()
        .getStatement()
        .orElseThrow(() -> new KsqlStatementException(
            "must have either queryID or statement",
            explain.getMaskedStatementText()));

    if (!(statement instanceof Query || statement instanceof QueryContainer)) {
      throw new KsqlException("The provided statement does not run a ksql query");
    }

    final PreparedStatement<?> preparedStatement = PreparedStatement.of(
        explain.getUnMaskedStatementText().substring("EXPLAIN ".length()),
        statement);

    final QueryMetadata metadata;
    final KsqlExecutionContext sandbox = executionContext.createSandbox(serviceContext);
    if (preparedStatement.getStatement() instanceof Query) {
      metadata = sandbox.executeTransientQuery(
          serviceContext,
          ConfiguredStatement.of(preparedStatement, explain.getSessionConfig()).cast(),
          false);
    } else {
      metadata = sandbox
          .execute(
              serviceContext,
              ConfiguredStatement.of(preparedStatement, explain.getSessionConfig()))
          .getQuery()
          .orElseThrow(() ->
              new IllegalStateException("The provided statement did not run a ksql query"));
    }

    return QueryDescriptionFactory.forQueryMetadata(
        metadata,
        Collections.emptyMap());
  }

  private static QueryDescription explainQuery(
      final String queryId,
      final KsqlExecutionContext executionContext,
      final SessionProperties sessionProperties
  ) {
    final PersistentQueryMetadata metadata = executionContext
        .getPersistentQuery(new QueryId(queryId))
        .orElseThrow(() -> new KsqlException(
            "Query with id:" + queryId + " does not exist, "
                + "use SHOW QUERIES to view the full set of queries."));

    return QueryDescriptionFactory.forQueryMetadata(
        metadata,
        Collections.singletonMap(
            new KsqlHostInfoEntity(sessionProperties.getKsqlHostInfo()),
                KsqlConstants.fromStreamsState(
                    metadata.getState())
        ));
  }

}
