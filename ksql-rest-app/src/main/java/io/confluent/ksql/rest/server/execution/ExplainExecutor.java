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
import io.confluent.ksql.rest.entity.KsqlEntity;
import io.confluent.ksql.rest.entity.QueryDescription;
import io.confluent.ksql.rest.entity.QueryDescriptionEntity;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.KsqlStatementException;
import io.confluent.ksql.util.PersistentQueryMetadata;
import io.confluent.ksql.util.QueryMetadata;
import java.util.Map;
import java.util.Optional;

/**
 * Explains the execution of either an existing persistent query or a statement
 * that has not yet been issued.
 */
public final class ExplainExecutor {

  private ExplainExecutor() { }

  @SuppressWarnings("unchecked")
  public static Optional<KsqlEntity> execute(
      final PreparedStatement<?> statement,
      final KsqlExecutionContext executionContext,
      final ServiceContext serviceContext,
      final KsqlConfig ksqlConfig,
      final Map<String, Object> propertyOverrides
  ) {
    return Optional
        .of(ExplainExecutor.explain(
            (PreparedStatement<Explain>) statement,
            propertyOverrides,
            ksqlConfig,
            executionContext));
  }

  /**
   * This method explains a prepared statement's execution plan, but does <i>not</i>
   * have any side-effects.
   *
   * @return explains the given statement contextualized by the parameters
   */
  private static QueryDescriptionEntity explain(
      final PreparedStatement<Explain> statement,
      final Map<String, Object> propertyOverrides,
      final KsqlConfig ksqlConfig,
      final KsqlExecutionContext executionContext
  ) {
    final Optional<String> queryId = statement.getStatement().getQueryId();

    try {
      final QueryDescription queryDescription = queryId
          .map(s -> explainQuery(s, executionContext))
          .orElseGet(() -> explainStatement(
              statement.getStatement().getStatement().orElseThrow(
                  () -> new KsqlStatementException(
                      "must have either queryID or statement",
                      statement.getStatementText())),
              statement.getStatementText().substring("EXPLAIN ".length()),
              executionContext,
              ksqlConfig,
              propertyOverrides));

      return new QueryDescriptionEntity(statement.getStatementText(), queryDescription);
    } catch (final KsqlException e) {
      throw new KsqlStatementException(e.getMessage(), statement.getStatementText(), e);
    }
  }

  private static QueryDescription explainStatement(
      final Statement statement,
      final String statementText,
      final KsqlExecutionContext executionContext,
      final KsqlConfig ksqlConfig,
      final Map<String, Object> propertyOverrides
  ) {
    if (!(statement instanceof Query || statement instanceof QueryContainer)) {
      throw new KsqlException("The provided statement does not run a ksql query");
    }

    final QueryMetadata metadata = executionContext.createSandbox().execute(
        PreparedStatement.of(statementText, statement),
        ksqlConfig, propertyOverrides)
        .getQuery()
        .orElseThrow(() ->
            new IllegalStateException("The provided statement did not run a ksql query"));

    return QueryDescription.forQueryMetadata(metadata);
  }

  private static QueryDescription explainQuery(
      final String queryId,
      final KsqlExecutionContext executionContext
  ) {
    final PersistentQueryMetadata metadata = executionContext
        .getPersistentQuery(new QueryId(queryId))
        .orElseThrow(() -> new KsqlException(
            "Query with id:" + queryId + " does not exist, "
                + "use SHOW QUERIES to view the full set of queries."));

    return QueryDescription.forQueryMetadata(metadata);
  }

}
