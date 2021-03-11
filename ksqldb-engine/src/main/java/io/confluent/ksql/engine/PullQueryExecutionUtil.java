/*
 * Copyright 2019 Confluent Inc.
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

package io.confluent.ksql.engine;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.RateLimiter;
import io.confluent.ksql.analyzer.ImmutableAnalysis;
import io.confluent.ksql.analyzer.PullQueryValidator;
import io.confluent.ksql.engine.rewrite.ExpressionTreeRewriter.Context;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.expression.tree.QualifiedColumnReferenceExp;
import io.confluent.ksql.execution.expression.tree.UnqualifiedColumnReferenceExp;
import io.confluent.ksql.execution.expression.tree.VisitParentExpressionVisitor;
import io.confluent.ksql.metastore.model.DataSource;
import io.confluent.ksql.metastore.model.DataSource.DataSourceType;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.PersistentQueryMetadata;
import java.util.Optional;
import java.util.Set;

public final class PullQueryExecutionUtil {

  private PullQueryExecutionUtil() {

  }

  @VisibleForTesting
  public static final class ColumnReferenceRewriter
      extends VisitParentExpressionVisitor<Optional<Expression>, Context<Void>> {

    ColumnReferenceRewriter() {
      super(Optional.empty());
    }

    @Override
    public Optional<Expression> visitQualifiedColumnReference(
        final QualifiedColumnReferenceExp node,
        final Context<Void> ctx
    ) {
      return Optional.of(new UnqualifiedColumnReferenceExp(node.getColumnName()));
    }
  }

  @VisibleForTesting
  public static void checkRateLimit(final RateLimiter rateLimiter) {
    if (!rateLimiter.tryAcquire()) {
      throw new KsqlException("Host is at rate limit for pull queries. Currently set to "
                                  + rateLimiter.getRate() + " qps.");
    }
  }

  static PersistentQueryMetadata findMaterializingQuery(
      final EngineContext engineContext, final ImmutableAnalysis analysis) {

    final DataSource source = analysis.getFrom().getDataSource();
    final SourceName sourceName = source.getName();

    final Set<QueryId> queries = engineContext.getQueriesWithSink(sourceName);

    if (source.getDataSourceType() != DataSourceType.KTABLE) {
      throw new KsqlException("Pull queries are not supported on streams."
          + PullQueryValidator.PULL_QUERY_SYNTAX_HELP);
    }

    if (queries.isEmpty()) {
      throw notMaterializedException(sourceName);
    }

    if (queries.size() > 1) {
      throw new IllegalStateException(
          "Tables do not support multiple queries writing into them, yet somehow this happened. "
              + "Source Name: " + sourceName + " Queries: " + queries + ". Please submit "
              + "a GitHub issue with the queries that were run.");
    }

    final QueryId queryId = Iterables.getOnlyElement(queries);

    return engineContext
        .getPersistentQuery(queryId)
        .orElseThrow(() -> new KsqlException("Materializing query has been stopped"));
  }

  private static KsqlException notMaterializedException(final SourceName sourceTable) {
    final String tableName = sourceTable.toString().replaceAll("`", "");
    return new KsqlException(
            "The " + sourceTable + " table isn't queryable. To derive a queryable table, "
                    + "you can do 'CREATE TABLE QUERYABLE_"
                    + tableName
                    + " AS SELECT * FROM "
                    + tableName
                    + "'."
                    + PullQueryValidator.PULL_QUERY_SYNTAX_HELP
    );
  }
}
