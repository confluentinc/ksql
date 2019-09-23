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

package io.confluent.ksql.analyzer;

import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.parser.tree.ResultMaterialization;
import io.confluent.ksql.parser.tree.Sink;
import io.confluent.ksql.util.KsqlException;
import java.util.Optional;

public class StaticQueryValidator implements QueryValidator {

  private static final String NEW_QUERY_SYNTAX_HELP = System.lineSeparator()
      + "Did you mean to execute a continuous query? If so, add an 'EMIT CHANGES' clause."
      + System.lineSeparator()
      + "Query syntax in KSQL has changed. There are now two broad categories of queries:"
      + System.lineSeparator()
      + "- Static queries: query the current state of the system, return a result and terminate."
      + System.lineSeparator()
      + "- Streaming queries: query the state of the system in motion and will continue to output "
      + "results until they meet any LIMIT clause criteria or are terminated by the user."
      + System.lineSeparator()
      + System.lineSeparator()
      + "'EMIT CHANGES' is used to indicate a query is continuous and outputs all changes."
      + "To turn a static query into a streaming query, as was the default in older versions "
      + "of KSQL, add `EMIT CHANGES` to the end of the statement before any limit clause."
      + System.lineSeparator()
      + System.lineSeparator()
      + "For example, the following are static queries:"
      + System.lineSeparator()
      + "\t'SELECT * FROM X WHERE ROWKEY=Y;' (non-windowed table)"
      + System.lineSeparator()
      + "\t'SELECT * FROM X WHERE ROWKEY=Y AND WINDOWSTART>=Z;' (windowed table)"
      + System.lineSeparator()
      + System.lineSeparator()
      + "and, the following is a streaming query:"
      + System.lineSeparator()
      + "\t'SELECT * FROM X EMIT CHANGES;'"
      + System.lineSeparator()
      + System.lineSeparator()
      + "Note: Persistent queries, e.g. `CREATE TABLE AS ...`, currently have an implicit "
      + "`EMIT CHANGES`. However, it is recommended to add `EMIT CHANGES` to such statements "
      + "going forward, as a this will be required in a future release.";

  @Override
  public void preValidate(
      final Query query,
      final Optional<Sink> sink
  ) {
    if (!query.isStatic()) {
      throw new IllegalArgumentException("not static");
    }

    if (query.getResultMaterialization() != ResultMaterialization.FINAL) {
      throw new KsqlException("Static queries do not yet support `EMIT CHANGES`. "
          + "Consider removing 'EMIT CHANGES' from any bare query."
          + NEW_QUERY_SYNTAX_HELP
      );
    }

    if (sink.isPresent()) {
      throw new IllegalArgumentException("static queries should not have a sink");
    }
  }

  @Override
  public void postValidate(final Analysis analysis) {
    try {
      if (analysis.getInto().isPresent()) {
        throw new KsqlException("Static queries do not support outputting to sinks.");
      }

      if (analysis.isJoin()) {
        throw new KsqlException("Static queries do not support joins.");
      }

      if (analysis.getWindowExpression() != null) {
        throw new KsqlException("Static queries do not support WINDOW clauses.");
      }

      if (!analysis.getGroupByExpressions().isEmpty()) {
        throw new KsqlException("Static queries do not support GROUP BY clauses.");
      }

      if (analysis.getPartitionBy().isPresent()) {
        throw new KsqlException("Static queries do not support PARTITION BY clauses.");
      }

      if (analysis.getHavingExpression() != null) {
        throw new KsqlException("Static queries do not support HAVING clauses.");
      }

      if (analysis.getLimitClause().isPresent()) {
        throw new KsqlException("Static queries do not support LIMIT clauses.");
      }
    } catch (final KsqlException e) {
      throw new KsqlException(e.getMessage() + NEW_QUERY_SYNTAX_HELP, e);
    }
  }
}
