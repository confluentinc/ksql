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
          + "Consider removing 'EMIT CHANGES' to any bare query."
          + QueryAnalyzer.NEW_QUERY_SYNTAX_HELP
      );
    }

    if (sink.isPresent()) {
      throw new IllegalArgumentException("static queries should not have a sink");
    }
  }

  @Override
  public void postValidate(final Analysis analysis) {
    if (analysis.getInto().isPresent()) {
      throw new KsqlException("Static queries do not support outputting to sinks.");
    }

    if (analysis.isJoin()) {
      throw new KsqlException("Static queries do not support joins.");
    }

    if (analysis.getWindowExpression().isPresent()) {
      throw new KsqlException("Static queries do not support WINDOW clauses.");
    }

    if (!analysis.getGroupByExpressions().isEmpty()) {
      throw new KsqlException("Static queries do not support GROUP BY clauses.");
    }

    if (analysis.getPartitionBy().isPresent()) {
      throw new KsqlException("Static queries do not support PARTITION BY clauses.");
    }

    if (analysis.getHavingExpression().isPresent()) {
      throw new KsqlException("Static queries do not support HAVING clauses.");
    }

    if (analysis.getLimitClause().isPresent()) {
      throw new KsqlException("Static queries do not support LIMIT clauses.");
    }
  }
}
