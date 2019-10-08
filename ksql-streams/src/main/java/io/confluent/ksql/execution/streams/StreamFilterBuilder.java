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

package io.confluent.ksql.execution.streams;

import io.confluent.ksql.execution.builder.KsqlQueryBuilder;
import io.confluent.ksql.execution.context.QueryContext;
import io.confluent.ksql.execution.context.QueryLoggerUtil;
import io.confluent.ksql.execution.plan.KStreamHolder;
import io.confluent.ksql.execution.plan.StreamFilter;
import io.confluent.ksql.execution.sqlpredicate.SqlPredicate;

public final class StreamFilterBuilder {
  private StreamFilterBuilder() {
  }

  public static <K> KStreamHolder<K> build(
      final KStreamHolder<K> stream,
      final StreamFilter<K> step,
      final KsqlQueryBuilder queryBuilder) {
    return build(stream, step, queryBuilder, SqlPredicate::new);
  }

  static <K> KStreamHolder<K> build(
      final KStreamHolder<K> stream,
      final StreamFilter<K> step,
      final KsqlQueryBuilder queryBuilder,
      final SqlPredicateFactory predicateFactory) {
    final QueryContext.Stacker contextStacker = QueryContext.Stacker.of(
        step.getProperties().getQueryContext()
    );
    final SqlPredicate predicate = predicateFactory.create(
        step.getFilterExpression(),
        step.getSource().getProperties().getSchema(),
        queryBuilder.getKsqlConfig(),
        queryBuilder.getFunctionRegistry(),
        queryBuilder.getProcessingLogContext().getLoggerFactory().getLogger(
            QueryLoggerUtil.queryLoggerName(
                queryBuilder.getQueryId(),
                contextStacker.push("FILTER").getQueryContext())
        )
    );
    return stream.withStream(
        stream.getStream().filter(predicate.getPredicate())
    );
  }
}
