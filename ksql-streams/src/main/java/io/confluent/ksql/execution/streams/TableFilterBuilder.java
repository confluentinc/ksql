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

import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.builder.KsqlQueryBuilder;
import io.confluent.ksql.execution.context.QueryContext;
import io.confluent.ksql.execution.context.QueryLoggerUtil;
import io.confluent.ksql.execution.plan.TableFilter;
import io.confluent.ksql.execution.sqlpredicate.SqlPredicate;
import org.apache.kafka.streams.kstream.KTable;

public final class TableFilterBuilder {
  private TableFilterBuilder() {
  }

  public static <K> KTable<K, GenericRow> build(
      final KTable<K, GenericRow> ktable,
      final TableFilter<KTable<K, GenericRow>> step,
      final KsqlQueryBuilder queryBuilder) {
    return build(ktable, step, queryBuilder, SqlPredicate::new);
  }

  static <K> KTable<K, GenericRow> build(
      final KTable<K, GenericRow> ktable,
      final TableFilter<KTable<K, GenericRow>> step,
      final KsqlQueryBuilder queryBuilder,
      final SqlPredicateFactory sqlPredicateFactory) {
    final QueryContext.Stacker contextStacker = QueryContext.Stacker.of(
        step.getProperties().getQueryContext()
    );
    final SqlPredicate predicate = sqlPredicateFactory.create(
        step.getFilterExpression(),
        step.getSource().getProperties().getSchema(),
        queryBuilder.getKsqlConfig(),
        queryBuilder.getFunctionRegistry(),
        queryBuilder.getProcessingLogContext().getLoggerFactory().getLogger(
            QueryLoggerUtil.queryLoggerName(
                contextStacker.push("FILTER").getQueryContext())
        )
    );
    return ktable.filter(predicate.getPredicate());
  }
}