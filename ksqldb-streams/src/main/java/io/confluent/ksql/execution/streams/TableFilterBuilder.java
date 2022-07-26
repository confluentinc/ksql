/*
 * Copyright 2022 Confluent Inc.
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
import io.confluent.ksql.execution.context.QueryContext.Stacker;
import io.confluent.ksql.execution.plan.KTableHolder;
import io.confluent.ksql.execution.plan.TableFilter;
import io.confluent.ksql.execution.runtime.RuntimeBuildContext;
import io.confluent.ksql.execution.streams.transform.KsValueTransformer;
import io.confluent.ksql.execution.transform.sqlpredicate.SqlPredicate;
import io.confluent.ksql.logging.processing.ProcessingLogger;
import java.util.Optional;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Named;

public final class TableFilterBuilder {
  private static final String PRE_PROCESS_OP = "ApplyPredicate";
  private static final String FILTER_OP = "Filter";
  private static final String POST_PROCESS_OP = "PostProcess";


  private TableFilterBuilder() {
  }

  public static <K> KTableHolder<K> build(
      final KTableHolder<K> table,
      final TableFilter<K> step,
      final RuntimeBuildContext buildContext) {
    return build(table, step, buildContext, SqlPredicate::new);
  }

  static <K> KTableHolder<K> build(
      final KTableHolder<K> table,
      final TableFilter<K> step,
      final RuntimeBuildContext buildContext,
      final SqlPredicateFactory sqlPredicateFactory
  ) {
    final SqlPredicate predicate = sqlPredicateFactory.create(
        step.getFilterExpression(),
        table.getSchema(),
        buildContext.getKsqlConfig(),
        buildContext.getFunctionRegistry()
    );

    final ProcessingLogger processingLogger = buildContext
        .getProcessingLogger(step.getProperties().getQueryContext());

    final Stacker stacker = Stacker.of(step.getProperties().getQueryContext());
    final KTable<K, GenericRow> filtered = table.getTable()
        .transformValues(
            () -> new KsValueTransformer<>(predicate.getTransformer(processingLogger)),
            Named.as(StreamsUtil.buildOpName(stacker.push(PRE_PROCESS_OP).getQueryContext()))
        )
        .filter(
            (k, v) -> v.isPresent(),
            Named.as(StreamsUtil.buildOpName(stacker.push(FILTER_OP).getQueryContext()))
        )
        .mapValues(
            Optional::get,
            Named.as(StreamsUtil.buildOpName(stacker.push(POST_PROCESS_OP).getQueryContext()))
        );

    return table
        .withTable(
            filtered,
            table.getSchema()
        )
        .withMaterialization(
            table.getMaterializationBuilder().map(b -> b.filter(
                predicate::getTransformer,
                step.getProperties().getQueryContext()
            ))
        );
  }
}
