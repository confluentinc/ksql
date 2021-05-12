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

import static java.util.Objects.requireNonNull;

import io.confluent.ksql.GenericKey;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.codegen.CodeGenRunner;
import io.confluent.ksql.execution.codegen.CompiledExpression;
import io.confluent.ksql.execution.context.QueryContext;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.plan.Formats;
import io.confluent.ksql.execution.plan.KGroupedTableHolder;
import io.confluent.ksql.execution.plan.KTableHolder;
import io.confluent.ksql.execution.runtime.RuntimeBuildContext;
import io.confluent.ksql.logging.processing.ProcessingLogger;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.PhysicalSchema;
import java.util.List;
import java.util.function.Function;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KGroupedTable;
import org.apache.kafka.streams.kstream.KeyValueMapper;

class TableGroupByBuilderBase {

  private final RuntimeBuildContext buildContext;
  private final GroupedFactory groupedFactory;
  private final ParamsFactory paramsFactory;

  TableGroupByBuilderBase(
      final RuntimeBuildContext buildContext,
      final GroupedFactory groupedFactory,
      final ParamsFactory paramsFactory
  ) {
    this.buildContext = requireNonNull(buildContext, "buildContext");
    this.groupedFactory = requireNonNull(groupedFactory, "groupedFactory");
    this.paramsFactory = requireNonNull(paramsFactory, "paramsFactory");
  }

  public <K> KGroupedTableHolder build(
      final KTableHolder<K> table,
      final QueryContext queryContext,
      final Formats formats,
      final List<Expression> groupByExpressions
  ) {
    final LogicalSchema sourceSchema = table.getSchema();

    final List<CompiledExpression> groupBy = CodeGenRunner.compileExpressions(
        groupByExpressions.stream(),
        "Group By",
        sourceSchema,
        buildContext.getKsqlConfig(),
        buildContext.getFunctionRegistry()
    );

    final ProcessingLogger logger = buildContext.getProcessingLogger(queryContext);

    final GroupByParams params = paramsFactory
        .build(sourceSchema, groupBy, logger);

    final PhysicalSchema physicalSchema = PhysicalSchema.from(
        params.getSchema(),
        formats.getKeyFeatures(),
        formats.getValueFeatures()
    );

    final Serde<GenericKey> keySerde = buildContext.buildKeySerde(
        formats.getKeyFormat(),
        physicalSchema,
        queryContext
    );
    final Serde<GenericRow> valSerde = buildContext.buildValueSerde(
        formats.getValueFormat(),
        physicalSchema,
        queryContext
    );
    final Grouped<GenericKey, GenericRow> grouped = groupedFactory.create(
        StreamsUtil.buildOpName(queryContext),
        keySerde,
        valSerde
    );

    final KGroupedTable<GenericKey, GenericRow> groupedTable = table.getTable()
        .filter((k, v) -> v != null)
        .groupBy(new TableKeyValueMapper<>(params.getMapper()), grouped);

    return KGroupedTableHolder.of(groupedTable, params.getSchema());
  }

  public static final class TableKeyValueMapper<K>
      implements KeyValueMapper<K, GenericRow, KeyValue<GenericKey, GenericRow>> {

    private final Function<GenericRow, GenericKey> groupByMapper;

    private TableKeyValueMapper(final Function<GenericRow, GenericKey> groupByMapper) {
      this.groupByMapper = requireNonNull(groupByMapper, "groupByMapper");
    }

    @Override
    public KeyValue<GenericKey, GenericRow> apply(final K key, final GenericRow value) {
      return new KeyValue<>(groupByMapper.apply(value), value);
    }
  }

  interface ParamsFactory {

    GroupByParams build(
        LogicalSchema sourceSchema,
        List<CompiledExpression> groupBys,
        ProcessingLogger logger
    );
  }
}
