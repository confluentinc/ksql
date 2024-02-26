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

import io.confluent.ksql.GenericKey;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.materialization.MaterializationInfo;
import io.confluent.ksql.execution.plan.ExecutionKeyFactory;
import io.confluent.ksql.execution.plan.KGroupedTableHolder;
import io.confluent.ksql.execution.plan.KTableHolder;
import io.confluent.ksql.execution.plan.TableAggregate;
import io.confluent.ksql.execution.runtime.MaterializedFactory;
import io.confluent.ksql.execution.runtime.RuntimeBuildContext;
import io.confluent.ksql.execution.streams.transform.KsValueTransformer;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import java.util.List;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.state.KeyValueStore;

public final class TableAggregateBuilder {
  private TableAggregateBuilder() {
  }

  public static KTableHolder<GenericKey> build(
      final KGroupedTableHolder groupedTable,
      final TableAggregate aggregate,
      final RuntimeBuildContext buildContext,
      final MaterializedFactory materializedFactory) {
    return build(
        groupedTable,
        aggregate,
        buildContext,
        materializedFactory,
        new AggregateParamsFactory()
    );
  }

  public static KTableHolder<GenericKey> build(
      final KGroupedTableHolder groupedTable,
      final TableAggregate aggregate,
      final RuntimeBuildContext buildContext,
      final MaterializedFactory materializedFactory,
      final AggregateParamsFactory aggregateParamsFactory
  ) {
    final LogicalSchema sourceSchema = groupedTable.getSchema();
    final List<ColumnName> nonFuncColumns = aggregate.getNonAggregateColumns();
    final AggregateParams aggregateParams = aggregateParamsFactory.createUndoable(
        sourceSchema,
        nonFuncColumns,
        buildContext.getFunctionRegistry(),
        aggregate.getAggregationFunctions(),
        buildContext.getKsqlConfig()
    );
    final LogicalSchema aggregateSchema = aggregateParams.getAggregateSchema();
    final LogicalSchema resultSchema = aggregateParams.getSchema();
    final Materialized<GenericKey, GenericRow, KeyValueStore<Bytes, byte[]>> materialized =
        MaterializationUtil.buildMaterialized(
            aggregate,
            aggregateSchema,
            aggregate.getInternalFormats(),
            buildContext,
            materializedFactory,
            ExecutionKeyFactory.unwindowed(buildContext)
        );
    final KTable<GenericKey, GenericRow> aggregated = groupedTable.getGroupedTable().aggregate(
        aggregateParams.getInitializer(),
        aggregateParams.getAggregator(),
        aggregateParams.getUndoAggregator().get(),
        materialized
    ).transformValues(
        () -> new KsValueTransformer<>(
            aggregateParams.<GenericKey>getAggregator().getResultMapper()
        ),
        Named.as(StreamsUtil.buildOpName(AggregateBuilderUtils.outputContext(aggregate)))
    );

    final MaterializationInfo.Builder materializationBuilder =
        AggregateBuilderUtils.materializationInfoBuilder(
            aggregateParams.getAggregator(),
            aggregate,
            aggregateSchema,
            resultSchema
        );

    return KTableHolder.materialized(
        aggregated,
        resultSchema,
        ExecutionKeyFactory.unwindowed(buildContext),
        materializationBuilder
    );
  }
}
