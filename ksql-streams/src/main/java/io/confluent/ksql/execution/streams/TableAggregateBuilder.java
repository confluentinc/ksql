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
import io.confluent.ksql.execution.materialization.MaterializationInfo;
import io.confluent.ksql.execution.plan.KGroupedTableHolder;
import io.confluent.ksql.execution.plan.KTableHolder;
import io.confluent.ksql.execution.plan.KeySerdeFactory;
import io.confluent.ksql.execution.plan.TableAggregate;
import io.confluent.ksql.execution.streams.transform.KsTransformer;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.state.KeyValueStore;

public final class TableAggregateBuilder {
  private TableAggregateBuilder() {
  }

  public static KTableHolder<Struct> build(
      final KGroupedTableHolder groupedTable,
      final TableAggregate aggregate,
      final KsqlQueryBuilder queryBuilder,
      final MaterializedFactory materializedFactory) {
    return build(
        groupedTable,
        aggregate,
        queryBuilder,
        materializedFactory,
        new AggregateParamsFactory()
    );
  }

  public static KTableHolder<Struct> build(
      final KGroupedTableHolder groupedTable,
      final TableAggregate aggregate,
      final KsqlQueryBuilder queryBuilder,
      final MaterializedFactory materializedFactory,
      final AggregateParamsFactory aggregateParamsFactory
  ) {
    final LogicalSchema sourceSchema = groupedTable.getSchema();
    final int nonFuncColumns = aggregate.getNonFuncColumnCount();
    final AggregateParams aggregateParams = aggregateParamsFactory.createUndoable(
        sourceSchema,
        nonFuncColumns,
        queryBuilder.getFunctionRegistry(),
        aggregate.getAggregationFunctions()
    );
    final LogicalSchema aggregateSchema = aggregateParams.getAggregateSchema();
    final LogicalSchema resultSchema = aggregateParams.getSchema();
    final Materialized<Struct, GenericRow, KeyValueStore<Bytes, byte[]>> materialized =
        AggregateBuilderUtils.buildMaterialized(
            aggregate,
            aggregateSchema,
            aggregate.getInternalFormats(),
            queryBuilder,
            materializedFactory
        );
    final KTable<Struct, GenericRow> aggregated = groupedTable.getGroupedTable().aggregate(
        aggregateParams.getInitializer(),
        aggregateParams.getAggregator(),
        aggregateParams.getUndoAggregator().get(),
        materialized
    ).transformValues(
        () -> new KsTransformer<>(aggregateParams.<Struct>getAggregator().getResultMapper()),
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
        KeySerdeFactory.unwindowed(queryBuilder),
        materializationBuilder
    );
  }
}
