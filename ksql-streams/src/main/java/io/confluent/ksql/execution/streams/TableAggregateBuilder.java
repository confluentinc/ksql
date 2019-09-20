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
import io.confluent.ksql.execution.plan.KTableHolder;
import io.confluent.ksql.execution.plan.KeySerdeFactory;
import io.confluent.ksql.execution.plan.TableAggregate;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.streams.kstream.KGroupedTable;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueStore;

public final class TableAggregateBuilder {
  private TableAggregateBuilder() {
  }

  public static KTableHolder<Struct> build(
      final KGroupedTable<Struct, GenericRow> kgroupedTable,
      final TableAggregate aggregate,
      final KsqlQueryBuilder queryBuilder,
      final MaterializedFactory materializedFactory) {
    return build(
        kgroupedTable,
        aggregate,
        queryBuilder,
        materializedFactory,
        AggregateParams::new
    );
  }

  public static KTableHolder<Struct> build(
      final KGroupedTable<Struct, GenericRow> kgroupedTable,
      final TableAggregate aggregate,
      final KsqlQueryBuilder queryBuilder,
      final MaterializedFactory materializedFactory,
      final AggregateParams.Factory aggregateParamsFactory) {
    final LogicalSchema sourceSchema = aggregate.getSources().get(0).getSchema();
    final int nonFuncColumns = aggregate.getNonFuncColumnCount();
    final AggregateParams aggregateParams = aggregateParamsFactory.create(
        sourceSchema,
        nonFuncColumns,
        queryBuilder.getFunctionRegistry(),
        aggregate.getAggregations()
    );
    final Materialized<Struct, GenericRow, KeyValueStore<Bytes, byte[]>> materialized =
        AggregateBuilderUtils.buildMaterialized(
            aggregate.getProperties().getQueryContext(),
            aggregate.getAggregationSchema(),
            aggregate.getFormats(),
            queryBuilder,
            materializedFactory
        );
    final KTable<Struct, GenericRow> aggregated = kgroupedTable.aggregate(
        aggregateParams.getInitializer(),
        aggregateParams.getAggregator(),
        aggregateParams.getUndoAggregator(),
        materialized
    ).mapValues(aggregateParams.getAggregator().getResultMapper());
    return new KTableHolder<>(aggregated, KeySerdeFactory.unwindowed(queryBuilder));
  }
}