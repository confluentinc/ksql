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

import com.google.common.annotations.VisibleForTesting;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.builder.KsqlQueryBuilder;
import io.confluent.ksql.execution.context.QueryContext;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.materialization.MaterializationInfo;
import io.confluent.ksql.execution.plan.KTableHolder;
import io.confluent.ksql.execution.plan.KeySerdeFactory;
import io.confluent.ksql.execution.plan.TableSelectKey;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.logging.processing.ProcessingLogger;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.util.KsqlConfig;
import java.util.function.BiFunction;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.state.KeyValueStore;

public final class TableSelectKeyBuilder {

  private TableSelectKeyBuilder() {
  }

  public static KTableHolder<Struct> build(
      final KTableHolder<?> table,
      final TableSelectKey selectKey,
      final KsqlQueryBuilder queryBuilder,
      final MaterializedFactory materializedFactory
  ) {
    return build(
        table,
        selectKey,
        queryBuilder,
        materializedFactory,
        PartitionByParamsFactory::build
    );
  }

  @VisibleForTesting
  static KTableHolder<Struct> build(
      final KTableHolder<?> table,
      final TableSelectKey selectKey,
      final KsqlQueryBuilder queryBuilder,
      final MaterializedFactory materializedFactory,
      final PartitionByParamsBuilder paramsBuilder
  ) {
    final LogicalSchema sourceSchema = table.getSchema();
    final QueryContext queryContext = selectKey.getProperties().getQueryContext();

    final ProcessingLogger logger = queryBuilder.getProcessingLogger(queryContext);

    final PartitionByParams params = paramsBuilder.build(
        sourceSchema,
        selectKey.getKeyExpression(),
        queryBuilder.getKsqlConfig(),
        queryBuilder.getFunctionRegistry(),
        logger
    );

    final BiFunction<Object, GenericRow, KeyValue<Struct, GenericRow>> mapper = params.getMapper();

    final KTable<?, GenericRow> kTable = table.getTable();

    final Materialized<Struct, GenericRow, KeyValueStore<Bytes, byte[]>> materialized =
        MaterializationUtil.buildMaterialized(
            selectKey,
            params.getSchema(),
            selectKey.getInternalFormats(),
            queryBuilder,
            materializedFactory
        );

    final KTable<Struct, GenericRow> reKeyed = kTable.toStream()
        .map(mapper::apply, Named.as(queryContext.formatContext() + "-SelectKey-Mapper"))
        .toTable(Named.as(queryContext.formatContext() + "-SelectKey"), materialized);

    final MaterializationInfo.Builder materializationBuilder = MaterializationInfo.builder(
        StreamsUtil.buildOpName(MaterializationUtil.materializeContext(selectKey)),
        params.getSchema()
    );

    return KTableHolder.materialized(
        reKeyed,
        params.getSchema(),
        KeySerdeFactory.unwindowed(queryBuilder),
        materializationBuilder
    );
  }

  interface PartitionByParamsBuilder {

    PartitionByParams build(
        LogicalSchema sourceSchema,
        Expression partitionBy,
        KsqlConfig ksqlConfig,
        FunctionRegistry functionRegistry,
        ProcessingLogger logger
    );
  }
}
