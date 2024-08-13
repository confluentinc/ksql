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
import io.confluent.ksql.execution.context.QueryContext;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.materialization.MaterializationInfo;
import io.confluent.ksql.execution.plan.ExecutionKeyFactory;
import io.confluent.ksql.execution.plan.KTableHolder;
import io.confluent.ksql.execution.plan.TableSelectKey;
import io.confluent.ksql.execution.runtime.MaterializedFactory;
import io.confluent.ksql.execution.runtime.RuntimeBuildContext;
import io.confluent.ksql.execution.streams.PartitionByParams.Mapper;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.logging.processing.ProcessingLogger;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.util.KsqlConfig;
import java.util.List;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.state.KeyValueStore;

public final class TableSelectKeyBuilder {

  private TableSelectKeyBuilder() {
  }

  public static <K> KTableHolder<K> build(
      final KTableHolder<K> table,
      final TableSelectKey<K> selectKey,
      final RuntimeBuildContext buildContext,
      final MaterializedFactory materializedFactory
  ) {
    return build(
        table,
        selectKey,
        buildContext,
        materializedFactory,
        PartitionByParamsFactory::build
    );
  }

  @VisibleForTesting
  static <K> KTableHolder<K> build(
      final KTableHolder<K> table,
      final TableSelectKey<K> selectKey,
      final RuntimeBuildContext buildContext,
      final MaterializedFactory materializedFactory,
      final PartitionByParamsBuilder paramsBuilder
  ) {
    final LogicalSchema sourceSchema = table.getSchema();
    final QueryContext queryContext = selectKey.getProperties().getQueryContext();

    final ProcessingLogger logger = buildContext.getProcessingLogger(queryContext);

    final PartitionByParams<K> params = paramsBuilder.build(
        sourceSchema,
        table.getExecutionKeyFactory(),
        selectKey.getKeyExpressions(),
        buildContext.getKsqlConfig(),
        buildContext.getFunctionRegistry(),
        logger
    );

    final Mapper<K> mapper = params.getMapper();

    final KTable<K, GenericRow> kTable = table.getTable();

    final Materialized<K, GenericRow, KeyValueStore<Bytes, byte[]>> materialized =
        MaterializationUtil.buildMaterialized(
            selectKey,
            params.getSchema(),
            selectKey.getInternalFormats(),
            buildContext,
            materializedFactory,
            table.getExecutionKeyFactory()
        );

    final KTable<K, GenericRow> reKeyed = kTable.toStream()
        .map(mapper, Named.as(queryContext.formatContext() + "-SelectKey-Mapper"))
        .toTable(Named.as(queryContext.formatContext() + "-SelectKey"), materialized);

    final MaterializationInfo.Builder materializationBuilder = MaterializationInfo.builder(
        StreamsUtil.buildOpName(MaterializationUtil.materializeContext(selectKey)),
        params.getSchema()
    );

    return KTableHolder.materialized(
        reKeyed,
        params.getSchema(),
        table.getExecutionKeyFactory().withQueryBuilder(buildContext),
        materializationBuilder
    );
  }

  interface PartitionByParamsBuilder {

    <K> PartitionByParams<K> build(
        LogicalSchema sourceSchema,
        ExecutionKeyFactory<K> executionKeyFactory,
        List<Expression> partitionBy,
        KsqlConfig ksqlConfig,
        FunctionRegistry functionRegistry,
        ProcessingLogger logger
    );
  }
}
