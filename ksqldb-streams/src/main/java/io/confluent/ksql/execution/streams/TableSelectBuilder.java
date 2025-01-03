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
import io.confluent.ksql.execution.context.QueryContext;
import io.confluent.ksql.execution.context.QueryContext.Stacker;
import io.confluent.ksql.execution.materialization.MaterializationInfo;
import io.confluent.ksql.execution.plan.Formats;
import io.confluent.ksql.execution.plan.KTableHolder;
import io.confluent.ksql.execution.plan.TableSelect;
import io.confluent.ksql.execution.runtime.MaterializedFactory;
import io.confluent.ksql.execution.runtime.RuntimeBuildContext;
import io.confluent.ksql.execution.streams.transform.KsValueTransformer;
import io.confluent.ksql.execution.transform.KsqlTransformer;
import io.confluent.ksql.execution.transform.select.SelectValueMapper;
import io.confluent.ksql.execution.transform.select.Selection;
import io.confluent.ksql.logging.processing.ProcessingLogger;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.PhysicalSchema;
import java.util.Optional;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.state.KeyValueStore;

public final class TableSelectBuilder {
  private static final String PROJECT_OP = "Last";

  private TableSelectBuilder() {
  }

  @SuppressWarnings("unchecked")
  public static <K> KTableHolder<K> build(
          final KTableHolder<K> table,
          final TableSelect<K> step,
          final RuntimeBuildContext buildContext,
          final Optional<Formats> formats,
          final MaterializedFactory materializedFactory
  ) {
    final LogicalSchema sourceSchema = table.getSchema();
    final QueryContext queryContext = step.getProperties().getQueryContext();

    final Selection<K> selection = Selection.of(
        sourceSchema,
        step.getKeyColumnNames(),
        Optional.empty(),
        step.getSelectExpressions(),
        buildContext.getKsqlConfig(),
        buildContext.getFunctionRegistry()
    );

    final SelectValueMapper<K> selectMapper = selection.getMapper();

    final ProcessingLogger logger = buildContext.getProcessingLogger(queryContext);

    final Named selectName = Named.as(StreamsUtil.buildOpName(queryContext));

    final Optional<MaterializationInfo.Builder> matBuilder =  table.getMaterializationBuilder();

    final boolean forceMaterialize = !matBuilder.isPresent();

    final Serde<K> keySerde;
    final Serde<GenericRow> valSerde;

    if (formats.isPresent()) {
      final Formats materializationFormat = formats.get();

      final PhysicalSchema physicalSchema = PhysicalSchema.from(
          selection.getSchema(),
          materializationFormat.getKeyFeatures(),
          materializationFormat.getValueFeatures()
      );

      keySerde = (Serde<K>) buildContext.buildKeySerde(
          materializationFormat.getKeyFormat(),
          physicalSchema,
          queryContext
      );

      valSerde = buildContext.buildValueSerde(
          materializationFormat.getValueFormat(),
          physicalSchema,
          queryContext
      );

      if (forceMaterialize) {
        final Stacker stacker = Stacker.of(step.getProperties().getQueryContext());

        final String stateStoreName = StreamsUtil.buildOpName(
            stacker.push(PROJECT_OP).getQueryContext());

        final Materialized<K, GenericRow, KeyValueStore<Bytes, byte[]>> materialized =
            materializedFactory.create(
                keySerde,
                valSerde,
                stateStoreName
            );

        final KTable<K, GenericRow> transFormedTable = table.getTable().transformValues(
            () -> new KsValueTransformer<>(selectMapper.getTransformer(logger)),
            materialized);

        return KTableHolder.materialized(
            transFormedTable,
            selection.getSchema(),
            table.getExecutionKeyFactory(),
            MaterializationInfo.builder(stateStoreName, selection.getSchema())
        );
      }
    } else {
      keySerde = null;
      valSerde = null;
    }

    final KTable<K, GenericRow> transFormedTable = table.getTable().transformValues(
        () -> new KsValueTransformer<>(selectMapper.getTransformer(logger)),
        materializedFactory.create(keySerde, valSerde),
        selectName
    );

    final Optional<MaterializationInfo.Builder> materialization = matBuilder.map(b -> b.map(
        pl -> (KsqlTransformer<Object, GenericRow>) selectMapper.getTransformer(pl),
            selection.getSchema(),
            queryContext)
    );

    return table
            .withTable(transFormedTable, selection.getSchema())
            .withMaterialization(materialization);
  }
}