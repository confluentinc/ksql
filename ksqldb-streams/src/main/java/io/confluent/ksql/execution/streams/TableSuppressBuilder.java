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

import com.google.common.annotations.VisibleForTesting;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.context.QueryContext;
import io.confluent.ksql.execution.plan.ExecutionKeyFactory;
import io.confluent.ksql.execution.plan.KTableHolder;
import io.confluent.ksql.execution.plan.TableSuppress;
import io.confluent.ksql.execution.runtime.MaterializedFactory;
import io.confluent.ksql.execution.runtime.RuntimeBuildContext;
import io.confluent.ksql.execution.streams.transform.KsValueTransformer;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.PhysicalSchema;
import io.confluent.ksql.serde.SerdeFeatures;
import io.confluent.ksql.util.KsqlConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Suppressed;
import org.apache.kafka.streams.state.KeyValueStore;


public final class TableSuppressBuilder {

  private static final String SUPPRESS_OP_NAME = "Suppress";

  public TableSuppressBuilder() {
  }

  public <K> KTableHolder<K> build(
      final KTableHolder<K> table,
      final TableSuppress<K> step,
      final RuntimeBuildContext buildContext,
      final ExecutionKeyFactory<K> executionKeyFactory
  ) {
    return build(
        table,
        step,
        buildContext,
        executionKeyFactory,
        PhysicalSchema::from,
        buildContext.getMaterializedFactory()
    );
  }

  @VisibleForTesting
  @SuppressWarnings("unchecked")
  <K> KTableHolder<K> build(
      final KTableHolder<K> table,
      final TableSuppress<K> step,
      final RuntimeBuildContext buildContext,
      final ExecutionKeyFactory<K> executionKeyFactory,
      final PhysicalSchemaFactory physicalSchemaFactory,
      final MaterializedFactory materializedFactory
  ) {
    final PhysicalSchema physicalSchema = physicalSchemaFactory.create(
        table.getSchema(),
        step.getInternalFormats().getKeyFeatures(),
        step.getInternalFormats().getValueFeatures()
    );

    final QueryContext queryContext = QueryContext.Stacker.of(
        step.getProperties().getQueryContext())
        .push(SUPPRESS_OP_NAME).getQueryContext();
    final Serde<K> keySerde = executionKeyFactory.buildKeySerde(
        step.getInternalFormats().getKeyFormat(),
        physicalSchema,
        queryContext
    );
    final Serde<GenericRow> valueSerde = buildContext.buildValueSerde(
        step.getInternalFormats().getValueFormat(),
        physicalSchema,
        queryContext
    );
    final Materialized<K, GenericRow, KeyValueStore<Bytes, byte[]>> materialized =
        materializedFactory.create(
            keySerde,
            valueSerde
        );

    final Suppressed.StrictBufferConfig strictBufferConfig;
    final long maxBytes = buildContext.getKsqlConfig().getLong(
        KsqlConfig.KSQL_SUPPRESS_BUFFER_SIZE_BYTES);

    if (maxBytes < 0) {
      strictBufferConfig = Suppressed.BufferConfig.unbounded();
    } else {
      strictBufferConfig = Suppressed.BufferConfig
          .maxBytes(maxBytes)
          .shutDownWhenFull();
    }

    /* This is a dummy transformValues() call, we do this to ensure that the correct materialized
    with the correct key and val serdes is passed on when we call suppress
     */
    final KTable<K, GenericRow> suppressed = table.getTable().transformValues(
        (() -> new KsValueTransformer<>((k, v, ctx) -> v)),
        materialized
    ).suppress(
        (Suppressed<? super K>) Suppressed
            .untilWindowCloses(strictBufferConfig)
            .withName(SUPPRESS_OP_NAME)
    );

    return table
        .withTable(
            suppressed,
            table.getSchema()
        );
  }

  interface PhysicalSchemaFactory {

    PhysicalSchema create(
        LogicalSchema schema,
        SerdeFeatures keyFeatures,
        SerdeFeatures valueFeatures
    );
  }
}
