/*
 * Copyright 2020 Confluent Inc.
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
import io.confluent.ksql.execution.plan.KTableHolder;
import io.confluent.ksql.execution.plan.KeySerdeFactory;
import io.confluent.ksql.execution.plan.TableSuppress;
import io.confluent.ksql.execution.streams.transform.KsTransformer;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.PhysicalSchema;
import io.confluent.ksql.serde.SerdeOption;
import io.confluent.ksql.util.KsqlConfig;
import java.util.Set;
import java.util.function.BiFunction;

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
      final KsqlQueryBuilder queryBuilder,
      final KeySerdeFactory keySerdeFactory
  ) {
    return build(
        table,
        step,
        queryBuilder,
        keySerdeFactory,
        PhysicalSchema::from,
        Materialized::with
    );
  }

  @VisibleForTesting
  @SuppressWarnings("unchecked")
  <K> KTableHolder<K> build(
      final KTableHolder<K> table,
      final TableSuppress<K> step,
      final KsqlQueryBuilder queryBuilder,
      final KeySerdeFactory keySerdeFactory,
      final BiFunction<LogicalSchema, Set<SerdeOption>, PhysicalSchema> physicalSchemaFactory,
      final BiFunction<Serde<K>, Serde<GenericRow>, Materialized> materializedFactory
  ) {
    final PhysicalSchema physicalSchema = physicalSchemaFactory.apply(
        table.getSchema(),
        step.getInternalFormats().getOptions()
    );
    final QueryContext queryContext = QueryContext.Stacker.of(
        step.getProperties().getQueryContext())
        .push(SUPPRESS_OP_NAME).getQueryContext();
    final Serde<K> keySerde = keySerdeFactory.buildKeySerde(
        step.getInternalFormats().getKeyFormat(),
        physicalSchema,
        queryContext
    );
    final Serde<GenericRow> valueSerde = queryBuilder.buildValueSerde(
        step.getInternalFormats().getValueFormat(),
        physicalSchema,
        queryContext
    );
    final Materialized<K, GenericRow, KeyValueStore<Bytes, byte[]>> materialized =
        materializedFactory.apply(
            keySerde,
            valueSerde
        );

    final Suppressed.StrictBufferConfig strictBufferConfig;
    final long maxBytes = queryBuilder.getKsqlConfig().getLong(
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
        (() -> new KsTransformer<>((k, v, ctx) -> v)),
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
}
