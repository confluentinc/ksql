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

import io.confluent.ksql.GenericRow;
import io.confluent.ksql.SchemaNotSupportedException;
import io.confluent.ksql.execution.context.QueryContext;
import io.confluent.ksql.execution.plan.ExecutionKeyFactory;
import io.confluent.ksql.execution.plan.ExecutionStep;
import io.confluent.ksql.execution.plan.Formats;
import io.confluent.ksql.execution.runtime.MaterializedFactory;
import io.confluent.ksql.execution.runtime.RuntimeBuildContext;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.PhysicalSchema;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueStore;

/**
 * Helper methods for building {@link org.apache.kafka.streams.kstream.Materialized}
 * instances when performing stream -> table conversions, such as during aggregations.
 */
final class MaterializationUtil {

  private static final String MATERIALIZE_OP = "Materialize";

  private MaterializationUtil() {
  }

  static QueryContext materializeContext(final ExecutionStep<?> step) {
    return QueryContext.Stacker.of(step.getProperties().getQueryContext())
        .push(MATERIALIZE_OP)
        .getQueryContext();
  }

  static <K> Materialized<K, GenericRow, KeyValueStore<Bytes, byte[]>> buildMaterialized(
      final ExecutionStep<?> step,
      final LogicalSchema aggregateSchema,
      final Formats formats,
      final RuntimeBuildContext buildContext,
      final MaterializedFactory materializedFactory,
      final ExecutionKeyFactory<K> executionKeyFactory
  ) {
    final PhysicalSchema physicalAggregationSchema = PhysicalSchema.from(
        aggregateSchema,
        formats.getKeyFeatures(),
        formats.getValueFeatures()
    );

    final QueryContext queryContext = MaterializationUtil.materializeContext(step);

    final Serde<K> keySerde =
        buildKeySerde(formats, physicalAggregationSchema, queryContext, executionKeyFactory);

    final Serde<GenericRow> valueSerde =
        buildValueSerde(formats, buildContext, physicalAggregationSchema, queryContext);

    return materializedFactory
        .create(keySerde, valueSerde, StreamsUtil.buildOpName(queryContext));
  }

  private static <K> Serde<K> buildKeySerde(
      final Formats formats,
      final PhysicalSchema physicalAggregationSchema,
      final QueryContext queryContext,
      final ExecutionKeyFactory<K> executionKeyFactory
  ) {
    try {
      return executionKeyFactory.buildKeySerde(
          formats.getKeyFormat(),
          physicalAggregationSchema,
          queryContext
      );
    } catch (final SchemaNotSupportedException e) {
      throw schemaNotSupportedException(e, "key");
    }
  }

  private static Serde<GenericRow> buildValueSerde(
      final Formats formats,
      final RuntimeBuildContext buildContext,
      final PhysicalSchema physicalAggregationSchema,
      final QueryContext queryContext
  ) {
    try {
      return buildContext.buildValueSerde(
          formats.getValueFormat(),
          physicalAggregationSchema,
          queryContext
      );
    } catch (final SchemaNotSupportedException e) {
      throw schemaNotSupportedException(e, "value");
    }
  }

  private static SchemaNotSupportedException schemaNotSupportedException(
      final SchemaNotSupportedException e,
      final String type
  ) {
    return new SchemaNotSupportedException(
        "One of the functions used in the statement has an intermediate type that the "
            + type + " format can not handle. "
            + "Please remove the function or change the format."
            + System.lineSeparator()
            + "Consider up-voting https://github.com/confluentinc/ksql/issues/3950, "
            + "which will resolve this limitation",
        e
    );
  }
}
