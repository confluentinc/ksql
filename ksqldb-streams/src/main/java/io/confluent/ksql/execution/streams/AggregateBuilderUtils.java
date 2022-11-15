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
import io.confluent.ksql.SchemaNotSupportedException;
import io.confluent.ksql.execution.builder.KsqlQueryBuilder;
import io.confluent.ksql.execution.context.QueryContext;
import io.confluent.ksql.execution.context.QueryContext.Stacker;
import io.confluent.ksql.execution.function.udaf.KudafAggregator;
import io.confluent.ksql.execution.materialization.MaterializationInfo;
import io.confluent.ksql.execution.plan.ExecutionStep;
import io.confluent.ksql.execution.plan.Formats;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.PhysicalSchema;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueStore;

final class AggregateBuilderUtils {

  private static final String MATERIALIZE_OP = "Materialize";
  private static final String WINDOW_SELECT_OP = "WindowSelect";
  private static final String TO_OUTPUT_SCHEMA_OP = "ToOutputSchema";

  private AggregateBuilderUtils() {
  }

  static QueryContext materializeContext(final ExecutionStep<?> step) {
    return Stacker.of(step.getProperties().getQueryContext())
        .push(MATERIALIZE_OP)
        .getQueryContext();
  }

  static QueryContext windowSelectContext(final ExecutionStep<?> step) {
    return Stacker.of(step.getProperties().getQueryContext())
        .push(WINDOW_SELECT_OP)
        .getQueryContext();
  }

  static QueryContext outputContext(final ExecutionStep<?> step) {
    return Stacker.of(step.getProperties().getQueryContext())
        .push(TO_OUTPUT_SCHEMA_OP)
        .getQueryContext();
  }

  static Materialized<Struct, GenericRow, KeyValueStore<Bytes, byte[]>> buildMaterialized(
      final ExecutionStep<?> step,
      final LogicalSchema aggregateSchema,
      final Formats formats,
      final KsqlQueryBuilder queryBuilder,
      final MaterializedFactory materializedFactory
  ) {
    final PhysicalSchema physicalAggregationSchema = PhysicalSchema.from(
        aggregateSchema,
        formats.getKeyFeatures(),
        formats.getValueFeatures()
    );

    final QueryContext queryContext = materializeContext(step);

    final Serde<Struct> keySerde =
        buildKeySerde(formats, queryBuilder, physicalAggregationSchema, queryContext);

    final Serde<GenericRow> valueSerde =
        buildValueSerde(formats, queryBuilder, physicalAggregationSchema, queryContext);

    return materializedFactory
        .create(keySerde, valueSerde, StreamsUtil.buildOpName(queryContext));
  }

  static MaterializationInfo.Builder materializationInfoBuilder(
      final KudafAggregator<Object> aggregator,
      final ExecutionStep<?> step,
      final LogicalSchema aggregationSchema,
      final LogicalSchema outputSchema
  ) {
    final QueryContext queryContext = materializeContext(step);
    return MaterializationInfo.builder(StreamsUtil.buildOpName(queryContext), aggregationSchema)
        .map(pl -> aggregator.getResultMapper(), outputSchema, queryContext);
  }

  private static Serde<Struct> buildKeySerde(
      final Formats formats,
      final KsqlQueryBuilder queryBuilder,
      final PhysicalSchema physicalAggregationSchema,
      final QueryContext queryContext
  ) {
    try {
      return queryBuilder.buildKeySerde(
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
      final KsqlQueryBuilder queryBuilder,
      final PhysicalSchema physicalAggregationSchema,
      final QueryContext queryContext
  ) {
    try {
      return queryBuilder.buildValueSerde(
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
