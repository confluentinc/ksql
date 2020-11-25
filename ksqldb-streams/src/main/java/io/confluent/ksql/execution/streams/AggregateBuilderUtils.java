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

import io.confluent.ksql.execution.context.QueryContext;
import io.confluent.ksql.execution.context.QueryContext.Stacker;
import io.confluent.ksql.execution.function.udaf.KudafAggregator;
import io.confluent.ksql.execution.materialization.MaterializationInfo;
import io.confluent.ksql.execution.plan.ExecutionStep;
import io.confluent.ksql.schema.ksql.LogicalSchema;

final class AggregateBuilderUtils {

  private static final String WINDOW_SELECT_OP = "WindowSelect";
  private static final String TO_OUTPUT_SCHEMA_OP = "ToOutputSchema";

  private AggregateBuilderUtils() {
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

  static MaterializationInfo.Builder materializationInfoBuilder(
      final KudafAggregator<Object> aggregator,
      final ExecutionStep<?> step,
      final LogicalSchema aggregationSchema,
      final LogicalSchema outputSchema
  ) {
    final QueryContext queryContext = MaterializationUtil.materializeContext(step);
    return MaterializationInfo.builder(StreamsUtil.buildOpName(queryContext), aggregationSchema)
        .map(pl -> aggregator.getResultMapper(), outputSchema, queryContext);
  }
}
