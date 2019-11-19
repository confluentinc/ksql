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

package io.confluent.ksql.execution.materialization;

import static java.util.Objects.requireNonNull;

import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.execution.expression.tree.FunctionCall;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import java.util.List;


@Immutable
public final class AggregatesInfo {

  private final int startingColumnIndex;
  private final List<FunctionCall> aggregateFunctions;
  private final LogicalSchema schema;

  /**
   * @param startingColumnIndex column index of first aggregate function.
   * @param aggregateFunctions  the map of column index to aggregate function.
   * @param schema              the schema required by the aggregators.
   * @return the immutable instance.
   */
  public static AggregatesInfo of(
      int startingColumnIndex, List<FunctionCall> aggregateFunctions, LogicalSchema schema
  ) {
    return new AggregatesInfo(startingColumnIndex, aggregateFunctions, schema);
  }

  private AggregatesInfo(
      int startingColumnIndex, List<FunctionCall> aggregateFunctions, LogicalSchema prepareSchema
  ) {
    this.startingColumnIndex = startingColumnIndex;
    this.aggregateFunctions = ImmutableList
        .copyOf(requireNonNull(aggregateFunctions, "aggregateFunctions"));
    this.schema = requireNonNull(prepareSchema, "prepareSchema");
  }

  public int startingColumnIndex() {
    return startingColumnIndex;
  }

  public List<FunctionCall> aggregateFunctions() {
    return aggregateFunctions;
  }

  int valueColumnCount() {
    return startingColumnIndex + aggregateFunctions.size();
  }

  public LogicalSchema schema() {
    return schema;
  }
}
