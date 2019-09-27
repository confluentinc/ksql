/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.ksql.execution.function.udaf;

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.function.TableAggregationFunction;
import java.util.Map;
import java.util.Objects;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.streams.kstream.Aggregator;

public class KudafUndoAggregator implements Aggregator<Struct, GenericRow, GenericRow> {

  private final int nonFuncColumnCount;
  private final Map<Integer, TableAggregationFunction> aggValToAggFunctionMap;

  public KudafUndoAggregator(
      final int nonFuncColumnCount,
      final Map<Integer, TableAggregationFunction<?, ?, ?>> aggValToAggFunctionMap
  ) {
    Objects.requireNonNull(aggValToAggFunctionMap);
    this.aggValToAggFunctionMap = ImmutableMap.copyOf(aggValToAggFunctionMap);
    this.nonFuncColumnCount = nonFuncColumnCount;
  }

  @SuppressWarnings("unchecked")
  @Override
  public GenericRow apply(final Struct k, final GenericRow rowValue, final GenericRow aggRowValue) {
    for (int idx = 0; idx < nonFuncColumnCount; idx++) {
      aggRowValue.getColumns().set(idx, rowValue.getColumns().get(idx));
    }

    aggValToAggFunctionMap.forEach(
        (aggRowIndex, function) ->
            aggRowValue.getColumns().set(
                aggRowIndex,
                function.undo(
                    rowValue.getColumns().get(function.getArgIndexInValue()),
                    aggRowValue.getColumns().get(aggRowIndex))));
    return aggRowValue;
  }

  public int getNonFuncColumnCount() {
    return nonFuncColumnCount;
  }

  public Map<Integer, TableAggregationFunction> getAggValToAggFunctionMap() {
    return aggValToAggFunctionMap;
  }
}
