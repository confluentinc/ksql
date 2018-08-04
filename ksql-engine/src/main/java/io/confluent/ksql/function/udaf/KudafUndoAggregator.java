/**
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.confluent.ksql.function.udaf;

import io.confluent.ksql.GenericRow;
import io.confluent.ksql.function.TableAggregationFunction;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.apache.kafka.streams.kstream.Aggregator;

public class KudafUndoAggregator implements Aggregator<String, GenericRow, GenericRow> {
  private Map<Integer, TableAggregationFunction> aggValToAggFunctionMap;
  private Map<Integer, Integer> aggValToValColumnMap;

  public KudafUndoAggregator(
      Map<Integer, TableAggregationFunction> aggValToAggFunctionMap,
      Map<Integer, Integer> aggValToValColumnMap) {
    Objects.requireNonNull(aggValToAggFunctionMap);
    Objects.requireNonNull(aggValToValColumnMap);
    this.aggValToAggFunctionMap = Collections.unmodifiableMap(
        new HashMap<>(aggValToAggFunctionMap));
    this.aggValToValColumnMap = Collections.unmodifiableMap(new HashMap<>(aggValToValColumnMap));
  }

  @SuppressWarnings("unchecked")
  @Override
  public GenericRow apply(String s, GenericRow rowValue, GenericRow aggRowValue) {
    aggValToValColumnMap.forEach(
        (aggRowIndex, rowIndex) ->
            aggRowValue.getColumns().set(aggRowIndex, rowValue.getColumns().get(rowIndex)));

    aggValToAggFunctionMap.forEach(
        (aggRowIndex, function) ->
            aggRowValue.getColumns().set(
                aggRowIndex,
                function.undo(
                    rowValue.getColumns().get(function.getArgIndexInValue()),
                    aggRowValue.getColumns().get(aggRowIndex))));
    return aggRowValue;
  }
}
