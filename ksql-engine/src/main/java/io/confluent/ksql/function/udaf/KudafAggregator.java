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

package io.confluent.ksql.function.udaf;

import io.confluent.ksql.GenericRow;
import io.confluent.ksql.function.KsqlAggregateFunction;
import io.confluent.ksql.function.UdafAggregator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.streams.kstream.Merger;

public class KudafAggregator implements UdafAggregator {

  private final Map<Integer, KsqlAggregateFunction> aggValToAggFunctionMap;
  private final Map<Integer, Integer> aggValToValColumnMap;

  public KudafAggregator(
      final Map<Integer, KsqlAggregateFunction> aggValToAggFunctionMap,
      final Map<Integer, Integer> aggValToValColumnMap
  ) {
    this.aggValToAggFunctionMap = aggValToAggFunctionMap;
    this.aggValToValColumnMap = aggValToValColumnMap;
  }

  @SuppressWarnings("unchecked")
  @Override
  public GenericRow apply(final Struct k, final GenericRow rowValue, final GenericRow aggRowValue) {
    // copy over group-by and aggregate parameter columns into the output row
    aggValToValColumnMap.forEach(
        (destIdx, sourceIdx) ->
            aggRowValue.getColumns().set(destIdx, rowValue.getColumns().get(sourceIdx)));

    // compute the aggregation and write it into the output row. Its assumed that
    // the columns written by this statement do not overlap with those written by
    // the above statement.
    aggValToAggFunctionMap.forEach((key, value) ->
        aggRowValue.getColumns().set(
            key,
            value.aggregate(
                rowValue.getColumns().get(value.getArgIndexInValue()),
                aggRowValue.getColumns().get(key))));

    return aggRowValue;
  }

  @SuppressWarnings("unchecked")
  @Override
  public Merger<Struct, GenericRow> getMerger() {
    return (key, aggRowOne, aggRowTwo) -> {
      final List<Object> columns = Stream.generate(String::new).limit(aggRowOne.getColumns().size())
          .collect(Collectors.toList());

      aggValToValColumnMap.forEach((columnIndex, value) -> {
        if (aggRowOne.getColumns().get(value) == null) {
          columns.set(columnIndex, aggRowTwo.getColumns().get(value));
        } else {
          columns.set(columnIndex, aggRowOne.getColumns().get(value));
        }
      });

      aggValToAggFunctionMap.forEach((functionIndex, ksqlAggregateFunction) ->
          columns.set(functionIndex, ksqlAggregateFunction.getMerger()
          .apply(key,
              aggRowOne.getColumns().get(functionIndex),
              aggRowTwo.getColumns().get(functionIndex))));

      return new GenericRow(columns);
    };
  }

}
