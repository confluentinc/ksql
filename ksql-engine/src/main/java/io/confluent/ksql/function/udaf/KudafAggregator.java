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

import com.google.common.collect.ImmutableMap;
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

  private final int nonFuncColumnCount;
  private final Map<Integer, KsqlAggregateFunction> aggValToAggFunctionMap;

  public KudafAggregator(
      final int nonFuncColumnCount,
      final Map<Integer, KsqlAggregateFunction> aggValToAggFunctionMap
  ) {
    this.nonFuncColumnCount = nonFuncColumnCount;
    this.aggValToAggFunctionMap = ImmutableMap.copyOf(aggValToAggFunctionMap);
  }

  @SuppressWarnings("unchecked")
  @Override
  public GenericRow apply(final Struct k, final GenericRow rowValue, final GenericRow aggRowValue) {
    // copy over group-by and aggregate parameter columns into the output row
    for (int idx = 0; idx < nonFuncColumnCount; idx++) {
      aggRowValue.getColumns().set(idx, rowValue.getColumns().get(idx));
    }

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

      for (int idx = 0; idx < nonFuncColumnCount; idx++) {
        if (aggRowOne.getColumns().get(idx) == null) {
          columns.set(idx, aggRowTwo.getColumns().get(idx));
        } else {
          columns.set(idx, aggRowOne.getColumns().get(idx));
        }
      }

      aggValToAggFunctionMap.forEach((functionIndex, ksqlAggregateFunction) ->
          columns.set(functionIndex, ksqlAggregateFunction.getMerger()
              .apply(key,
                  aggRowOne.getColumns().get(functionIndex),
                  aggRowTwo.getColumns().get(functionIndex))));

      return new GenericRow(columns);
    };
  }
}
