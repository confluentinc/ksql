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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.streams.kstream.Merger;
import org.apache.kafka.streams.kstream.ValueMapper;

public class KudafAggregator implements UdafAggregator {

  private final int nonFuncColumnCount;
  private final Map<Integer, KsqlAggregateFunction> aggValToAggFunctionMap;
  private static final String UNINITIALIZED_COLUMN = "";

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
    for (Entry<Integer, KsqlAggregateFunction> entry: aggValToAggFunctionMap.entrySet()) {
      final int index = entry.getKey();
      final KsqlAggregateFunction ksqlAggregateFunction = entry.getValue();
      aggRowValue.getColumns().set(
            index,
            ksqlAggregateFunction.aggregate(
                rowValue.getColumns().get(ksqlAggregateFunction.getArgIndexInValue()),
                aggRowValue.getColumns().get(index)));
    }

    return aggRowValue;
  }

  @SuppressWarnings("unchecked")
  public ValueMapper<GenericRow, GenericRow> getResultMapper() {

    return (aggRow) -> {
      final int size = aggRow.getColumns().size();
      final List<Object> columns = new ArrayList<>(size);
      for (int i = 0; i < size; i++) {
        columns.add(UNINITIALIZED_COLUMN);
      }

      for (int idx = 0; idx < nonFuncColumnCount; idx++) {
        columns.set(idx, aggRow.getColumns().get(idx));
      }

      for (Entry<Integer, KsqlAggregateFunction> entry: aggValToAggFunctionMap.entrySet()) {
        final int index = entry.getKey();
        final KsqlAggregateFunction ksqlAggregateFunction = entry.getValue();
        columns.set(index,
                    ksqlAggregateFunction.getResultMapper().apply(aggRow.getColumns().get(index)));
      }

      return new GenericRow(columns);
    };
  }

  @SuppressWarnings("unchecked")
  @Override
  public Merger<Struct, GenericRow> getMerger() {

    return (key, aggRowOne, aggRowTwo) -> {
      final int size = aggRowOne.getColumns().size();
      final List<Object> columns = new ArrayList<>(size);
      for (int i = 0; i < size; i++) {
        columns.add(UNINITIALIZED_COLUMN);
      }

      for (int idx = 0; idx < nonFuncColumnCount; idx++) {
        if (aggRowOne.getColumns().get(idx) == null) {
          columns.set(idx, aggRowTwo.getColumns().get(idx));
        } else {
          columns.set(idx, aggRowOne.getColumns().get(idx));
        }
      }

      for (Entry<Integer, KsqlAggregateFunction> entry: aggValToAggFunctionMap.entrySet()) {
        final int index = entry.getKey();
        final KsqlAggregateFunction ksqlAggregateFunction = entry.getValue();
        columns.set(index, ksqlAggregateFunction.getMerger()
            .apply(key, aggRowOne.getColumns().get(index), aggRowTwo.getColumns().get(index)));
      }

      return new GenericRow(columns);
    };
  }
}
