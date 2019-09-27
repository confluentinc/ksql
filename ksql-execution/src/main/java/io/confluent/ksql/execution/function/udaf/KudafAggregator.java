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

import static java.util.Objects.requireNonNull;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.function.KsqlAggregateFunction;
import io.confluent.ksql.function.UdafAggregator;
import java.util.ArrayList;
import java.util.List;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.streams.kstream.Merger;
import org.apache.kafka.streams.kstream.ValueMapper;

public class KudafAggregator implements UdafAggregator {

  private final int initialUdafIndex;
  private final List<KsqlAggregateFunction<?, ?, ?>> aggregateFunctions;
  private final int columnCount;

  public KudafAggregator(
      final int initialUdafIndex,
      final List<KsqlAggregateFunction<?, ?, ?>> functions
  ) {
    this.initialUdafIndex = initialUdafIndex;
    this.aggregateFunctions = ImmutableList.copyOf(requireNonNull(functions, "functions"));
    this.columnCount = initialUdafIndex + aggregateFunctions.size();

    if (aggregateFunctions.isEmpty()) {
      throw new IllegalArgumentException("Aggregator needs aggregate functions");
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public GenericRow apply(final Struct k, final GenericRow rowValue, final GenericRow aggRowValue) {
    // copy over group-by and aggregate parameter columns into the output row
    for (int idx = 0; idx < initialUdafIndex; idx++) {
      aggRowValue.getColumns().set(idx, rowValue.getColumns().get(idx));
    }

    // compute the aggregation and write it into the output row. Its assumed that
    // the columns written by this statement do not overlap with those written by
    // the above statement.
    for (int idx = initialUdafIndex; idx < columnCount; idx++) {
      final KsqlAggregateFunction function = aggregateFunctionForColumn(idx);
      final Object currentValue = rowValue.getColumns().get(function.getArgIndexInValue());
      final Object currentAggregate = aggRowValue.getColumns().get(idx);
      final Object newAggregate = function.aggregate(currentValue, currentAggregate);
      aggRowValue.getColumns().set(idx, newAggregate);
    }

    return aggRowValue;
  }

  @SuppressWarnings("unchecked")
  public ValueMapper<GenericRow, GenericRow> getResultMapper() {

    return aggRow -> {
      final List<Object> columns = new ArrayList<>(columnCount);

      for (int idx = 0; idx < initialUdafIndex; idx++) {
        columns.add(idx, aggRow.getColumns().get(idx));
      }

      for (int idx = initialUdafIndex; idx < columnCount; idx++) {
        final KsqlAggregateFunction function = aggregateFunctionForColumn(idx);
        final Object agg = aggRow.getColumns().get(idx);
        final Object reduced = function.getResultMapper().apply(agg);
        columns.add(idx, reduced);
      }

      return new GenericRow(columns);
    };
  }

  @SuppressWarnings("unchecked")
  @Override
  public Merger<Struct, GenericRow> getMerger() {

    return (key, aggRowOne, aggRowTwo) -> {
      final List<Object> columns = new ArrayList<>(columnCount);

      for (int idx = 0; idx < initialUdafIndex; idx++) {
        if (aggRowOne.getColumns().get(idx) == null) {
          columns.add(idx, aggRowTwo.getColumns().get(idx));
        } else {
          columns.add(idx, aggRowOne.getColumns().get(idx));
        }
      }

      for (int idx = initialUdafIndex; idx < columnCount; idx++) {
        final KsqlAggregateFunction function = aggregateFunctionForColumn(idx);
        final Object aggOne = aggRowOne.getColumns().get(idx);
        final Object aggTwo = aggRowTwo.getColumns().get(idx);
        final Object merged = function.getMerger().apply(key, aggOne, aggTwo);
        columns.add(idx, merged);
      }

      return new GenericRow(columns);
    };
  }

  private KsqlAggregateFunction aggregateFunctionForColumn(final int columnIndex) {
    return aggregateFunctions.get(columnIndex - initialUdafIndex);
  }
}
