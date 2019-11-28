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
import io.confluent.ksql.execution.transform.KsqlValueTransformerWithKey;
import io.confluent.ksql.function.KsqlAggregateFunction;
import io.confluent.ksql.function.UdafAggregator;
import java.util.ArrayList;
import java.util.List;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.streams.kstream.Merger;
import org.apache.kafka.streams.kstream.ValueTransformerWithKey;

public class KudafAggregator<K> implements UdafAggregator<K> {

  private final int initialUdafIndex;
  private final List<KsqlAggregateFunction<?, ?, ?>> aggregateFunctions;
  private final int columnCount;

  public KudafAggregator(int initialUdafIndex, List<KsqlAggregateFunction<?, ?, ?>> functions) {
    this.initialUdafIndex = initialUdafIndex;
    this.aggregateFunctions = ImmutableList.copyOf(requireNonNull(functions, "functions"));
    this.columnCount = initialUdafIndex + aggregateFunctions.size();

    if (aggregateFunctions.isEmpty()) {
      throw new IllegalArgumentException("Aggregator needs aggregate functions");
    }
  }

  @Override
  public GenericRow apply(K k, GenericRow rowValue, GenericRow aggRowValue) {
    // copy over group-by and aggregate parameter columns into the output row
    for (int idx = 0; idx < initialUdafIndex; idx++) {
      aggRowValue.getColumns().set(idx, rowValue.getColumns().get(idx));
    }

    // compute the aggregation and write it into the output row. Its assumed that
    // the columns written by this statement do not overlap with those written by
    // the above statement.
    for (int idx = initialUdafIndex; idx < columnCount; idx++) {
      KsqlAggregateFunction<Object, Object, Object> function = aggregateFunctionForColumn(idx);
      Object currentValue = rowValue.getColumns().get(function.getArgIndexInValue());
      Object currentAggregate = aggRowValue.getColumns().get(idx);
      Object newAggregate = function.aggregate(currentValue, currentAggregate);
      aggRowValue.getColumns().set(idx, newAggregate);
    }

    return aggRowValue;
  }

  public ValueTransformerWithKey<K, GenericRow, GenericRow> getResultMapper() {
    return new ResultTransformer();
  }

  @Override
  public Merger<Struct, GenericRow> getMerger() {

    return (key, aggRowOne, aggRowTwo) -> {
      List<Object> columns = new ArrayList<>(columnCount);

      for (int idx = 0; idx < initialUdafIndex; idx++) {
        if (aggRowOne.getColumns().get(idx) == null) {
          columns.add(idx, aggRowTwo.getColumns().get(idx));
        } else {
          columns.add(idx, aggRowOne.getColumns().get(idx));
        }
      }

      for (int idx = initialUdafIndex; idx < columnCount; idx++) {
        KsqlAggregateFunction<Object, Object, Object> function = aggregateFunctionForColumn(idx);
        Object aggOne = aggRowOne.getColumns().get(idx);
        Object aggTwo = aggRowTwo.getColumns().get(idx);
        Object merged = function.getMerger().apply(key, aggOne, aggTwo);
        columns.add(idx, merged);
      }

      return new GenericRow(columns);
    };
  }

  @SuppressWarnings("unchecked") // Types have already been checked
  private KsqlAggregateFunction<Object, Object, Object> aggregateFunctionForColumn(
      final int columnIndex
  ) {
    return (KsqlAggregateFunction) aggregateFunctions.get(columnIndex - initialUdafIndex);
  }

  private final class ResultTransformer extends KsqlValueTransformerWithKey<K, GenericRow> {

    @Override
    protected GenericRow transform(final GenericRow aggRow) {
      if (aggRow == null) {
        return null;
      }

      final List<Object> columns = new ArrayList<>(columnCount);

      for (int idx = 0; idx < initialUdafIndex; idx++) {
        columns.add(idx, aggRow.getColumns().get(idx));
      }

      for (int idx = initialUdafIndex; idx < columnCount; idx++) {
        final KsqlAggregateFunction<Object, Object, Object> function =
            aggregateFunctionForColumn(idx);

        final Object agg = aggRow.getColumns().get(idx);
        final Object reduced = function.getResultMapper().apply(agg);
        columns.add(idx, reduced);
      }

      return new GenericRow(columns);
    }
  }
}
