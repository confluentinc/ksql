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

import static java.util.Objects.requireNonNull;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.function.KsqlAggregateFunction;
import io.confluent.ksql.function.UdafAggregator;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.streams.kstream.Merger;
import org.apache.kafka.streams.kstream.ValueMapper;

public class KudafAggregator implements UdafAggregator {

  private final int nonFuncColumnCount;
  private final List<KsqlAggregateFunction> aggregateFunctions;
  private final int columnCount;

  public KudafAggregator(
      final int nonFuncColumnCount,
      final Map<Integer, KsqlAggregateFunction> aggValToAggFunctionMap
  ) {
    this.nonFuncColumnCount = nonFuncColumnCount;
    this.aggregateFunctions = validateAggregates(
        nonFuncColumnCount,
        requireNonNull(aggValToAggFunctionMap, "aggValToAggFunctionMap")
    );
    this.columnCount = nonFuncColumnCount + aggregateFunctions.size();
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
    for (int idx = nonFuncColumnCount; idx < columnCount; idx++) {
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

      for (int idx = 0; idx < nonFuncColumnCount; idx++) {
        columns.add(idx, aggRow.getColumns().get(idx));
      }

      for (int idx = nonFuncColumnCount; idx < columnCount; idx++) {
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

      for (int idx = 0; idx < nonFuncColumnCount; idx++) {
        if (aggRowOne.getColumns().get(idx) == null) {
          columns.add(idx, aggRowTwo.getColumns().get(idx));
        } else {
          columns.add(idx, aggRowOne.getColumns().get(idx));
        }
      }

      for (int idx = nonFuncColumnCount; idx < columnCount; idx++) {
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
    return aggregateFunctions.get(columnIndex - nonFuncColumnCount);
  }

  private static List<KsqlAggregateFunction> validateAggregates(
      final int nonFuncColumnCount,
      final Map<Integer, KsqlAggregateFunction> aggValToAggFunctionMap
  ) {
    final List<Integer> indexes = aggValToAggFunctionMap.keySet().stream()
        .sorted()
        .collect(Collectors.toList());

    if (indexes.isEmpty()) {
      throw new IllegalArgumentException("Aggregator needs aggregate functions");
    }

    Integer last = nonFuncColumnCount - 1;
    for (final Integer idx : indexes) {
      if (idx != (last + 1)) {
        throw new IllegalArgumentException("Non-sequential aggregate indexes");
      }

      last = idx;
    }

    final Builder<KsqlAggregateFunction> builder = ImmutableList.builder();

    final int total = nonFuncColumnCount + aggValToAggFunctionMap.size();
    for (int idx = nonFuncColumnCount; idx < total; idx++) {
      builder.add(aggValToAggFunctionMap.get(idx));
    }
    return builder.build();
  }
}
