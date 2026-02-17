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
import io.confluent.ksql.GenericKey;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.function.UdafAggregator;
import io.confluent.ksql.execution.transform.KsqlTransformer;
import io.confluent.ksql.function.KsqlAggregateFunction;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.kafka.streams.kstream.Merger;

public class KudafAggregator<K> implements UdafAggregator<K> {

  private final int nonAggColumnCount;
  private final List<KsqlAggregateFunction<?, ?, ?>> aggregateFunctions;
  private final int columnCount;

  public KudafAggregator(
      final int nonAggColumnCount,
      final List<KsqlAggregateFunction<?, ?, ?>> functions
  ) {
    this.nonAggColumnCount = nonAggColumnCount;
    this.aggregateFunctions = ImmutableList.copyOf(requireNonNull(functions, "functions"));
    this.columnCount = nonAggColumnCount + aggregateFunctions.size();

    if (aggregateFunctions.isEmpty()) {
      throw new IllegalArgumentException("Aggregator needs aggregate functions");
    }

    if (nonAggColumnCount < 0) {
      throw new IllegalArgumentException("negative nonAggColumnCount: " + nonAggColumnCount);
    }
  }

  @Override
  public GenericRow apply(final K k, final GenericRow rowValue, final GenericRow aggRowValue) {
    final GenericRow result = GenericRow.fromList(aggRowValue.values());

    // copy over group-by and aggregate parameter columns into the output row
    for (int idx = 0; idx < nonAggColumnCount; idx++) {
      result.set(idx, rowValue.get(idx));
    }

    // compute the aggregation and write it into the output row. Its assumed that
    // the columns written by this statement do not overlap with those written by
    // the above statement.
    for (int idx = nonAggColumnCount; idx < columnCount; idx++) {
      final KsqlAggregateFunction<Object, Object, Object> func = aggregateFunctionForColumn(idx);
      final Object currentValue = getCurrentValue(
              rowValue,
              func.getArgIndicesInValue(),
              func::convertToInput
      );
      final Object currentAggregate = result.get(idx);
      final Object newAggregate = func.aggregate(currentValue, currentAggregate);
      result.set(idx, newAggregate);
    }

    return result;
  }

  public KsqlTransformer<K, GenericRow> getResultMapper() {
    return new ResultTransformer();
  }

  @Override
  public Merger<GenericKey, GenericRow> getMerger() {

    return (key, aggRowOne, aggRowTwo) -> {

      final GenericRow output = new GenericRow(columnCount);

      for (int idx = 0; idx < nonAggColumnCount; idx++) {
        if (aggRowOne.get(idx) == null) {
          output.append(aggRowTwo.get(idx));
        } else {
          output.append(aggRowOne.get(idx));
        }
      }

      for (int idx = nonAggColumnCount; idx < columnCount; idx++) {
        final KsqlAggregateFunction<Object, Object, Object> func = aggregateFunctionForColumn(idx);
        final Object aggOne = aggRowOne.get(idx);
        final Object aggTwo = aggRowTwo.get(idx);
        final Object merged = func.getMerger().apply(key, aggOne, aggTwo);
        output.append(merged);
      }

      return output;
    };
  }

  @SuppressWarnings({"unchecked", "rawtypes"}) // Types have already been checked
  private KsqlAggregateFunction<Object, Object, Object> aggregateFunctionForColumn(
      final int columnIndex
  ) {
    return (KsqlAggregateFunction) aggregateFunctions.get(columnIndex - nonAggColumnCount);
  }

  private Object getCurrentValue(final GenericRow row, final List<Integer> indices,
                                 final Function<List<Object>, Object> inputConverter) {
    return inputConverter.apply(
            indices.stream()
                    .map(row::get)
                    .collect(Collectors.toList())
    );
  }

  private final class ResultTransformer implements KsqlTransformer<K, GenericRow> {

    @Override
    public GenericRow transform(
        final K readOnlyKey,
        final GenericRow value
    ) {
      if (value == null) {
        return null;
      }

      final GenericRow output = new GenericRow(columnCount);

      for (int idx = 0; idx < nonAggColumnCount; idx++) {
        output.append(value.get(idx));
      }

      for (int idx = nonAggColumnCount; idx < columnCount; idx++) {
        final KsqlAggregateFunction<Object, Object, Object> function =
            aggregateFunctionForColumn(idx);

        final Object agg = value.get(idx);
        final Object reduced = function.getResultMapper().apply(agg);
        output.append(reduced);
      }

      return output;
    }
  }
}
