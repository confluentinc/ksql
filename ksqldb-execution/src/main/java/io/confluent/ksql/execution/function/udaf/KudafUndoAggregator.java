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
import io.confluent.ksql.execution.function.TableAggregationFunction;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.kafka.streams.kstream.Aggregator;

public class KudafUndoAggregator implements Aggregator<GenericKey, GenericRow, GenericRow> {

  private final int nonAggColumnCount;
  private final List<TableAggregationFunction<?, ?, ?>> aggregateFunctions;
  private final int columnCount;

  public KudafUndoAggregator(
      final int nonAggColumnCount,
      final List<TableAggregationFunction<?, ?, ?>> aggregateFunctions
  ) {
    this.nonAggColumnCount = nonAggColumnCount;
    this.aggregateFunctions = ImmutableList
        .copyOf(requireNonNull(aggregateFunctions, "aggregateFunctions"));
    this.columnCount = nonAggColumnCount + aggregateFunctions.size();

    if (aggregateFunctions.isEmpty()) {
      throw new IllegalArgumentException("Aggregator needs aggregate functions");
    }

    if (nonAggColumnCount < 0) {
      throw new IllegalArgumentException("negative nonAggColumnCount: " + nonAggColumnCount);
    }
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  @Override
  public GenericRow apply(
      final GenericKey k,
      final GenericRow rowValue,
      final GenericRow aggRowValue
  ) {
    final GenericRow result = GenericRow.fromList(aggRowValue.values());

    for (int idx = 0; idx < nonAggColumnCount; idx++) {
      result.set(idx, rowValue.get(idx));
    }

    for (int idx = nonAggColumnCount; idx < columnCount; idx++) {
      final TableAggregationFunction function = aggregateFunctions.get(idx - nonAggColumnCount);
      final Object argument = getCurrentValue(
              rowValue,
              function.getArgIndicesInValue(),
              function::convertToInput
      );
      final Object previous = result.get(idx);
      result.set(idx, function.undo(argument, previous));
    }

    return result;
  }

  private Object getCurrentValue(final GenericRow row, final List<Integer> indices,
                                 final Function<List<Object>, Object> inputConverter) {
    return inputConverter.apply(
            indices.stream()
                    .map(row::get)
                    .collect(Collectors.toList())
    );
  }
}
