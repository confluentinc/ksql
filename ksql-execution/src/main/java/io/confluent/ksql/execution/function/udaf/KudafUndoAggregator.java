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

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.function.TableAggregationFunction;
import java.util.List;
import java.util.Objects;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.streams.kstream.Aggregator;

public class KudafUndoAggregator implements Aggregator<Struct, GenericRow, GenericRow> {

  private final List<Integer> nonAggColumnIndexes;
  private final List<TableAggregationFunction<?, ?, ?>> aggregateFunctions;

  public KudafUndoAggregator(
      List<Integer> nonAggColumnIndexes,
      List<TableAggregationFunction<?, ?, ?>> aggregateFunctions
  ) {
    Objects.requireNonNull(aggregateFunctions, "aggregateFunctions");
    this.aggregateFunctions = ImmutableList.copyOf(aggregateFunctions);
    this.nonAggColumnIndexes = ImmutableList.copyOf(nonAggColumnIndexes);
  }

  @SuppressWarnings("unchecked")
  @Override
  public GenericRow apply(Struct k, GenericRow rowValue, GenericRow aggRowValue) {
    int idx = 0;
    for (; idx < nonAggColumnIndexes.size(); idx++) {
      final int idxInRow = nonAggColumnIndexes.get(idx);
      aggRowValue.getColumns().set(idx, rowValue.getColumns().get(idxInRow));
    }

    for (TableAggregationFunction function : aggregateFunctions) {
      Object argument = rowValue.getColumns().get(function.getArgIndexInValue());
      Object previous = aggRowValue.getColumns().get(idx);
      aggRowValue.getColumns().set(idx, function.undo(argument, previous));
      idx++;
    }

    return aggRowValue;
  }

  public List<Integer> getNonAggColumnIndexes() {
    return nonAggColumnIndexes;
  }

  public List<TableAggregationFunction<?, ?, ?>> getAggregateFunctions() {
    return aggregateFunctions;
  }
}
