/*
 * Copyright 2017 Confluent Inc.
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

package io.confluent.ksql.function.udaf.topkdistinct;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.streams.kstream.Merger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import io.confluent.ksql.function.AggregateFunctionArguments;
import io.confluent.ksql.function.BaseAggregateFunction;
import io.confluent.ksql.function.KsqlAggregateFunction;
import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.util.KsqlException;

public class TopkDistinctKudaf<T extends Comparable<? super T>>
    extends BaseAggregateFunction<T, List<T>> {

  private final int tkVal;
  private final Class<T> ttClass;
  private final Schema outputSchema;


  @SuppressWarnings("unchecked")
  TopkDistinctKudaf(final String functionName,
                    final int argIndexInValue,
                    final int tkVal,
                    final Schema outputSchema,
                    final Class<T> ttClass) {
    super(
        functionName, argIndexInValue,
        (() -> new ArrayList<T>()),
        SchemaBuilder.array(outputSchema).build(),
        Collections.singletonList(outputSchema)
    );

    this.tkVal = tkVal;
    this.ttClass = ttClass;
    this.outputSchema = outputSchema;
  }

  @Override
  public List<T> aggregate(final T currentVal, final List<T> currentAggValList) {
    if (currentVal == null) {
      return currentAggValList;
    }

    Set<T> set = new HashSet<>(currentAggValList);
    set.add(currentVal);
    ArrayList list = new ArrayList(set);
    Collections.sort(list);
    Collections.reverse(list);
    if (list.size() > tkVal) {
      list.remove(list.size() - 1);
    }
    return list;
  }

  @SuppressWarnings("unchecked")
  @Override
  public Merger<String, List<T>> getMerger() {
    return (aggKey, aggOneList, aggTwoList) -> {
      Set<T> set = new HashSet<>(aggOneList);
      set.addAll(aggTwoList);
      List<T> list = new ArrayList<>(set);
      Collections.sort(list);
      Collections.reverse(list);
      while (list.size() > tkVal) {
        list.remove(list.size() - 1);
      }
      return list;
    };
  }

  @Override
  public KsqlAggregateFunction<T, List<T>> getInstance(
      final AggregateFunctionArguments aggregateFunctionArguments) {
    aggregateFunctionArguments.ensureArgCount(2, "TopkDistinct");
    final int udafIndex = aggregateFunctionArguments.udafIndex();
    final int tkValFromArg = Integer.parseInt(aggregateFunctionArguments.arg(1));
    return new TopkDistinctKudaf<>(functionName, udafIndex, tkValFromArg, outputSchema, ttClass);
  }
}