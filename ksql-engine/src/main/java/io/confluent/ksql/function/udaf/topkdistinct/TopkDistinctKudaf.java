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

import io.confluent.ksql.function.BaseAggregateFunction;
import io.confluent.ksql.function.KsqlAggregateFunction;
import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.util.KsqlException;

public class TopkDistinctKudaf<T extends Comparable<? super T>>
    extends BaseAggregateFunction<T, ArrayList<T>> {

  private final int tkVal;
  private final Class<T> ttClass;
  private final Schema outputSchema;

  @SuppressWarnings("unchecked")
  TopkDistinctKudaf(final String functionName,
                    final int argIndexInValue,
                    final int tkVal,
                    final Schema outputSchema,
                    final Class<T> ttClass) {
    super(functionName, argIndexInValue,
        () -> new ArrayList<T>(),
          SchemaBuilder.array(outputSchema).build(),
          Collections.singletonList(outputSchema)
    );

    this.tkVal = tkVal;
    this.ttClass = ttClass;
    this.outputSchema = outputSchema;
  }

  @Override
  public ArrayList<T> aggregate(final T currentVal, final ArrayList<T> currentAggValList) {
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
  public Merger<String, ArrayList<T>> getMerger() {
    return (aggKey, aggOneList, aggTwoList) -> {
      Set<T> set = new HashSet<>(aggOneList);
      set.addAll(aggTwoList);
      ArrayList<T> list = new ArrayList<>(set);
      Collections.sort(list);
      Collections.reverse(list);
      while (list.size() > tkVal) {
        list.remove(list.size() - 1);
      }
      return list;
    };
  }

  @Override
  public KsqlAggregateFunction<T, ArrayList<T>> getInstance(
      final Map<String, Integer> expressionNames,
      final List<Expression> functionArguments) {
    if (functionArguments.size() != 2) {
      throw new KsqlException(String.format("Invalid parameter count. Need 2 args, got %d arg(s)"
                                            + ".", functionArguments.size()));
    }

    final int udafIndex = expressionNames.get(functionArguments.get(0).toString());
    final int tkValFromArg = Integer.parseInt(functionArguments.get(1).toString());
    return new TopkDistinctKudaf<>(functionName, udafIndex, tkValFromArg, outputSchema, ttClass);
  }
}