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

package io.confluent.ksql.function.udaf.topk;

import io.confluent.ksql.function.udaf.Udaf;
import io.confluent.ksql.function.udaf.UdafDescription;
import io.confluent.ksql.function.udaf.UdafFactory;
import io.confluent.ksql.schema.ksql.SqlArgument;
import io.confluent.ksql.schema.ksql.types.SqlArray;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.util.KsqlConstants;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;

@UdafDescription(
        name = "TOPK",
        description = "Computes the top k values for a column, per key.",
        author = KsqlConstants.CONFLUENT_AUTHOR
)
public class TopkKudaf<T extends Comparable<? super T>> implements Udaf<T, List<T>, List<T>> {

  @UdafFactory(description = "Calculates the top k values for an integer column, per key.")
  public static Udaf<Integer, List<Integer>, List<Integer>> createTopKInt(final int k) {
    return new TopkKudaf<>(k);
  }

  @UdafFactory(description = "Calculates the top k values for a long column, per key.")
  public static Udaf<Long, List<Long>, List<Long>> createTopKLong(final int k) {
    return new TopkKudaf<>(k);
  }

  @UdafFactory(description = "Calculates the top k values for a double column, per key.")
  public static Udaf<Double, List<Double>, List<Double>> createTopKDouble(final int k) {
    return new TopkKudaf<>(k);
  }

  @UdafFactory(description = "Calculates the top k values for a string column, per key.")
  public static Udaf<String, List<String>, List<String>> createTopKString(final int k) {
    return new TopkKudaf<>(k);
  }
  
  private final int topKSize;
  private SqlType inputSchema;

  TopkKudaf(final int topKSize) {
    this.topKSize = topKSize;
  }

  @Override
  public void initializeTypeArguments(final List<SqlArgument> argTypeList) {
    inputSchema = argTypeList.get(0).getSqlTypeOrThrow();
  }

  @Override
  public Optional<SqlType> getAggregateSqlType() {
    return Optional.of(SqlArray.of(inputSchema));
  }

  @Override
  public Optional<SqlType> getReturnSqlType() {
    return Optional.of(SqlArray.of(inputSchema));
  }

  @Override
  public List<T> initialize() {
    return new ArrayList<>();
  }

  @Override
  public List<T> aggregate(final T currentValue, final List<T> aggregateValue) {
    if (currentValue == null) {
      return aggregateValue;
    }

    final int currentSize = aggregateValue.size();
    if (!aggregateValue.isEmpty()) {
      final T last = aggregateValue.get(currentSize - 1);
      if (currentValue.compareTo(last) <= 0
          && currentSize == topKSize) {
        return aggregateValue;
      }
    }

    if (currentSize == topKSize) {
      aggregateValue.set(currentSize - 1, currentValue);
    } else {
      aggregateValue.add(currentValue);
    }

    aggregateValue.sort(Comparator.reverseOrder());
    return aggregateValue;
  }

  @Override
  public List<T> merge(final List<T> aggOne, final List<T> aggTwo) {
    final List<T> merged = new ArrayList<>(
            Math.min(topKSize, aggOne.size() + aggTwo.size()));

    int idx1 = 0;
    int idx2 = 0;
    for (int i = 0; i != topKSize; ++i) {
      final T v1 = idx1 < aggOne.size() ? aggOne.get(idx1) : null;
      final T v2 = idx2 < aggTwo.size() ? aggTwo.get(idx2) : null;

      if (v1 != null && (v2 == null || v1.compareTo(v2) >= 0)) {
        merged.add(v1);
        idx1++;
      } else if (v2 != null && (v1 == null || v1.compareTo(v2) < 0)) {
        merged.add(v2);
        idx2++;
      } else {
        break;
      }
    }

    return merged;
  }

  @Override
  public List<T> map(final List<T> agg) {
    return agg;
  }

}