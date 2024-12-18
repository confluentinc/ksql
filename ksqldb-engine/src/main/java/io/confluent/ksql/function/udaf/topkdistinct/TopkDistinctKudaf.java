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

package io.confluent.ksql.function.udaf.topkdistinct;

import io.confluent.ksql.function.udaf.Udaf;
import io.confluent.ksql.function.udaf.UdafDescription;
import io.confluent.ksql.function.udaf.UdafFactory;
import io.confluent.ksql.schema.ksql.SqlArgument;
import io.confluent.ksql.schema.ksql.types.SqlArray;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.util.KsqlConstants;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;

@UdafDescription(
        name = "TOPKDISTINCT",
        description = "Computes the top k distinct values for a column, per key.",
        author = KsqlConstants.CONFLUENT_AUTHOR
)
public class TopkDistinctKudaf<T extends Comparable<? super T>>
    implements Udaf<T, List<T>, List<T>> {

  @SuppressWarnings("unused")
  @UdafFactory(description = "Calculates the top k distinct values for an integer column, per key.")
  public static Udaf<Integer, List<Integer>, List<Integer>> createTopKDistinctInt(final int k) {
    return new TopkDistinctKudaf<>(k);
  }

  @SuppressWarnings("unused")
  @UdafFactory(description = "Calculates the top k distinct values for a long column, per key.")
  public static Udaf<Long, List<Long>, List<Long>> createTopKDistinctLong(final int k) {
    return new TopkDistinctKudaf<>(k);
  }

  @SuppressWarnings("unused")
  @UdafFactory(description = "Calculates the top k distinct values for a double column, per key.")
  public static Udaf<Double, List<Double>, List<Double>> createTopKDistinctDouble(final int k) {
    return new TopkDistinctKudaf<>(k);
  }

  @SuppressWarnings("unused")
  @UdafFactory(description = "Calculates the top k distinct values for a string column, per key.")
  public static Udaf<String, List<String>, List<String>> createTopKDistinctString(final int k) {
    return new TopkDistinctKudaf<>(k);
  }

  @SuppressWarnings("unused")
  @UdafFactory(description = "Calculates the top k distinct values for a decimal column, per key.")
  public static Udaf<BigDecimal, List<BigDecimal>, List<BigDecimal>>
      createTopKDistinctDecimal(final int k) {
    return new TopkDistinctKudaf<>(k);
  }

  @SuppressWarnings("unused")
  @UdafFactory(description = "Calculates the top k distinct values for a date column, per key.")
  public static Udaf<Date, List<Date>, List<Date>> createTopKDistinctDate(final int k) {
    return new TopkDistinctKudaf<>(k);
  }

  @SuppressWarnings("unused")
  @UdafFactory(description = "Calculates the top k distinct values for a time column, per key.")
  public static Udaf<Time, List<Time>, List<Time>> createTopKDistinctTime(final int k) {
    return new TopkDistinctKudaf<>(k);
  }

  @SuppressWarnings("unused")
  @UdafFactory(
          description = "Calculates the top k distinct values for a timestamp column, per key."
  )
  public static Udaf<Timestamp, List<Timestamp>, List<Timestamp>>
      createTopKDistinctTimestamp(final int k) {
    return new TopkDistinctKudaf<>(k);
  }

  @SuppressWarnings("unused")
  @UdafFactory(description = "Calculates the top k distinct values for a bytes column, per key.")
  public static Udaf<ByteBuffer, List<ByteBuffer>, List<ByteBuffer>>
      createTopKDistinctBytes(final int k) {
    return new TopkDistinctKudaf<>(k);
  }

  private final int tkVal;
  private SqlType inputType;

  TopkDistinctKudaf(final int tkVal) {
    this.tkVal = tkVal;
  }

  @Override
  public void initializeTypeArguments(final List<SqlArgument> argTypeList) {
    inputType = argTypeList.get(0).getSqlTypeOrThrow();
  }

  @Override
  public Optional<SqlType> getAggregateSqlType() {
    return Optional.of(SqlArray.of(inputType));
  }

  @Override
  public Optional<SqlType> getReturnSqlType() {
    return Optional.of(SqlArray.of(inputType));
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
    if (currentSize == tkVal && currentValue.compareTo(aggregateValue.get(currentSize - 1)) <= 0) {
      return aggregateValue;
    }

    if (aggregateValue.contains(currentValue)) {
      return aggregateValue;
    }

    if (currentSize == tkVal) {
      aggregateValue.set(currentSize - 1, currentValue);
    } else {
      aggregateValue.add(currentValue);
    }

    aggregateValue.sort(Comparator.reverseOrder());
    return aggregateValue;
  }

  @Override
  public List<T> merge(final List<T> aggOne, final List<T> aggTwo) {
    final List<T> merged = new ArrayList<>(Math.min(tkVal, aggOne.size() + aggTwo.size()));

    int idx1 = 0;
    int idx2 = 0;
    for (int i = 0; i != tkVal; ++i) {
      final T v1 = getNextItem(aggOne, idx1);
      final T v2 = getNextItem(aggTwo, idx2);

      if (v1 == null && v2 == null) {
        break;
      }

      if (v1 != null && (v2 == null || v1.compareTo(v2) > 0)) {
        merged.add(v1);
        idx1++;
      } else if (v1 == null || v2.compareTo(v1) > 0) {
        merged.add(v2);
        idx2++;
      } else if (v1.compareTo(v2) == 0) {
        merged.add(v1);
        idx1++;
        idx2++;
      }
    }
    return merged;
  }

  @Override
  public List<T> map(final List<T> agg) {
    return agg;
  }

  private static <T> T getNextItem(final List<T> aggList, final int idx) {
    return idx < aggList.size() ? aggList.get(idx) : null;
  }

}