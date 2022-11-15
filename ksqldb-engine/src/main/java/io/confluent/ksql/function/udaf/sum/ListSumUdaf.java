/*
 * Copyright 2019 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"; you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.function.udaf.sum;

import io.confluent.ksql.function.udaf.TableUdaf;
import io.confluent.ksql.function.udaf.UdafDescription;
import io.confluent.ksql.function.udaf.UdafFactory;
import io.confluent.ksql.util.KsqlConstants;
import java.util.List;
import java.util.function.BinaryOperator;

@UdafDescription(name = "sum_list",
    description = "Returns the sum of elements contained in the list.",
    author = KsqlConstants.CONFLUENT_AUTHOR
)
public final class ListSumUdaf {

  private ListSumUdaf() {
  }

  @UdafFactory(description = "sum double values in a list into a single double")
  public static TableUdaf<List<Double>, Double, Double> sumDoubleList() {
    return new TableUdaf<List<Double>, Double, Double>() {

      @Override
      public Double initialize() {
        return 0.0;
      }

      @Override
      public Double aggregate(final List<Double> valueToAdd, final Double aggregateValue) {
        if (valueToAdd == null) {
          return aggregateValue;
        }
        return aggregateValue + sumList(valueToAdd);
      }

      @Override
      public Double merge(final Double aggOne, final Double aggTwo) {
        return aggOne + aggTwo;
      }

      @Override
      public Double map(final Double agg) {
        return agg;
      }

      @Override
      public Double undo(final List<Double> valueToUndo, final Double aggregateValue) {
        if (valueToUndo == null) {
          return aggregateValue;
        }
        return aggregateValue - sumList(valueToUndo);
      }

      private double sumList(final List<Double> list) {
        return sum(list, initialize(), Double::sum);
      }
    };
  }

  @UdafFactory(description = "sum int values in a list into a single int")
  public static TableUdaf<List<Integer>, Integer, Integer> sumIntList() {
    return new TableUdaf<List<Integer>, Integer, Integer>() {

      @Override
      public Integer initialize() {
        return 0;
      }

      @Override
      public Integer aggregate(final List<Integer> valueToAdd, final Integer aggregateValue) {
        if (valueToAdd == null) {
          return aggregateValue;
        }
        return aggregateValue + sumList(valueToAdd);
      }

      @Override
      public Integer merge(final Integer aggOne, final Integer aggTwo) {
        return aggOne + aggTwo;
      }

      @Override
      public Integer map(final Integer agg) {
        return agg;
      }

      @Override
      public Integer undo(final List<Integer> valueToUndo, final Integer aggregateValue) {
        if (valueToUndo == null) {
          return aggregateValue;
        }
        return aggregateValue - sumList(valueToUndo);
      }

      private int sumList(final List<Integer> list) {
        return sum(list, initialize(), Integer::sum);
      }
    };
  }

  @UdafFactory(description = "sum long values in a list into a single long")
  public static TableUdaf<List<Long>, Long, Long> sumLongList() {
    return new TableUdaf<List<Long>, Long, Long>() {

      @Override
      public Long initialize() {
        return 0L;
      }

      @Override
      public Long aggregate(final List<Long> valueToAdd, final Long aggregateValue) {
        if (valueToAdd == null) {
          return aggregateValue;
        }
        return aggregateValue + sumList(valueToAdd);
      }

      @Override
      public Long merge(final Long aggOne, final Long aggTwo) {
        return aggOne + aggTwo;
      }

      @Override
      public Long map(final Long agg) {
        return agg;
      }

      @Override
      public Long undo(final List<Long> valueToUndo, final Long aggregateValue) {
        if (valueToUndo == null) {
          return aggregateValue;
        }
        return aggregateValue - sumList(valueToUndo);
      }

      private long sumList(final List<Long> list) {
        return sum(list, initialize(), Long::sum);
      }
    };
  }

  private static <T> T sum(
      final Iterable<T> list,
      final T initial,
      final BinaryOperator<T> summer) {

    T sum = initial;
    for (final T v: list) {
      if (v == null) {
        continue;
      }
      sum = summer.apply(sum, v);
    }
    return sum;
  }
}
