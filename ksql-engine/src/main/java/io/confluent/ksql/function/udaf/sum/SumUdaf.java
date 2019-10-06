/*
 * Copyright 2019 Confluent Inc.
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

package io.confluent.ksql.function.udaf.sum;

import io.confluent.ksql.function.udaf.TableUdaf;
import io.confluent.ksql.function.udaf.UdafDescription;
import io.confluent.ksql.function.udaf.UdafFactory;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;

@UdafDescription(name = "sum",
    description = "Sums the column values.")

public final class SumUdaf {

  private SumUdaf() {
  }


  @UdafFactory(description = "Sum long values.")
  public static TableUdaf<Long, Long, Long> sumLong() {

    return getSumImplementation(
        0L,
        Long::sum,
        (valueToUndo, aggregateValue) -> aggregateValue - valueToUndo
    );
  }

  @UdafFactory(description = "Sum int values.")
  public static TableUdaf<Integer, Integer, Integer> sumInt() {

    return getSumImplementation(
        0,
        Integer::sum,
        (valueToUndo, aggregateValue) -> aggregateValue - valueToUndo
    );
  }

  @UdafFactory(description = "Sum double values.")
  public static TableUdaf<Double, Double, Double> sumDouble() {

    return getSumImplementation(
        0.0,
        Double::sum,
        (valueToUndo, aggregateValue) -> aggregateValue - valueToUndo
    );
  }

  @UdafFactory(description = "Sum double values in a list into a single double")
  public static TableUdaf<List<Long>, Long, Long> sumLongList() {
    return getListSumImplementation(
        0L,
        (list) -> sumLongList(list),
        Long::sum,
        (valueToUndo, aggregateValue) -> aggregateValue - valueToUndo
    );
  }

  private static long sumLongList(final List<Long> list) {
    return sumGenericList(list, 0L, (a,b) -> a + b);
  }

  @UdafFactory(description = "Sum integer values in a list into a single integer")
  public static TableUdaf<List<Integer>, Integer, Integer> sumIntList() {
    return getListSumImplementation(
        0,
        (list) -> sumIntList(list),
        Integer::sum,
        (valueToUndo, aggregateValue) -> aggregateValue - valueToUndo
    );
  }

  private static int sumIntList(final List<Integer> list) {
    return sumGenericList(list, 0, (a,b) -> a + b);
  }

  @UdafFactory(description = "Sum double values in a list into a single double")
  public static TableUdaf<List<Double>, Double, Double> sumDoubleList() {
    return getListSumImplementation(
        0.0,
        (list) -> sumDoubleList(list),
        Double::sum,
        (valueToUndo, aggregateValue) -> aggregateValue - valueToUndo
    );
  }

  private static double sumDoubleList(final List<Double> list) {
    return sumGenericList(list, 0.0, (a,b) -> a + b);
  }

  private static  <I> TableUdaf<I, I, I> getSumImplementation(
      final I initialValue,
      final BiFunction<I, I, I> summer,
      final BiFunction<I, I, I> undoer) {

    return new TableUdaf<I, I, I>() {

      @Override
      public I initialize() {
        return initialValue;
      }

      @Override
      public I aggregate(final I newValue, final I aggregateValue) {
        if (newValue == null) {
          return aggregateValue;
        }
        return summer.apply(newValue, aggregateValue);
      }

      @Override
      public I map(final I aggregate) {
        return aggregate;
      }

      @Override
      public I merge(final I agg1, final I agg2) {
        return summer.apply(agg1, agg2);
      }

      @Override
      public I undo(final I valueToUndo, final I aggregateValue) {
        if (valueToUndo == null) {
          return aggregateValue;
        }
        return undoer.apply(valueToUndo, aggregateValue);
      }
    };
  }

  private static <I> TableUdaf<List<I>, I, I> getListSumImplementation(
      final I initialValue,
      final Function<List<I>, I> sumList,
      final BiFunction<I, I, I> summer,
      final BiFunction<I, I, I> undoer) {

    return new TableUdaf<List<I>, I, I>() {

      @Override
      public I initialize() {
        return initialValue;
      }

      @Override
      public I aggregate(final List<I> newValue, final I aggregateValue) {
        if (newValue == null) {
          return aggregateValue;
        }
        return summer.apply(aggregateValue, sumList.apply(newValue));
      }

      @Override
      public I map(final I aggregate) {
        return aggregate;
      }

      @Override
      public I merge(final I agg1, final I agg2) {
        return summer.apply(agg1, agg2);
      }

      @Override
      public I undo(final List<I> valueToUndo, final I aggregateValue) {
        if (valueToUndo == null) {
          return aggregateValue;
        }
        return undoer.apply(sumList.apply(valueToUndo), aggregateValue);
      }
    };
  }

  private static <I> I sumGenericList(
      final List<I> list,
      final I initial,
      final BiFunction<I,I,I> summer) {

    I sum = initial;
    for (I v: list) {
      if (v == null) {
        continue;
      }
      sum = summer.apply(sum, v);
    }
    return sum;
  }
}
