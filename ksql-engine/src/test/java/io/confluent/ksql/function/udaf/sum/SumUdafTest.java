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

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

import io.confluent.ksql.function.udaf.TableUdaf;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.Test;

public class SumUdafTest {

  @Test
  public void shouldSumLongs() {
    final TableUdaf<Long, Long, Long> udaf = SumUdaf.sumLong();
    final Long[] values = new Long[]{3L, 5L, 8L, 2L, 3L, 4L, 5L};
    testSum(udaf, values, 30L);
  }

  @Test
  public void shouldHandleNullLongs() {
    final TableUdaf<Long, Long, Long> udaf = SumUdaf.sumLong();
    final Long[] values = new Long[]{3L, 5L, 8L, 2L, 3L, 4L, 5L};
    testHandleNulls(udaf, values, 30L);
  }

  @Test
  public void shouldMergeLongs() {
    final TableUdaf<Long, Long, Long> udaf = SumUdaf.sumLong();
    final Long mergeResult1 = udaf.merge(10L, 12L);
    assertThat(mergeResult1, equalTo(22L));
    final Long mergeResult2 = udaf.merge(10L, -12L);
    assertThat(mergeResult2, equalTo(-2L));
    final Long mergeResult3 = udaf.merge(-10L, 0L);
    assertThat(mergeResult3, equalTo(-10L));
  }

  @Test
  public void shouldUndoElementLongs() {
    final TableUdaf<Long, Long, Long> udaf = SumUdaf.sumLong();
    final Long[] values = new Long[]{3L, 5L, 8L, 2L, 3L, 4L, 5L};
    testUndoElement(udaf, values, 3L, 30L, 27L);
  }

  @Test
  public void shouldUndoElementHandleNullLongs() {
    final TableUdaf<Long, Long, Long> udaf = SumUdaf.sumLong();
    final Long[] values = new Long[]{3L, 5L, 8L, 2L, 3L, 4L, 5L};
    testUndoElementHandleNull(udaf, values, 30L);
  }

  @Test
  public void shouldSumIntegers() {
    final TableUdaf<Integer, Integer, Integer> udaf = SumUdaf.sumInt();
    final Integer[] values = new Integer[]{3, 5, 8, 2, 3, 4, 5};
    testSum(udaf, values, 30);
  }

  @Test
  public void shouldHandleNullIntegers() {
    final TableUdaf<Integer, Integer, Integer> udaf = SumUdaf.sumInt();
    final Integer[] values = new Integer[]{3, 5, 8, 2, 3, 4, 5};
    testHandleNulls(udaf, values, 30);
  }

  @Test
  public void shouldMergeIntegers() {
    final TableUdaf<Integer, Integer, Integer> udaf = SumUdaf.sumInt();
    final Integer mergeResult1 = udaf.merge(10, 12);
    assertThat(mergeResult1, equalTo(22));
    final Integer mergeResult2 = udaf.merge(10, -12);
    assertThat(mergeResult2, equalTo(-2));
    final Integer mergeResult3 = udaf.merge(-10, 0);
    assertThat(mergeResult3, equalTo(-10));
  }

  @Test
  public void shouldUndoElementIntegers() {
    final TableUdaf<Integer, Integer, Integer> udaf = SumUdaf.sumInt();
    final Integer[] values = new Integer[]{3, 5, 8, 2, 3, 4, 5};
    testUndoElement(udaf, values, 3, 30, 27);
  }

  @Test
  public void shouldUndoElementHandleNullIntegers() {
    final TableUdaf<Integer, Integer, Integer> udaf = SumUdaf.sumInt();
    final Integer[] values = new Integer[]{3, 5, 8, 2, 3, 4, 5};
    testUndoElementHandleNull(udaf, values, 30);
  }

  @Test
  public void shouldSumDoubles() {
    final TableUdaf<Double, Double, Double> udaf = SumUdaf.sumDouble();
    final Double[] values = new Double[]{3.0, 5.0, 8.0, 2.2, 3.0, 4.0, 5.0};
    testSum(udaf, values, 30.2);
  }

  @Test
  public void shouldHandleNullDoubles() {
    final TableUdaf<Double, Double, Double> udaf = SumUdaf.sumDouble();
    final Double[] values = new Double[]{3.0, 5.0, 8.0, 2.2, 3.0, 4.0, 5.0};
    testHandleNulls(udaf, values, 30.2);
  }
  @Test
  public void shouldMergeDoubles() {
    final TableUdaf<Double, Double, Double> udaf = SumUdaf.sumDouble();
    final Double mergeResult1 = udaf.merge(10.0, 12.0);
    assertThat(mergeResult1, equalTo(22.0));
    final Double mergeResult2 = udaf.merge(10.0, -12.0);
    assertThat(mergeResult2, equalTo(-2.0));
    final Double mergeResult3 = udaf.merge(-10.0, 0.0);
    assertThat(mergeResult3, equalTo(-10.0));
  }

  @Test
  public void shouldUndoElementDoubles() {
    final TableUdaf<Double, Double, Double> udaf = SumUdaf.sumDouble();
    final Double[] values = new Double[]{3.0, 5.0, 8.0, 2.2, 3.0, 4.0, 5.0};
    testUndoElement(udaf, values, 3.0, 30.2, 27.2);
  }

  @Test
  public void shouldUndoElementHandleNullDoubles() {
    final TableUdaf<Double, Double, Double> udaf = SumUdaf.sumDouble();
    final Double[] values = new Double[]{3.0, 5.0, 8.0, 2.2, 3.0, 4.0, 5.0};
    testUndoElementHandleNull(udaf, values, 30.2);
  }

  @Test
  public void shouldSumLongList() {
    final TableUdaf<List<Long>, Long, Long>  udaf = SumUdaf.sumLongList();
    final Long[] values = new Long[]{3L, 5L, 8L, 2L, 3L, 4L, 5L};
    testSumList(udaf, values, 30L);
  }

  @Test
  public void shouldHandleNullLongList() {
    final TableUdaf<List<Long>, Long, Long>  udaf = SumUdaf.sumLongList();
    final Long[] values = new Long[]{3L, 5L, 8L, 2L, 3L, 4L, 5L};
    testHandleNullList(udaf, values, 30L);
  }

  @Test
  public void shouldHandleEmptyLongList() {
    final TableUdaf<List<Long>, Long, Long>  udaf = SumUdaf.sumLongList();
    testHandleEmptyList(udaf, 0L);
  }

  @Test
  public void shouldMergeLongLists() {
    final TableUdaf<List<Long>, Long, Long>  udaf = SumUdaf.sumLongList();
    final Long mergeResult1 = udaf.merge(10L, 12L);
    assertThat(mergeResult1, equalTo(22L));
    final Long mergeResult2 = udaf.merge(10L, -12L);
    assertThat(mergeResult2, equalTo(-2L));
    final Long mergeResult3 = udaf.merge(-10L, 0L);
    assertThat(mergeResult3, equalTo(-10L));
  }

  @Test
  public void shouldUndoElementLongList() {
    final TableUdaf<List<Long>, Long, Long>  udaf = SumUdaf.sumLongList();
    final Long[] values = new Long[]{3L, 5L, 8L, 2L, 3L, 4L, 5L};
    final Long[] undoValues = new Long[]{3L, 5L, 8L, 2L, 3L, 4L};
    testUndoElementList(udaf, values, undoValues, 30L, 5L);
  }

  @Test
  public void shouldUndoElementHandleNullLongList() {
    final TableUdaf<List<Long>, Long, Long>  udaf = SumUdaf.sumLongList();
    final Long[] values = new Long[]{3L, 5L, 8L, 2L, 3L, 4L, 5L};
    testUndoElementHandleNullList(udaf, values, 30L);
  }

  @Test
  public void shouldSumIntegerList() {
    final TableUdaf<List<Integer>, Integer, Integer> udaf = SumUdaf.sumIntList();
    final Integer[] values = new Integer[]{3, 5, 8, 2, 3, 4, 5};
    testSumList(udaf, values, 30);
  }

  @Test
  public void shouldHandleNullIntegerList() {
    final TableUdaf<List<Integer>, Integer, Integer> udaf = SumUdaf.sumIntList();
    final Integer[] values = new Integer[]{3, 5, 8, 2, 3, 4, 5};
    testHandleNullList(udaf, values, 30);
  }

  @Test
  public void shouldHandleEmptyIntegerList() {
    final TableUdaf<List<Integer>, Integer, Integer> udaf = SumUdaf.sumIntList();
    testHandleEmptyList(udaf, 0);
  }

  @Test
  public void shouldMergeIntegerLists() {
    final TableUdaf<List<Integer>, Integer, Integer> udaf = SumUdaf.sumIntList();
    final Integer mergeResult1 = udaf.merge(10, 12);
    assertThat(mergeResult1, equalTo(22));
    final Integer mergeResult2 = udaf.merge(10, -12);
    assertThat(mergeResult2, equalTo(-2));
    final Integer mergeResult3 = udaf.merge(-10, 0);
    assertThat(mergeResult3, equalTo(-10));
  }

  @Test
  public void shouldUndoElementIntegerList() {
    final TableUdaf<List<Integer>, Integer, Integer> udaf = SumUdaf.sumIntList();
    final Integer[] values = new Integer[]{3, 5, 8, 2, 3, 4, 5};
    final Integer[] undoValues = new Integer[]{3, 5, 8, 2, 3, 4, 5};
    testUndoElementList(udaf, values, undoValues, 30, 0);
  }

  @Test
  public void shouldUndoElementHandleNullIntegerList() {
    final TableUdaf<List<Integer>, Integer, Integer> udaf = SumUdaf.sumIntList();
    final Integer[] values = new Integer[]{3, 5, 8, 2, 3, 4, 5};
    testUndoElementHandleNullList(udaf, values, 30);
  }

  @Test
  public void shouldSumDoubleList() {
    final TableUdaf<List<Double>, Double, Double> udaf = SumUdaf.sumDoubleList();
    final Double[] values = new Double[]{3.0, 5.0, 8.0, 2.2, 3.0, 4.0, 5.0};
    testSumList(udaf, values, 30.2);
  }

  @Test
  public void shouldHandleNullDoubleList() {
    final TableUdaf<List<Double>, Double, Double> udaf = SumUdaf.sumDoubleList();
    final Double[] values = new Double[]{3.0, 5.0, 8.0, 2.2, 3.0, 4.0, 5.0};
    testHandleNullList(udaf, values, 30.2);
  }
  @Test
  public void shouldHandleEmptyDoubleList() {
    final TableUdaf<List<Double>, Double, Double> udaf = SumUdaf.sumDoubleList();
    testHandleEmptyList(udaf, 0.0);
  }

  @Test
  public void shouldMergeDoubleLists() {
    final TableUdaf<List<Double>, Double, Double> udaf = SumUdaf.sumDoubleList();
    final Double mergeResult1 = udaf.merge(10.0, 12.0);
    assertThat(mergeResult1, equalTo(22.0));
    final Double mergeResult2 = udaf.merge(10.0, -12.0);
    assertThat(mergeResult2, equalTo(-2.0));
    final Double mergeResult3 = udaf.merge(-10.0, 0.0);
    assertThat(mergeResult3, equalTo(-10.0));
  }

  @Test
  public void shouldUndoElementDoubleList() {
    final TableUdaf<List<Double>, Double, Double> udaf = SumUdaf.sumDoubleList();
    final Double[] values = new Double[]{3.0, 5.0, 8.0, 2.2, 3.0, 4.0, 5.0};
    final Double[] undoValues = new Double[]{3.0, 5.0, 8.0, 2.2, 3.0, 4.0};
    testUndoElementList(udaf, values, undoValues, 30.2, 5.0);
  }

  @Test
  public void shouldUndoElementHandleNullDoubleList() {
    final TableUdaf<List<Double>, Double, Double> udaf = SumUdaf.sumDoubleList();
    final Double[] values = new Double[]{3.0, 5.0, 8.0, 2.2, 3.0, 4.0, 5.0};
    testUndoElementHandleNullList(udaf, values, 30.2);
  }

  private <I> void testSum(final TableUdaf<I, I, I> udaf,
                          final I[] values,
                          final I expectedValue) {

    I currentSum = udaf.initialize();
    for (final I i: values) {
      currentSum = udaf.aggregate(i, currentSum);
    }
    assertThat(expectedValue, equalTo(currentSum));
  }

  private <I> void testSumList(final TableUdaf<List<I>, I, I> udaf,
                           final I[] valuesArray,
                           final I expectedValue) {

    List<I> values = Arrays.asList(valuesArray);
    I currentSum = udaf.initialize();
    currentSum = udaf.aggregate(values, currentSum);
    assertThat(expectedValue, equalTo(currentSum));
  }


  private <I> void testHandleNulls(final TableUdaf<I, I, I> udaf,
                                  final I[] values,
                                  final I expectedValue) {

    I initialSum = udaf.initialize();
    I currentSum = udaf.initialize();

    // null before any aggregation
    currentSum = udaf.aggregate(null, currentSum);
    assertThat(initialSum, equalTo(currentSum));

    // now send each value to aggregation and verify
    for (final I i: values) {
      currentSum = udaf.aggregate(i, currentSum);
    }
    assertThat(expectedValue, equalTo(currentSum));

    // null should not impact result
    currentSum = udaf.aggregate(null, currentSum);
    assertThat(expectedValue, equalTo(currentSum));
  }

  private <I> void testHandleNullList(final TableUdaf<List<I>, I, I> udaf,
                                   final I[] valuesArray,
                                   final I expectedValue) {

    List<I> values = Arrays.asList(valuesArray);

    I initialSum = udaf.initialize();
    I currentSum = udaf.initialize();

    // null before any aggregation
    currentSum = udaf.aggregate(null, currentSum);
    assertThat(initialSum, equalTo(currentSum));

    // now send each value to aggregation and verify
    currentSum = udaf.aggregate(values, currentSum);
    assertThat(expectedValue, equalTo(currentSum));

    // null should not impact result
    currentSum = udaf.aggregate(null, currentSum);
    assertThat(expectedValue, equalTo(currentSum));
  }

  private <I> void testHandleEmptyList(final TableUdaf<List<I>, I, I> udaf,
                                      final I expectedValue) {

    I currentSum = udaf.initialize();
    currentSum = udaf.aggregate(Collections.emptyList(), currentSum);
    assertThat(expectedValue, equalTo(currentSum));
  }

  private <I> void testUndoElement(final TableUdaf<I, I, I> udaf,
                                  final I[] values,
                                  final I undoValue,
                                  final I expectedValue,
                                  final I newExpectedValue) {

    I currentSum = udaf.initialize();
    for (final I i: values) {
      currentSum = udaf.aggregate(i, currentSum);
    }
    assertThat(expectedValue, equalTo(currentSum));
    currentSum = udaf.undo(undoValue, currentSum);
    assertThat(newExpectedValue, equalTo(currentSum));
  }

  private <I> void testUndoElementList(final TableUdaf<List<I>, I, I> udaf,
                                   final I[] valuesArray,
                                   final I[] undoValuesArray,
                                   final I expectedValue,
                                   final I newExpectedValue) {

    List<I> values = Arrays.asList(valuesArray);
    List<I> undoValues = Arrays.asList(undoValuesArray);

    I currentSum = udaf.initialize();
    currentSum = udaf.aggregate(values, currentSum);
    assertThat(expectedValue, equalTo(currentSum));

    currentSum = udaf.undo(undoValues, currentSum);
    assertThat(newExpectedValue, equalTo(currentSum));
  }

  private <I> void testUndoElementHandleNull(final TableUdaf<I, I, I> udaf,
                                  final I[] values,
                                  final I expectedValue){

    I currentSum = udaf.initialize();
    for (final I i: values) {
      currentSum = udaf.aggregate(i, currentSum);
    }
    assertThat(expectedValue, equalTo(currentSum));
    currentSum = udaf.undo(null, currentSum);
    assertThat(expectedValue, equalTo(currentSum));
  }

  private <I> void testUndoElementHandleNullList(final TableUdaf<List<I>, I, I> udaf,
                                             final I[] valuesArray,
                                             final I expectedValue){

    List<I> values = Arrays.asList(valuesArray);
    I currentSum = udaf.initialize();
    currentSum = udaf.aggregate(values, currentSum);
    assertThat(expectedValue, equalTo(currentSum));

    currentSum = udaf.undo(null, currentSum);
    assertThat(expectedValue, equalTo(currentSum));
  }
}
