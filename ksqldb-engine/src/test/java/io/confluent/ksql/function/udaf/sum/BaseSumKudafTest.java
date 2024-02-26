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

package io.confluent.ksql.function.udaf.sum;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

import io.confluent.ksql.function.udaf.TableUdaf;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.Ignore;
import org.junit.Test;

@Ignore
public abstract class BaseSumKudafTest<
    T extends Number, AT extends TableUdaf<T, T, T>> {
  protected interface TGenerator<TG> {
    TG fromInt(Integer s);
  }

  @Test
  public void shouldComputeCorrectSum() {
    final TGenerator<T> tGenerator = getTGenerator();
    final AT sumKudaf = getSumKudaf();
    T currentVal = tGenerator.fromInt(0);
    final List<T> values = Stream.of(3, 5, 8, 2, 3, 4, 5)
        .map(tGenerator::fromInt).collect(Collectors.toList());
    for (final T i : values) {
      currentVal = sumKudaf.aggregate(i, currentVal);
    }
    assertThat(currentVal, equalTo(tGenerator.fromInt(30)));
  }

  @Test
  public void shouldHandleNullsInSum() {
    final TGenerator<T> tGenerator = getTGenerator();
    final AT sumKudaf = getSumKudaf();
    T currentVal = tGenerator.fromInt(0);
    final List<T> values = Stream.of(3, null, 8, 2, 3, 4, 5)
        .map(tGenerator::fromInt).collect(Collectors.toList());
    for (final T i : values) {
      currentVal = sumKudaf.aggregate(i, currentVal);
    }
    assertThat(currentVal, equalTo(tGenerator.fromInt(25)));
  }

  @Test
  public void shouldComputeCorrectSubtraction() {
    final TGenerator<T> tGenerator = getTGenerator();
    final AT sumKudaf = getSumKudaf();
    T currentVal = tGenerator.fromInt(30);
    final List<T> values = Stream.of(3, 5, 8, 2, 3, 4, 5)
        .map(tGenerator::fromInt).collect(Collectors.toList());
    for (final T i: values) {
      currentVal = sumKudaf.undo(i, currentVal);
    }
    assertThat(currentVal, equalTo(tGenerator.fromInt(0)));
  }

  @Test
  public void shouldHandleNullsInUndo() {
    final TGenerator<T> tGenerator = getTGenerator();
    final AT sumKudaf = getSumKudaf();
    T currentVal = tGenerator.fromInt(30);
    final List<T> values = Stream.of(3, null, 8, 2, 3, 4, 5)
        .map(tGenerator::fromInt).collect(Collectors.toList());
    for (final T i: values) {
      currentVal = sumKudaf.undo(i, currentVal);
    }
    assertThat(currentVal, equalTo(tGenerator.fromInt(5)));
  }

  @Test
  public void shouldComputeCorrectSumMerge() {
    final TGenerator<T> tGenerator = getTGenerator();
    final AT sumKudaf = getSumKudaf();
    final T mergeResult1 = sumKudaf.merge(tGenerator.fromInt(10), tGenerator.fromInt(12));
    assertThat(mergeResult1, equalTo(tGenerator.fromInt(22)));
    final T mergeResult2 = sumKudaf.merge(tGenerator.fromInt(10), tGenerator.fromInt(-12));
    assertThat(mergeResult2, equalTo(tGenerator.fromInt(-2)));
    final T mergeResult3 = sumKudaf.merge(tGenerator.fromInt(-10), tGenerator.fromInt(0));
    assertThat(mergeResult3, equalTo(tGenerator.fromInt(-10)));
  }

  private TGenerator<T> getTGenerator() {
    final TGenerator<T> underlying = getNumberGenerator();
    return value -> {
      if (value == null) {
        return null;
      }
      return underlying.fromInt(value);
    };
  }

  abstract TGenerator<T> getNumberGenerator();

  protected abstract AT getSumKudaf();
}
