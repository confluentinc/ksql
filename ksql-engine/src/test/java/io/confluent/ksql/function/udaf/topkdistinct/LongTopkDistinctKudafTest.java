/**
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

import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

public class LongTopkDistinctKudafTest {

  Long[] valueArray;
  @Before
  public void setup() {
    valueArray = new Long[]{10l, 30l, 45l, 10l, 50l, 60l, 20l, 60l, 80l, 35l, 25l,
                              60l, 80l};

  }

  @Test
  public void shouldAggregateTopK() {
    LongTopkDistinctKudaf longTopkDistinctKudaf = new LongTopkDistinctKudaf(0, 3);
    Long[] currentVal = new Long[]{null, null, null};
    for (Long d: valueArray) {
      currentVal = longTopkDistinctKudaf.aggregate(d, currentVal);
    }

    assertThat("Invalid results.", currentVal, equalTo(new Long[]{80l, 60l, 50l}));
  }

  @Test
  public void shouldMergeTopK() {
    LongTopkDistinctKudaf longTopkDistinctKudaf = new LongTopkDistinctKudaf(0, 3);
    Long[] array1 = new Long[]{50l, 45l, 25l};
    Long[] array2 = new Long[]{60l, 50l, 48l};

    assertThat("Invalid results.", longTopkDistinctKudaf.getMerger().apply("key", array1, array2), equalTo(
        new Long[]{60l, 50l, 48l}));
  }

  @Test
  public void shouldMergeTopKWithNulls() {
    LongTopkDistinctKudaf longTopkDistinctKudaf = new LongTopkDistinctKudaf(0, 3);
    Long[] array1 = new Long[]{50l, 45l, null};
    Long[] array2 = new Long[]{60l, null, null};

    assertThat("Invalid results.", longTopkDistinctKudaf.getMerger().apply("key", array1, array2), equalTo(
        new Long[]{60l, 50l, 45l}));
  }

  @Test
  public void shouldMergeTopKWithNullsDuplicates() {
    LongTopkDistinctKudaf longTopkDistinctKudaf = new LongTopkDistinctKudaf(0, 3);
    Long[] array1 = new Long[]{50l, 45l, null};
    Long[] array2 = new Long[]{60l, 50l, null};

    assertThat("Invalid results.", longTopkDistinctKudaf.getMerger().apply("key", array1, array2), equalTo(
        new Long[]{60l, 50l, 45l}));
  }

  @Test
  public void shouldMergeTopKWithMoreNulls() {
    LongTopkDistinctKudaf longTopkDistinctKudaf = new LongTopkDistinctKudaf(0, 3);
    Long[] array1 = new Long[]{60l, null, null};
    Long[] array2 = new Long[]{60l, null, null};

    assertThat("Invalid results.", longTopkDistinctKudaf.getMerger().apply("key", array1, array2), equalTo(
        new Long[]{60l, null, null}));
  }
}
