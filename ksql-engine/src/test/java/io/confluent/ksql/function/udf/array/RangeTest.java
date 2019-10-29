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

package io.confluent.ksql.function.udf.array;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

import java.util.List;
import org.junit.Test;

public class RangeTest {

  private Range rangeUdf = new Range();

  @Test
  public void shouldComputeIntRange() {
    List<Integer> range = rangeUdf.rangeInt(0, 10);
    assertThat(range, hasSize(10));
    int index = 0;
    for (Integer i : range) {
      assertThat(index++, is(i));
    }
  }

  @Test
  public void shouldComputeEmptyIntRange() {
    List<Integer> range = rangeUdf.rangeInt(5, 5);
    assertThat(range, hasSize(0));
  }

  @Test
  public void shouldComputeEmptyIntRangeWhenEndLessThanStart() {
    List<Integer> range = rangeUdf.rangeInt(5, 0);
    assertThat(range, hasSize(0));
  }

  @Test
  public void shouldComputeLongRange() {
    List<Long> range = rangeUdf.rangeLong(0, 10);
    assertThat(range, hasSize(10));
    long index = 0;
    for (Long i : range) {
      assertThat(index++, is(i));
    }
  }

  @Test
  public void shouldComputeEmptyLongRange() {
    List<Long> range = rangeUdf.rangeLong(5, 5);
    assertThat(range, hasSize(0));
  }

  @Test
  public void shouldComputeEmptyLongRangeWhenEndLessThanStart() {
    List<Long> range = rangeUdf.rangeLong(5, 0);
    assertThat(range, hasSize(0));
  }

}
