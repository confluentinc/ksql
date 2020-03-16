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

package io.confluent.ksql.function.udaf.latest;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

import io.confluent.ksql.function.udaf.Udaf;
import org.apache.kafka.connect.data.Struct;
import org.junit.Test;

public class LatestByOffsetUdafTest {

  @Test
  public void shouldInitializeToNull() {
    // Given:
    final Udaf<Integer, Struct, Integer> udaf = LatestByOffset
        .latest(LatestByOffset.STRUCT_LONG);

    // When:
    Struct init = udaf.initialize();

    // Then:
    assertThat(init, is(nullValue()));
  }

  @Test
  public void shouldComputeLatestInteger() {
    // Given:
    final Udaf<Integer, Struct, Integer> udaf = LatestByOffset.latestInteger();

    // When:
    Struct res = udaf
        .aggregate(123, LatestByOffset.createStruct(LatestByOffset.STRUCT_INTEGER, 321));

    // Then:
    assertThat(res.get(LatestByOffset.VAL_FIELD), is(123));
  }

  @Test
  public void shouldMerge() {
    // Given:
    final Udaf<Integer, Struct, Integer> udaf = LatestByOffset.latestInteger();

    Struct agg1 = LatestByOffset.createStruct(LatestByOffset.STRUCT_INTEGER, 123);
    Struct agg2 = LatestByOffset.createStruct(LatestByOffset.STRUCT_INTEGER, 321);

    // When:
    Struct merged1 = udaf.merge(agg1, agg2);
    Struct merged2 = udaf.merge(agg2, agg1);

    // Then:
    assertThat(merged1, is(agg2));
    assertThat(merged2, is(agg2));
  }

  @Test
  public void shouldMergeWithOverflow() {
    // Given:
    final Udaf<Integer, Struct, Integer> udaf = LatestByOffset.latestInteger();

    LatestByOffset.sequence.set(Long.MAX_VALUE);

    Struct agg1 = LatestByOffset.createStruct(LatestByOffset.STRUCT_INTEGER, 123);
    Struct agg2 = LatestByOffset.createStruct(LatestByOffset.STRUCT_INTEGER, 321);

    // When:
    Struct merged1 = udaf.merge(agg1, agg2);
    Struct merged2 = udaf.merge(agg2, agg1);

    // Then:
    assertThat(agg1.getInt64(LatestByOffset.SEQ_FIELD), is(Long.MAX_VALUE));
    assertThat(agg2.getInt64(LatestByOffset.SEQ_FIELD), is(Long.MIN_VALUE));
    assertThat(merged1, is(agg2));
    assertThat(merged2, is(agg2));
  }


  @Test
  public void shouldComputeLatestLong() {
    // Given:
    final Udaf<Long, Struct, Long> udaf = LatestByOffset.latestLong();

    // When:
    Struct res = udaf
        .aggregate(123L, LatestByOffset.createStruct(LatestByOffset.STRUCT_LONG, 321L));

    // Then:
    assertThat(res.getInt64(LatestByOffset.VAL_FIELD), is(123L));
  }

  @Test
  public void shouldComputeLatestDouble() {
    // Given:
    final Udaf<Double, Struct, Double> udaf = LatestByOffset.latestDouble();

    // When:
    Struct res = udaf
        .aggregate(1.1d, LatestByOffset.createStruct(LatestByOffset.STRUCT_DOUBLE, 2.2d));

    // Then:
    assertThat(res.getFloat64(LatestByOffset.VAL_FIELD), is(1.1d));
  }

  @Test
  public void shouldComputeLatestBoolean() {
    // Given:
    final Udaf<Boolean, Struct, Boolean> udaf = LatestByOffset.latestBoolean();

    // When:
    Struct res = udaf
        .aggregate(true, LatestByOffset.createStruct(LatestByOffset.STRUCT_BOOLEAN, false));

    // Then:
    assertThat(res.getBoolean(LatestByOffset.VAL_FIELD), is(true));
  }

  @Test
  public void shouldComputeLatestString() {
    // Given:
    final Udaf<String, Struct, String> udaf = LatestByOffset.latestString();

    // When:
    Struct res = udaf
        .aggregate("foo", LatestByOffset.createStruct(LatestByOffset.STRUCT_STRING, "bar"));

    // Then:
    assertThat(res.getString(LatestByOffset.VAL_FIELD), is("foo"));
  }

}