/*
 * Copyright 2020 Confluent Inc.
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

package io.confluent.ksql.function.udaf.earliest;

import io.confluent.ksql.function.udaf.Udaf;
import org.apache.kafka.connect.data.Struct;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class EarliestByOffsetUdafTest {
  @Test
  public void shouldInitialize() {
    // Given:
    final Udaf<Integer, Struct, Integer> udaf = EarliestByOffset
        .earliest(EarliestByOffset.STRUCT_LONG);

    // When:
    Struct init = udaf.initialize();

    // Then:
    assertThat(init, is(notNullValue()));
  }

  @Test
  public void shouldComputeEarliestInteger() {
    // Given:
    final Udaf<Integer, Struct, Integer> udaf = EarliestByOffset.earliestInteger();

    // When:
    Struct res = udaf
        .aggregate(123, EarliestByOffset.createStruct(EarliestByOffset.STRUCT_INTEGER, 321));

    // Then:
    assertThat(res.get(EarliestByOffset.VAL_FIELD), is(321));
  }

  @Test
  public void shouldMerge() {
    // Given:
    final Udaf<Integer, Struct, Integer> udaf = EarliestByOffset.earliestInteger();

    Struct agg1 = EarliestByOffset.createStruct(EarliestByOffset.STRUCT_INTEGER, 123);
    Struct agg2 = EarliestByOffset.createStruct(EarliestByOffset.STRUCT_INTEGER, 321);

    // When:
    Struct merged1 = udaf.merge(agg1, agg2);
    Struct merged2 = udaf.merge(agg2, agg1);

    // Then:
    assertThat(merged1, is(agg1));
    assertThat(merged2, is(agg1));
  }

  @Test
  public void shouldMergeWithOverflow() {
    // Given:
    final Udaf<Integer, Struct, Integer> udaf = EarliestByOffset.earliestInteger();

    EarliestByOffset.sequence.set(Long.MAX_VALUE);

    Struct agg1 = EarliestByOffset.createStruct(EarliestByOffset.STRUCT_INTEGER, 123);
    Struct agg2 = EarliestByOffset.createStruct(EarliestByOffset.STRUCT_INTEGER, 321);

    // When:
    Struct merged1 = udaf.merge(agg1, agg2);
    Struct merged2 = udaf.merge(agg2, agg1);

    // Then:
    assertThat(agg1.getInt64(EarliestByOffset.SEQ_FIELD), is(Long.MAX_VALUE));
    assertThat(agg2.getInt64(EarliestByOffset.SEQ_FIELD), is(Long.MIN_VALUE));
    assertThat(merged1, is(agg1));
    assertThat(merged2, is(agg1));
  }


  @Test
  public void shouldComputeEarliestLong() {
    // Given:
    final Udaf<Long, Struct, Long> udaf = EarliestByOffset.earliestLong();

    // When:
    Struct res = udaf
        .aggregate(123L, EarliestByOffset.createStruct(EarliestByOffset.STRUCT_LONG, 321L));

    // Then:
    assertThat(res.getInt64(EarliestByOffset.VAL_FIELD), is(321L));
  }

  @Test
  public void shouldComputeEarliestDouble() {
    // Given:
    final Udaf<Double, Struct, Double> udaf = EarliestByOffset.earliestDouble();

    // When:
    Struct res = udaf
        .aggregate(1.1d, EarliestByOffset.createStruct(EarliestByOffset.STRUCT_DOUBLE, 2.2d));

    // Then:
    assertThat(res.getFloat64(EarliestByOffset.VAL_FIELD), is(2.2d));
  }

  @Test
  public void shouldComputeEarliestBoolean() {
    // Given:
    final Udaf<Boolean, Struct, Boolean> udaf = EarliestByOffset.earliestBoolean();

    // When:
    Struct res = udaf
        .aggregate(true, EarliestByOffset.createStruct(EarliestByOffset.STRUCT_BOOLEAN, false));

    // Then:
    assertThat(res.getBoolean(EarliestByOffset.VAL_FIELD), is(false));
  }

  @Test
  public void shouldComputeEarliestString() {
    // Given:
    final Udaf<String, Struct, String> udaf = EarliestByOffset.earliestString();

    // When:
    Struct res = udaf
        .aggregate("foo", EarliestByOffset.createStruct(EarliestByOffset.STRUCT_STRING, "bar"));

    // Then:
    assertThat(res.getString(EarliestByOffset.VAL_FIELD), is("bar"));
  }
}
