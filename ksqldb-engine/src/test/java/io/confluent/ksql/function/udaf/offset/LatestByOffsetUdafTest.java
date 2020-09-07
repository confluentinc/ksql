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

package io.confluent.ksql.function.udaf.offset;

import static io.confluent.ksql.function.udaf.KudafByOffsetUtils.SEQ_FIELD;
import static io.confluent.ksql.function.udaf.KudafByOffsetUtils.STRUCT_BOOLEAN;
import static io.confluent.ksql.function.udaf.KudafByOffsetUtils.STRUCT_DOUBLE;
import static io.confluent.ksql.function.udaf.KudafByOffsetUtils.STRUCT_INTEGER;
import static io.confluent.ksql.function.udaf.KudafByOffsetUtils.STRUCT_LONG;
import static io.confluent.ksql.function.udaf.KudafByOffsetUtils.STRUCT_STRING;
import static io.confluent.ksql.function.udaf.KudafByOffsetUtils.VAL_FIELD;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

import com.google.common.collect.Lists;
import io.confluent.ksql.function.udaf.Udaf;
import io.confluent.ksql.util.KsqlException;
import java.util.ArrayList;
import java.util.List;
import org.apache.kafka.connect.data.Struct;
import org.junit.Test;


public class LatestByOffsetUdafTest {
  
  @Test
  public void shouldInitialize() {
    // Given:
    final Udaf<Integer, Struct, Integer> udaf = LatestByOffset.latest(STRUCT_LONG);

    // When:
    Struct init = udaf.initialize();

    // Then:
    assertThat(init, is(notNullValue()));
  }

  @Test
  public void shouldInitializeLatestN() {
    // Given:
    final Udaf<Integer, List<Struct>, List<Integer>> udaf = LatestByOffset.latestN(STRUCT_LONG, 2);

    // When:
    List<Struct> init = udaf.initialize();

    // Then:
    assertThat(init, is(notNullValue()));
  }
  
  @Test
  public void shouldThrowExceptionForInvalidN() {
    try {
      LatestByOffset
          // Given:
          .latestN(STRUCT_LONG, -1);

    } catch (KsqlException e) {
      assertThat(e.getMessage(), is("earliestN must be 1 or greater"));
    }
  }

  @Test
  public void shouldComputeLatestInteger() {
    // Given:
    final Udaf<Integer, Struct, Integer> udaf = LatestByOffset.latestInteger();

    // When:
    Struct res = udaf.aggregate(123, LatestByOffset.createStruct(STRUCT_INTEGER, 321));

    // Then:
    assertThat(res.get(VAL_FIELD), is(123));
  }

  @Test
  public void shouldComputeLatestNIntegers() {
    // Given:
    final Udaf<Integer, List<Struct>, List<Integer>> udaf = LatestByOffset.latestIntegers(2);

    // When:
    List<Struct> res = udaf
        .aggregate(123, Lists.newArrayList(LatestByOffset.createStruct(STRUCT_INTEGER, 321)));

    // Then:
    assertThat(res.get(0).get(VAL_FIELD), is(321));
    assertThat(res.get(1).get(VAL_FIELD), is(123));
  }
  
  @Test
  public void shouldCaptureValuesUpToN() {
    // Given:
    final Udaf<Integer, List<Struct>, List<Integer>> udaf = LatestByOffset.latestIntegers(2);
    // When:
    List<Struct> res0 = udaf.aggregate(321, new ArrayList<>());
    List<Struct> res1 = udaf.aggregate(123, res0);
    assertThat(res1, hasSize(2));
    assertThat(res1.get(0).get(VAL_FIELD), is(321));
    assertThat(res1.get(1).get(VAL_FIELD), is(123));
  }
  
  @Test
  public void shouldCaptureValuesPastN() {
    // Given:
    final Udaf<Integer, List<Struct>, List<Integer>> udaf = LatestByOffset.latestIntegers(2);
    final List<Struct> aggregate = Lists.newArrayList(
        LatestByOffset.createStruct(STRUCT_INTEGER, 10),
        LatestByOffset.createStruct(STRUCT_INTEGER, 3)
    );
    // When:
    final List<Struct> result = udaf.aggregate(2, aggregate);
    assertThat(result, hasSize(2));
    assertThat(result.get(0).get(VAL_FIELD), is(3));
    assertThat(result.get(1).get(VAL_FIELD), is(2));
  }

  @Test
  public void shouldMerge() {
    // Given:
    final Udaf<Integer, Struct, Integer> udaf = LatestByOffset.latestInteger();

    Struct agg1 = LatestByOffset.createStruct(STRUCT_INTEGER, 123);
    Struct agg2 = LatestByOffset.createStruct(STRUCT_INTEGER, 321);

    // When:
    Struct merged1 = udaf.merge(agg1, agg2);
    Struct merged2 = udaf.merge(agg2, agg1);

    // Then:
    assertThat(merged1, is(agg2));
    assertThat(merged2, is(agg2));
  }

  @Test
  public void shouldMergeNIntegers() {
    // Given:
    final Udaf<Integer, List<Struct>, List<Integer>> udaf = LatestByOffset.latestIntegers(2);
    final Struct struct1 = LatestByOffset.createStruct(STRUCT_INTEGER, 123);
    final Struct struct2 = LatestByOffset.createStruct(STRUCT_INTEGER, 321);
    final Struct struct3 = LatestByOffset.createStruct(STRUCT_INTEGER, 543);
    final Struct struct4 = LatestByOffset.createStruct(STRUCT_INTEGER, 654);
    final List<Struct> agg1 = new ArrayList<>(Lists.newArrayList(struct1, struct4));
    final List<Struct> agg2 = new ArrayList<>(Lists.newArrayList(struct2, struct3));

    // When:
    final List<Struct> merged1 = udaf.merge(agg1, agg2);
    final List<Struct> merged2 = udaf.merge(agg2, agg1);

    // Then:
    assertThat(merged1, contains(struct3, struct4));
    assertThat(merged2, contains(struct3, struct4));
  }
  
  @Test
  public void shouldMergeNIntegersSmallerThanN() {
    // Given:
    final Udaf<Integer, List<Struct>, List<Integer>> udaf = LatestByOffset.latestIntegers(5);
    final Struct struct1 = LatestByOffset.createStruct(STRUCT_INTEGER, 123);
    final Struct struct2 = LatestByOffset.createStruct(STRUCT_INTEGER, 321);
    final Struct struct3 = LatestByOffset.createStruct(STRUCT_INTEGER, 543);
    final Struct struct4 = LatestByOffset.createStruct(STRUCT_INTEGER, 654);
    final List<Struct> agg1 = new ArrayList<>(Lists.newArrayList(struct1, struct4));
    final List<Struct> agg2 = new ArrayList<>(Lists.newArrayList(struct2, struct3));
    
    // When:
    final List<Struct> merged1 = udaf.merge(agg1, agg2);
    final List<Struct> merged2 = udaf.merge(agg2, agg1);

    // Then:
    assertThat(merged1, contains(struct1, struct2, struct3, struct4));
    assertThat(merged1.size(), is(4));
    assertThat(merged2, contains(struct1, struct2, struct3, struct4));
    assertThat(merged2.size(), is(4));
    
  }

  @Test
  public void shouldMergeWithOverflow() {
    // Given:
    final Udaf<Integer, Struct, Integer> udaf = LatestByOffset.latestInteger();

    LatestByOffset.sequence.set(Long.MAX_VALUE);

    Struct agg1 = LatestByOffset.createStruct(STRUCT_INTEGER, 123);
    Struct agg2 = LatestByOffset.createStruct(STRUCT_INTEGER, 321);

    // When:
    Struct merged1 = udaf.merge(agg1, agg2);
    Struct merged2 = udaf.merge(agg2, agg1);

    // Then:
    assertThat(agg1.getInt64(SEQ_FIELD), is(Long.MAX_VALUE));
    assertThat(agg2.getInt64(SEQ_FIELD), is(Long.MIN_VALUE));
    assertThat(merged1, is(agg2));
    assertThat(merged2, is(agg2));
  }

  @Test
  public void shouldMergeWithOverflowNIntegers() {
    // Given:
    final Udaf<Integer, List<Struct>, List<Integer>> udaf = LatestByOffset.latestIntegers(2);

    LatestByOffset.sequence.set(Long.MAX_VALUE - 1);

    Struct struct1 = LatestByOffset.createStruct(STRUCT_INTEGER, 123);
    Struct struct2 = LatestByOffset.createStruct(STRUCT_INTEGER, 321);
    Struct struct3 = LatestByOffset.createStruct(STRUCT_INTEGER, 543);
    Struct struct4 = LatestByOffset.createStruct(STRUCT_INTEGER, 654);
    
    List<Struct> agg1 = Lists.newArrayList(struct1, struct2);
    List<Struct> agg2 = Lists.newArrayList(struct3, struct4);

    // When:
    final List<Struct> merged1 = udaf.merge(agg1, agg2);
    final List<Struct> merged2 = udaf.merge(agg2, agg1);

    // Then:
    assertThat(agg1.get(0).getInt64(SEQ_FIELD), is(Long.MAX_VALUE - 1));
    assertThat(agg2.get(0).getInt64(SEQ_FIELD), is(Long.MIN_VALUE));
    assertThat(merged1, contains(struct3, struct4));
    assertThat(merged2, contains(struct3, struct4));
  }

  @Test
  public void shouldComputeLatestLong() {
    // Given:
    final Udaf<Long, Struct, Long> udaf = LatestByOffset.latestLong();

    // When:
    Struct res = udaf.aggregate(123L, LatestByOffset.createStruct(STRUCT_LONG, 321L));

    // Then:
    assertThat(res.getInt64(VAL_FIELD), is(123L));
  }
  
  @Test
  public void shouldComputeLatestNLongs() {
    // Given:
    final Udaf<Long, List<Struct>, List<Long>> udaf = LatestByOffset.latestLong(3);

    // When:
    List<Struct> res = udaf
        .aggregate(123L, Lists.newArrayList(LatestByOffset.createStruct(STRUCT_LONG, 321L)));
    
    List<Struct> res2 = udaf
        .aggregate(543L, res);
    
    List<Struct> res3 = udaf
        .aggregate(654L, res2);
    
    // Then:
    assertThat(res3.size(), is(3));
    assertThat(res3.get(0).get(VAL_FIELD), is(123L));
    assertThat(res3.get(1).get(VAL_FIELD), is(543L));
    assertThat(res3.get(2).get(VAL_FIELD), is(654L));
  }

  @Test
  public void shouldComputeLatestDouble() {
    // Given:
    final Udaf<Double, Struct, Double> udaf = LatestByOffset.latestDouble();

    // When:
    Struct res = udaf.aggregate(1.1d, LatestByOffset.createStruct(STRUCT_DOUBLE, 2.2d));

    // Then:
    assertThat(res.getFloat64(VAL_FIELD), is(1.1d));
  }
  
  @Test
  public void shouldComputeLatestNDoubles() {
    // Given:
    final Udaf<Double, List<Struct>, List<Double>> udaf = LatestByOffset.latestDoubles(1);

    // When:
    List<Struct> res = udaf
        .aggregate(1.1d, Lists.newArrayList(EarliestByOffset.createStruct(STRUCT_DOUBLE, 2.2d)));

    // Then:
    assertThat(res.size(), is(1));
    assertThat(res.get(0).get(VAL_FIELD), is(1.1d));
  }

  @Test
  public void shouldComputeLatestBoolean() {
    // Given:
    final Udaf<Boolean, Struct, Boolean> udaf = LatestByOffset.latestBoolean();

    // When:
    Struct res = udaf.aggregate(true, LatestByOffset.createStruct(STRUCT_BOOLEAN, false));

    // Then:
    assertThat(res.getBoolean(VAL_FIELD), is(true));
  }
  
  @Test
  public void shouldComputeLatestNBooleans() {
    // Given:
    final Udaf<Boolean, List<Struct>, List<Boolean>> udaf = LatestByOffset.latestBooleans(2);

    // When:
    List<Struct> res = udaf
        .aggregate(true, Lists.newArrayList(LatestByOffset.createStruct(STRUCT_BOOLEAN, false)));

    // Then:
    assertThat(res.size(), is(2));
    assertThat(res.get(0).get(VAL_FIELD), is(false));
    assertThat(res.get(1).get(VAL_FIELD), is(true));
  }

  @Test
  public void shouldComputeLatestString() {
    // Given:
    final Udaf<String, Struct, String> udaf = LatestByOffset.latestString();

    // When:
    Struct res = udaf.aggregate("foo", LatestByOffset.createStruct(STRUCT_STRING, "bar"));

    // Then:
    assertThat(res.getString(VAL_FIELD), is("foo"));
  }
  
  @Test
  public void shouldComputeLatestNStrings() {
    // Given:
    final Udaf<String, List<Struct>, List<String>> udaf = LatestByOffset.latestStrings(3);

    // When:
    List<Struct> res = udaf.aggregate("boo",
        Lists.newArrayList(LatestByOffset.createStruct(STRUCT_STRING, "foo"),
            LatestByOffset.createStruct(STRUCT_STRING, "bar"),
            LatestByOffset.createStruct(STRUCT_STRING, "baz")));
    
    // Then:
    assertThat(res.size(), is(3));
    assertThat(res.get(0).get(VAL_FIELD), is("bar"));
    assertThat(res.get(1).get(VAL_FIELD), is("baz"));
    assertThat(res.get(2).get(VAL_FIELD), is("boo"));  
  }
}
