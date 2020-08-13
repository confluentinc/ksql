/*
 * Copyright 2019 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"; you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the
 * License.
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
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import io.confluent.ksql.function.udaf.Udaf;
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
  public void shouldInitializeLatest2Integers() {
    // Given:
    final Udaf<Integer, List<Struct>, List<Integer>> udaf = LatestByOffset.latestN(STRUCT_LONG, 2);

    // When:
    List<Struct> init = udaf.initialize();

    // Then:
    assertThat(init, is(notNullValue()));
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
  public void shouldComputeLatest2Integers() {
    // Given:
    final Udaf<Integer, List<Struct>, List<Integer>> udaf = LatestByOffset.latestInteger(2);

    List<Struct> list = new ArrayList<>();
    list.add(LatestByOffset.createStruct(STRUCT_INTEGER, 321));

    // When:
    List<Struct> res = udaf.aggregate(123, list);

    assertThat(list.size(), is(2));
    // Then:
    assertThat(res.get(0).get(VAL_FIELD), is(321));
    assertThat(res.get(1).get(VAL_FIELD), is(123));

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
  public void shouldMerge2Integers() {
    // Given:
    final Udaf<Integer, List<Struct>, List<Integer>> udaf = LatestByOffset.latestInteger(2);

    Struct struct1 = LatestByOffset.createStruct(STRUCT_INTEGER, 123);
    List<Struct> agg1 = new ArrayList<>(Arrays.asList(struct1));


    Struct struct2 = LatestByOffset.createStruct(STRUCT_INTEGER, 321);
    List<Struct> agg2 = new ArrayList<>(Arrays.asList(struct2));

    List<Struct> agg3 = new ArrayList<>(Arrays.asList(struct1, struct2));

    // When:
    List<Struct> merged1 = udaf.merge(agg1, agg2);
    List<Struct> merged2 = udaf.merge(agg2, agg1);

    // Then:
    assertThat(merged1, is(agg3));
    assertThat(merged2, is(agg3));
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
  public void shouldMergeWithOverflow1Integer() {
    // Given:
    final Udaf<Integer, List<Struct>, List<Integer>> udaf = LatestByOffset.latestInteger(1);

    LatestByOffset.sequence.set(Long.MAX_VALUE);

    List<Struct> agg1 =
        new ArrayList<>(Arrays.asList(LatestByOffset.createStruct(STRUCT_INTEGER, 123)));
    List<Struct> agg2 =
        new ArrayList<>(Arrays.asList(LatestByOffset.createStruct(STRUCT_INTEGER, 321)));
    List<Struct> agg3 =
        new ArrayList<>(Arrays.asList(agg1.get(0),agg2.get(0)));

    // When:
    List<Struct> merged1 = udaf.merge(agg1, agg2);
    List<Struct> merged2 = udaf.merge(agg2, agg1);

    // Then:
    assertThat(agg1.get(0).getInt64(SEQ_FIELD), is(Long.MAX_VALUE));
    assertThat(agg2.get(0).getInt64(SEQ_FIELD), is(Long.MIN_VALUE));
    assertThat(merged1, is(agg2));
    assertThat(merged2, is(agg2));
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
  public void shouldComputeLatest2Longs() {
    // Given:
    final Udaf<Long, List<Struct>, List<Long>> udaf = LatestByOffset.latestLong(2);

    List<Struct> list = new ArrayList<>();
    list.add(LatestByOffset.createStruct(STRUCT_LONG, 321L));

    // When:
    List<Struct> res = udaf.aggregate(123L, list);

    assertThat(list.size(), is(2));
    // Then:
    assertThat(res.get(0).get(VAL_FIELD), is(321L));
    assertThat(res.get(1).get(VAL_FIELD), is(123L));

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
  public void shouldComputeLatest2Doubles() {
    // Given:
    final Udaf<Double, List<Struct>, List<Double>> udaf = LatestByOffset.latestDouble(2);

    List<Struct> list = new ArrayList<>();
    list.add(LatestByOffset.createStruct(STRUCT_DOUBLE, 2.2d));

    // When:
    List<Struct> res = udaf.aggregate(1.1d, list);

    assertThat(list.size(), is(2));
    // Then:
    assertThat(res.get(0).get(VAL_FIELD), is(2.2d));
    assertThat(res.get(1).get(VAL_FIELD), is(1.1d));

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
  public void shouldComputeLatest2Booleans() {
    // Given:
    final Udaf<Boolean, List<Struct>, List<Boolean>> udaf = LatestByOffset.latestBoolean(2);

    List<Struct> list = new ArrayList<>();
    list.add(LatestByOffset.createStruct(STRUCT_BOOLEAN, true));

    // When:
    List<Struct> res = udaf.aggregate(false, list);

    assertThat(list.size(), is(2));
    // Then:
    assertThat(res.get(0).get(VAL_FIELD), is(true));
    assertThat(res.get(1).get(VAL_FIELD), is(false));

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
  public void shouldComputeLatest2Strings() {
    // Given:
    final Udaf<String, List<Struct>, List<String>> udaf = LatestByOffset.latestString(2);

    List<Struct> list = new ArrayList<>();
    list.add(LatestByOffset.createStruct(STRUCT_STRING, "foo"));

    // When:
    List<Struct> res = udaf.aggregate("bar", list);

    assertThat(list.size(), is(2));
    // Then:
    assertThat(res.get(0).get(VAL_FIELD), is("foo"));
    assertThat(res.get(1).get(VAL_FIELD), is("bar"));

  }

}
