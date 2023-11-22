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

import static io.confluent.ksql.function.udaf.offset.KudafByOffsetUtils.INTERMEDIATE_STRUCT_COMPARATOR_IGNORE_NULLS;
import static io.confluent.ksql.function.udaf.offset.KudafByOffsetUtils.SEQ_FIELD;
import static io.confluent.ksql.function.udaf.offset.KudafByOffsetUtils.STRUCT_BOOLEAN;
import static io.confluent.ksql.function.udaf.offset.KudafByOffsetUtils.STRUCT_BYTES;
import static io.confluent.ksql.function.udaf.offset.KudafByOffsetUtils.STRUCT_DATE;
import static io.confluent.ksql.function.udaf.offset.KudafByOffsetUtils.STRUCT_DOUBLE;
import static io.confluent.ksql.function.udaf.offset.KudafByOffsetUtils.STRUCT_INTEGER;
import static io.confluent.ksql.function.udaf.offset.KudafByOffsetUtils.STRUCT_LONG;
import static io.confluent.ksql.function.udaf.offset.KudafByOffsetUtils.STRUCT_STRING;
import static io.confluent.ksql.function.udaf.offset.KudafByOffsetUtils.STRUCT_TIME;
import static io.confluent.ksql.function.udaf.offset.KudafByOffsetUtils.STRUCT_TIMESTAMP;
import static io.confluent.ksql.function.udaf.offset.KudafByOffsetUtils.VAL_FIELD;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

import com.google.common.collect.Lists;
import io.confluent.ksql.function.udaf.Udaf;
import io.confluent.ksql.util.KsqlException;
import java.nio.ByteBuffer;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import org.apache.kafka.connect.data.Struct;
import org.junit.Test;


public class LatestByOffsetTest {

  @Test
  public void shouldInitialize() {
    // Given:
    final Udaf<Integer, Struct, Integer> udaf = LatestByOffset.latest(
        STRUCT_LONG, true, INTERMEDIATE_STRUCT_COMPARATOR_IGNORE_NULLS);

    // When:
    final Struct init = udaf.initialize();

    // Then:
    assertThat(init, is(notNullValue()));
  }

  @Test
  public void shouldInitializeLatestN() {
    // Given:
    final Udaf<Integer, List<Struct>, List<Integer>> udaf = LatestByOffset
        .latestN(STRUCT_LONG, 2, true, INTERMEDIATE_STRUCT_COMPARATOR_IGNORE_NULLS);

    // When:
    final List<Struct> init = udaf.initialize();

    // Then:
    assertThat(init, is(empty()));
  }

  @Test
  public void shouldThrowExceptionForInvalidN() {
    try {
      LatestByOffset.latestN(STRUCT_LONG, -1, true, INTERMEDIATE_STRUCT_COMPARATOR_IGNORE_NULLS);
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
    final List<Struct> res0 = udaf.aggregate(321, new ArrayList<>());
    final List<Struct> res1 = udaf.aggregate(123, res0);

    // Then:
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

    // Then:
    assertThat(result, hasSize(2));
    assertThat(result.get(0).get(VAL_FIELD), is(3));
    assertThat(result.get(1).get(VAL_FIELD), is(2));
  }

  @Test
  public void shouldMerge() {
    // Given:
    final Udaf<Integer, Struct, Integer> udaf = LatestByOffset.latestInteger();

    final Struct agg1 = LatestByOffset.createStruct(STRUCT_INTEGER, 123);
    final Struct agg2 = LatestByOffset.createStruct(STRUCT_INTEGER, 321);

    // When:
    final Struct merged1 = udaf.merge(agg1, agg2);
    final Struct merged2 = udaf.merge(agg2, agg1);

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

    final Struct agg1 = LatestByOffset.createStruct(STRUCT_INTEGER, 123);
    final Struct agg2 = LatestByOffset.createStruct(STRUCT_INTEGER, 321);

    // When:
    final Struct merged1 = udaf.merge(agg1, agg2);
    final Struct merged2 = udaf.merge(agg2, agg1);

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

    final Struct struct1 = LatestByOffset.createStruct(STRUCT_INTEGER, 123);
    final Struct struct2 = LatestByOffset.createStruct(STRUCT_INTEGER, 321);
    final Struct struct3 = LatestByOffset.createStruct(STRUCT_INTEGER, 543);
    final Struct struct4 = LatestByOffset.createStruct(STRUCT_INTEGER, 654);

    final List<Struct> agg1 = Lists.newArrayList(struct1, struct2);
    final List<Struct> agg2 = Lists.newArrayList(struct3, struct4);

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
  public void shouldMergeIgnoringNulls() {
    // Given:
    final Udaf<Integer, Struct, Integer> udaf = LatestByOffset.latestInteger();

    final Struct agg1 = LatestByOffset.createStruct(STRUCT_INTEGER, 123);
    final Struct agg2 = LatestByOffset.createStruct(STRUCT_INTEGER, null);

    // When:
    final Struct merged1 = udaf.merge(agg1, agg2);
    final Struct merged2 = udaf.merge(agg2, agg1);

    // Then:
    assertThat(merged1, is(agg1));
    assertThat(merged2, is(agg1));
  }

  @Test
  public void shouldComputeLatestLong() {
    // Given:
    final Udaf<Long, Struct, Long> udaf = LatestByOffset.latestLong();

    // When:
    final Struct res = udaf.aggregate(123L, LatestByOffset.createStruct(STRUCT_LONG, 321L));

    // Then:
    assertThat(res.getInt64(VAL_FIELD), is(123L));
  }

  @Test
  public void shouldComputeLatestNLongs() {
    // Given:
    final Udaf<Long, List<Struct>, List<Long>> udaf = LatestByOffset.latestLongs(3);

    // When:
    final List<Struct> res = udaf
        .aggregate(123L, Lists.newArrayList(LatestByOffset.createStruct(STRUCT_LONG, 321L)));

    final List<Struct> res2 = udaf
        .aggregate(543L, res);

    final List<Struct> res3 = udaf
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
    final Struct res = udaf.aggregate(1.1d, LatestByOffset.createStruct(STRUCT_DOUBLE, 2.2d));

    // Then:
    assertThat(res.getFloat64(VAL_FIELD), is(1.1d));
  }

  @Test
  public void shouldComputeLatestNDoubles() {
    // Given:
    final Udaf<Double, List<Struct>, List<Double>> udaf = LatestByOffset.latestDoubles(1);

    // When:
    final List<Struct> res = udaf
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
    final Struct res = udaf.aggregate(true, LatestByOffset.createStruct(STRUCT_BOOLEAN, false));

    // Then:
    assertThat(res.getBoolean(VAL_FIELD), is(true));
  }

  @Test
  public void shouldComputeLatestNBooleans() {
    // Given:
    final Udaf<Boolean, List<Struct>, List<Boolean>> udaf = LatestByOffset.latestBooleans(2);

    // When:
    final List<Struct> res = udaf
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
    final Struct res = udaf.aggregate("foo", LatestByOffset.createStruct(STRUCT_STRING, "bar"));

    // Then:
    assertThat(res.getString(VAL_FIELD), is("foo"));
  }

  @Test
  public void shouldComputeLatestNStrings() {
    // Given:
    final Udaf<String, List<Struct>, List<String>> udaf = LatestByOffset.latestStrings(3);

    // When:
    final List<Struct> res = udaf.aggregate("boo",
        Lists.newArrayList(LatestByOffset.createStruct(STRUCT_STRING, "foo"),
            LatestByOffset.createStruct(STRUCT_STRING, "bar"),
            LatestByOffset.createStruct(STRUCT_STRING, "baz")));

    // Then:
    assertThat(res.size(), is(3));
    assertThat(res.get(0).get(VAL_FIELD), is("bar"));
    assertThat(res.get(1).get(VAL_FIELD), is("baz"));
    assertThat(res.get(2).get(VAL_FIELD), is("boo"));
  }

  @Test
  public void shouldComputeLatestTimestamp() {
    // Given:
    final Udaf<Timestamp, Struct, Timestamp> udaf = LatestByOffset.latestTimestamp();

    // When:
    final Struct res = udaf.aggregate(new Timestamp(123), LatestByOffset.createStruct(STRUCT_TIMESTAMP, new Timestamp(456)));

    // Then:
    assertThat(res.get(VAL_FIELD), is(new Timestamp(123)));
  }

  @Test
  public void shouldComputeLatestNTimestamps() {
    // Given:
    final Udaf<Timestamp, List<Struct>, List<Timestamp>> udaf = LatestByOffset.latestTimestamps(3);

    // When:
    final List<Struct> res = udaf.aggregate(new Timestamp(123),
        Lists.newArrayList(LatestByOffset.createStruct(STRUCT_TIMESTAMP, new Timestamp(1)),
            LatestByOffset.createStruct(STRUCT_TIMESTAMP, new Timestamp(2)),
            LatestByOffset.createStruct(STRUCT_TIMESTAMP, new Timestamp(3))));

    // Then:
    assertThat(res.size(), is(3));
    assertThat(res.get(0).get(VAL_FIELD), is(new Timestamp(2)));
    assertThat(res.get(1).get(VAL_FIELD), is(new Timestamp(3)));
    assertThat(res.get(2).get(VAL_FIELD), is(new Timestamp(123)));
  }

  @Test
  public void shouldComputeLatestTime() {
    // Given:
    final Udaf<Time, Struct, Time> udaf = LatestByOffset.latestTime();

    // When:
    final Struct res = udaf.aggregate(new Time(1), LatestByOffset.createStruct(STRUCT_TIME, new Time(5)));

    // Then:
    assertThat(res.get(VAL_FIELD), is(new Time(1)));
  }

  @Test
  public void shouldComputeLatestNTimes() {
    // Given:
    final Udaf<Time, List<Struct>, List<Time>> udaf = LatestByOffset.latestTimes(3);

    // When:
    final List<Struct> res = udaf.aggregate(new Time(3),
        Lists.newArrayList(LatestByOffset.createStruct(STRUCT_TIME, new Time(6)),
            LatestByOffset.createStruct(STRUCT_TIME, new Time(5)),
            LatestByOffset.createStruct(STRUCT_TIME, new Time(4))));

    // Then:
    assertThat(res.size(), is(3));
    assertThat(res.get(0).get(VAL_FIELD), is(new Time(5)));
    assertThat(res.get(1).get(VAL_FIELD), is(new Time(4)));
    assertThat(res.get(2).get(VAL_FIELD), is(new Time(3)));
  }

  @Test
  public void shouldComputeLatestDate() {
    // Given:
    final Udaf<Date, Struct, Date> udaf = LatestByOffset.latestDate();

    // When:
    final Struct res = udaf.aggregate(new Date(123), LatestByOffset.createStruct(STRUCT_DATE, new Date(456)));

    // Then:
    assertThat(res.get(VAL_FIELD), is(new Date(123)));
  }

  @Test
  public void shouldComputeLatestNDates() {
    // Given:
    final Udaf<Date, List<Struct>, List<Date>> udaf = LatestByOffset.latestDates(3);

    // When:
    final List<Struct> res = udaf.aggregate(new Date(123),
        Lists.newArrayList(LatestByOffset.createStruct(STRUCT_DATE, new Date(1)),
            LatestByOffset.createStruct(STRUCT_DATE, new Date(2)),
            LatestByOffset.createStruct(STRUCT_DATE, new Date(3))));

    // Then:
    assertThat(res.size(), is(3));
    assertThat(res.get(0).get(VAL_FIELD), is(new Date(2)));
    assertThat(res.get(1).get(VAL_FIELD), is(new Date(3)));
    assertThat(res.get(2).get(VAL_FIELD), is(new Date(123)));
  }

  @Test
  public void shouldComputeLatestBytes() {
    // Given:
    final Udaf<ByteBuffer, Struct, ByteBuffer> udaf = LatestByOffset.latestBytes();

    // When:
    final Struct res = udaf.aggregate(
        ByteBuffer.wrap(new byte[] { 123 }),
        LatestByOffset.createStruct(STRUCT_BYTES, ByteBuffer.wrap(new byte[] { 23 })));

    // Then:
    assertThat(res.get(VAL_FIELD), is(ByteBuffer.wrap(new byte[] { 123 })));
  }

  @Test
  public void shouldComputeLatestNBytes() {
    // Given:
    final Udaf<ByteBuffer, List<Struct>, List<ByteBuffer>> udaf = LatestByOffset.latestBytes(3);

    // When:
    final List<Struct> res = udaf.aggregate(ByteBuffer.wrap(new byte[] { 123 }),
        Lists.newArrayList(LatestByOffset.createStruct(STRUCT_BYTES, ByteBuffer.wrap(new byte[] { 1 })),
            LatestByOffset.createStruct(STRUCT_BYTES, ByteBuffer.wrap(new byte[] { 2 })),
            LatestByOffset.createStruct(STRUCT_BYTES, ByteBuffer.wrap(new byte[] { 3 }))));

    // Then:
    assertThat(res.size(), is(3));
    assertThat(res.get(0).get(VAL_FIELD), is(ByteBuffer.wrap(new byte[] { 2 })));
    assertThat(res.get(1).get(VAL_FIELD), is(ByteBuffer.wrap(new byte[] { 3 })));
    assertThat(res.get(2).get(VAL_FIELD), is(ByteBuffer.wrap(new byte[] { 123 })));
  }

  @Test
  public void shouldNotAcceptFirstNullAsLatest() {
    // Given:
    final Udaf<String, Struct, String> udaf = LatestByOffset.latestString(true);

    // When:
    final Struct res = udaf
        .aggregate(null, udaf.initialize());

    // Then:
    assertThat(res.getString(VAL_FIELD), is(nullValue()));

    // When:
    final Struct res2 = udaf
        .aggregate("value", res);

    // Then:
    assertThat(res2.getString(VAL_FIELD), is("value"));
  }

  @Test
  public void shouldAcceptFirstNullAsLatest() {
    // Given:
    final Udaf<String, Struct, String> udaf = LatestByOffset.latestString(false);

    // When:
    final Struct res = udaf
        .aggregate(null, udaf.initialize());

    // Then:
    assertThat(res.getString(VAL_FIELD), is(nullValue()));
  }

  @Test
  public void shouldNotAcceptFirstNullAsLatestN() {
    // Given:
    final Udaf<String, List<Struct>, List<String>> udaf = LatestByOffset
        .latestStrings(2, true);

    // When:
    final List<Struct> res = udaf
        .aggregate(null, udaf.initialize());

    // Then:
    assertThat(res, is(empty()));
  }

  @Test
  public void shouldAcceptFirstNullAsLatestN() {
    // Given:
    final Udaf<String, List<Struct>, List<String>> udaf = LatestByOffset
        .latestStrings(2, false);

    // When:
    final List<Struct> res = udaf
        .aggregate(null, udaf.initialize());

    // Then:
    assertThat(res, hasSize(1));
    assertThat(res.get(0).getString(VAL_FIELD), is(nullValue()));
  }

  @Test
  public void shouldNotAcceptSubsequentNullAsLatest() {
    // Given:
    final Udaf<String, Struct, String> udaf = LatestByOffset.latestString(true);

    final Struct aggregate = udaf.aggregate("value", udaf.initialize());

    // When:
    final Struct res = udaf
        .aggregate(null, aggregate);

    // Then:
    assertThat(res.getString(VAL_FIELD), is("value"));
  }

  @Test
  public void shouldAcceptSubsequentNullAsLatest() {
    // Given:
    final Udaf<String, Struct, String> udaf = LatestByOffset.latestString(false);

    final Struct aggregate = udaf.aggregate("value", udaf.initialize());

    // When:
    final Struct res = udaf
        .aggregate(null, aggregate);

    // Then:
    assertThat(res.getString(VAL_FIELD), is(nullValue()));
  }

  @Test
  public void shouldNotAcceptSubsequentNullAsLatestN() {
    // Given:
    final Udaf<String, List<Struct>, List<String>> udaf = LatestByOffset
        .latestStrings(2, true);

    final List<Struct> aggregate = udaf.aggregate("bar", udaf.initialize());

    // When:
    final List<Struct> res = udaf.aggregate(null, aggregate);

    // Then:
    assertThat(res, hasSize(1));
    assertThat(res.get(0).getString(VAL_FIELD), is("bar"));
  }

  @Test
  public void shouldAcceptSubsequentNullAsLatestN() {
    // Given:
    final Udaf<String, List<Struct>, List<String>> udaf = LatestByOffset
        .latestStrings(2, false);

    final List<Struct> aggregate = udaf.aggregate("bar", udaf.initialize());

    // When:
    final List<Struct> res = udaf.aggregate(null, aggregate);

    // Then:
    assertThat(res, hasSize(2));
    assertThat(res.get(0).getString(VAL_FIELD), is("bar"));
    assertThat(res.get(1).getString(VAL_FIELD), is(nullValue()));
  }

  @Test
  public void shouldMapInitialized() {
    // Given:
    final Udaf<String, Struct, String> udaf = LatestByOffset.latestString();

    final Struct init = udaf.initialize();

    // When:
    final String result = udaf.map(init);

    // Then:
    assertThat(result, is(nullValue()));
  }

  @Test
  public void shouldMergeAndMapInitialized() {
    // Given:
    final Udaf<String, Struct, String> udaf = LatestByOffset.latestString();

    final Struct init1 = udaf.initialize();
    final Struct init2 = udaf.initialize();

    // When:
    final Struct merged = udaf.merge(init1, init2);
    final String result = udaf.map(merged);

    // Then:
    assertThat(result, is(nullValue()));
  }

  @Test
  public void shouldMapInitializedN() {
    // Given:
    final Udaf<String, List<Struct>, List<String>> udaf = LatestByOffset.latestStrings(2);

    final List<Struct> init = udaf.initialize();

    // When:
    final List<String> result = udaf.map(init);

    // Then:
    assertThat(result, is(empty()));
  }

  @Test
  public void shouldMergeAndMapInitializedN() {
    // Given:
    final Udaf<String, List<Struct>, List<String>> udaf = LatestByOffset.latestStrings(2);

    final List<Struct> init1 = udaf.initialize();
    final List<Struct> init2 = udaf.initialize();

    // When:
    final List<Struct> merged = udaf.merge(init1, init2);
    final List<String> result = udaf.map(merged);

    // Then:
    assertThat(result, is(empty()));
  }
}
