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
import static org.hamcrest.Matchers.nullValue;

import com.google.common.collect.Lists;
import io.confluent.ksql.GenericKey;
import io.confluent.ksql.function.AggregateFunctionInitArguments;
import io.confluent.ksql.function.KsqlAggregateFunction;
import io.confluent.ksql.schema.ksql.SqlArgument;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import java.nio.ByteBuffer;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.kafka.connect.data.Struct;
import org.junit.Test;


public class EarliestByOffsetTest {
  final private EarliestByOffsetFactory factory = new EarliestByOffsetFactory();
  final private GenericKey key = GenericKey.builder(1).append("key").build();

  @Test
  public void shouldInitialize() {
    // Given:
    final KsqlAggregateFunction<Integer, Struct, Integer> udaf = earliestInteger();

    // When:
    final Struct init = udaf.getInitialValueSupplier().get();

    // Then:
    assertThat(init, is(nullValue()));
  }

  @Test
  public void shouldInitializeN() {
    // Given:
    final KsqlAggregateFunction<Integer, List<Struct>, List<Integer>> udaf = earliestIntegers(2);

    // When:
    final List<Struct> init = udaf.getInitialValueSupplier().get();

    // Then:
    assertThat(init, is(empty()));
  }
  
  @Test
  public void shouldThrowExceptionForInvalidN() {
    try {
      earliestIntegers(-1);
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage(), is("For EarliestByOffset, n must be positive.  It was -1."));
    }
  }

  @Test
  public void shouldComputeEarliestInteger() {
    // Given:
    final KsqlAggregateFunction<Integer, Struct, Integer> udaf = earliestInteger();

    // When:
    final Struct res = udaf.aggregate(123, KudafByOffsetUtils.createStruct(STRUCT_INTEGER, 321));

    // Then:
    assertThat(res.get(VAL_FIELD), is(321));
  }
  
  @Test
  public void shouldComputeEarliestNIntegers() {
    // Given:
    final KsqlAggregateFunction<Integer, List<Struct>, List<Integer>> udaf = earliestIntegers(2);

    // When:
    final List<Struct> res = udaf
        .aggregate(123, Lists.newArrayList(KudafByOffsetUtils.createStruct(STRUCT_INTEGER, 321)));

    // Then:
    assertThat(res.get(0).get(VAL_FIELD), is(321));
    assertThat(res.get(1).get(VAL_FIELD), is(123));
  }

  @Test
  public void shouldCaptureValuesUpToN() {
    // Given:
    final KsqlAggregateFunction<Integer, List<Struct>, List<Integer>> udaf = earliestIntegers(2);

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
    final KsqlAggregateFunction<Integer, List<Struct>, List<Integer>> udaf = earliestIntegers(2);
    final List<Struct> aggregate = Lists.newArrayList(
        KudafByOffsetUtils.createStruct(STRUCT_INTEGER, 10),
        KudafByOffsetUtils.createStruct(STRUCT_INTEGER, 3)
    );

    // When:
    final List<Struct> result = udaf.aggregate(2, aggregate);

    // Then:
    assertThat(result, hasSize(2));
    assertThat(result.get(0).get(VAL_FIELD), is(10));
    assertThat(result.get(1).get(VAL_FIELD), is(3));
  }

  @Test
  public void shouldMerge() {
    // Given:
    final KsqlAggregateFunction<Integer, Struct, Integer> udaf = earliestInteger();

    final Struct agg1 = KudafByOffsetUtils.createStruct(STRUCT_INTEGER, 123);
    final Struct agg2 = KudafByOffsetUtils.createStruct(STRUCT_INTEGER, 321);

    // When:
    final Struct merged1 = udaf.getMerger().apply(key, agg1, agg2);
    final Struct merged2 = udaf.getMerger().apply(key, agg2, agg1);

    // Then:
    assertThat(merged1, is(agg1));
    assertThat(merged2, is(agg1));
  }

  @Test
  public void shouldMergeNIntegers() {
    // Given:
    final KsqlAggregateFunction<Integer, List<Struct>, List<Integer>> udaf = earliestIntegers(2);
    final Struct struct1 = KudafByOffsetUtils.createStruct(STRUCT_INTEGER, 123);
    final Struct struct2 = KudafByOffsetUtils.createStruct(STRUCT_INTEGER, 321);
    final Struct struct3 = KudafByOffsetUtils.createStruct(STRUCT_INTEGER, 543);
    final Struct struct4 = KudafByOffsetUtils.createStruct(STRUCT_INTEGER, 654);
    final List<Struct> agg1 = new ArrayList<>(Lists.newArrayList(struct1, struct4));
    final List<Struct> agg2 = new ArrayList<>(Lists.newArrayList(struct2, struct3));

    // When:
    final List<Struct> merged1 = udaf.getMerger().apply(key, agg1, agg2);
    final List<Struct> merged2 = udaf.getMerger().apply(key, agg2, agg1);

    // Then:
    assertThat(merged1, contains(struct1, struct2));
    assertThat(merged2, contains(struct1, struct2));
  }

  @Test
  public void shouldMergeNIntegersSmallerThanN() {
    // Given:
    final KsqlAggregateFunction<Integer, List<Struct>, List<Integer>> udaf = earliestIntegers(5);
    final Struct struct1 = KudafByOffsetUtils.createStruct(STRUCT_INTEGER, 123);
    final Struct struct2 = KudafByOffsetUtils.createStruct(STRUCT_INTEGER, 321);
    final Struct struct3 = KudafByOffsetUtils.createStruct(STRUCT_INTEGER, 543);
    final Struct struct4 = KudafByOffsetUtils.createStruct(STRUCT_INTEGER, 654);
    final List<Struct> agg1 = new ArrayList<>(Lists.newArrayList(struct1, struct4));
    final List<Struct> agg2 = new ArrayList<>(Lists.newArrayList(struct2, struct3));

    // When:
    final List<Struct> merged1 = udaf.getMerger().apply(key, agg1, agg2);
    final List<Struct> merged2 = udaf.getMerger().apply(key, agg2, agg1);

    // Then:
    assertThat(merged1, contains(struct1, struct2, struct3, struct4));
    assertThat(merged1.size(), is(4));
    assertThat(merged2, contains(struct1, struct2, struct3, struct4));
    assertThat(merged2.size(), is(4));
  }

  @Test
  public void shouldMergeWithOverflow() {
    // Given:
    final KsqlAggregateFunction<Integer, Struct, Integer> udaf = earliestInteger();

    KudafByOffsetUtils.sequence.set(Long.MAX_VALUE);

    final Struct agg1 = KudafByOffsetUtils.createStruct(STRUCT_INTEGER, 123);
    final Struct agg2 = KudafByOffsetUtils.createStruct(STRUCT_INTEGER, 321);

    // When:
    final Struct merged1 = udaf.getMerger().apply(key, agg1, agg2);
    final Struct merged2 = udaf.getMerger().apply(key, agg2, agg1);

    // Then:
    assertThat(agg1.getInt64(SEQ_FIELD), is(Long.MAX_VALUE));
    assertThat(agg2.getInt64(SEQ_FIELD), is(Long.MIN_VALUE));
    assertThat(merged1, is(agg1));
    assertThat(merged2, is(agg1));
  }

  @Test
  public void shouldMergeWithOverflowNIntegers() {
    // Given:
    final KsqlAggregateFunction<Integer, List<Struct>, List<Integer>> udaf = earliestIntegers(2);

    KudafByOffsetUtils.sequence.set(Long.MAX_VALUE - 1);

    final Struct struct1 = KudafByOffsetUtils.createStruct(STRUCT_INTEGER, 123);
    final Struct struct2 = KudafByOffsetUtils.createStruct(STRUCT_INTEGER, 321);
    final Struct struct3 = KudafByOffsetUtils.createStruct(STRUCT_INTEGER, 543);
    final Struct struct4 = KudafByOffsetUtils.createStruct(STRUCT_INTEGER, 654);

    final List<Struct> agg1 = Lists.newArrayList(struct1, struct2);
    final List<Struct> agg2 = Lists.newArrayList(struct3, struct4);

    // When:
    final List<Struct> merged1 = udaf.getMerger().apply(key, agg1, agg2);
    final List<Struct> merged2 = udaf.getMerger().apply(key, agg2, agg1);

    // Then:
    assertThat(agg1.get(0).getInt64(SEQ_FIELD), is(Long.MAX_VALUE - 1));
    assertThat(agg2.get(0).getInt64(SEQ_FIELD), is(Long.MIN_VALUE));
    assertThat(merged1, contains(struct1, struct2));
    assertThat(merged2, contains(struct1, struct2));
  }

  @Test
  public void shouldComputeEarliestLong() {
    // Given:
    final KsqlAggregateFunction<Long, Struct, Long> udaf = earliestLong();

    // When:
    final Struct res = udaf.aggregate(123L, KudafByOffsetUtils.createStruct(STRUCT_LONG, 321L));

    // Then:
    assertThat(res.getInt64(VAL_FIELD), is(321L));
  }

  @Test
  public void shouldComputeEarliestNLongs() {
    // Given:
    final KsqlAggregateFunction<Long, List<Struct>, List<Long>> udaf = earliestLongs(3);

    // When:
    final List<Struct> res = udaf
        .aggregate(123L, Lists.newArrayList(KudafByOffsetUtils.createStruct(STRUCT_LONG, 321L)));

    List<Struct> res2 = udaf
        .aggregate(543L, res);

    // Then:
    assertThat(res2.size(), is(3));
    assertThat(res2.get(0).get(VAL_FIELD), is(321L));
    assertThat(res2.get(1).get(VAL_FIELD), is(123L));
    assertThat(res2.get(2).get(VAL_FIELD), is(543L));
  }

  @Test
  public void shouldComputeEarliestDouble() {
    // Given:
    final KsqlAggregateFunction<Double, Struct, Double> udaf = earliestDouble();

    // When:
    final Struct res = udaf
        .aggregate(1.1d, KudafByOffsetUtils.createStruct(STRUCT_DOUBLE, 2.2d));

    // Then:
    assertThat(res.getFloat64(VAL_FIELD), is(2.2d));
  }

  @Test
  public void shouldComputeEarliestNDoubles() {
    // Given:
    final KsqlAggregateFunction<Double, List<Struct>, List<Double>> udaf = earliestDoubles(2);

    // When:
    final List<Struct> res = udaf
        .aggregate(1.1d, Lists.newArrayList(KudafByOffsetUtils.createStruct(STRUCT_DOUBLE, 2.2d)));

    // Then:
    assertThat(res.size(), is(2));
    assertThat(res.get(0).get(VAL_FIELD), is(2.2d));
  }

  @Test
  public void shouldComputeEarliestBoolean() {
    // Given:
    final KsqlAggregateFunction<Boolean, Struct, Boolean> udaf = earliestBoolean();

    // When:
    final Struct res = udaf
        .aggregate(true, KudafByOffsetUtils.createStruct(STRUCT_BOOLEAN, false));

    // Then:
    assertThat(res.getBoolean(VAL_FIELD), is(false));
  }

  @Test
  public void shouldComputeEarliestNBooleans() {
    // Given:
    final KsqlAggregateFunction<Boolean, List<Struct>, List<Boolean>> udaf = earliestBooleans(2);

    // When:
    final List<Struct> res = udaf
        .aggregate(true, Lists.newArrayList(KudafByOffsetUtils.createStruct(STRUCT_BOOLEAN, false)));

    // Then:
    assertThat(res.size(), is(2));
    assertThat(res.get(0).get(VAL_FIELD), is(false));
    assertThat(res.get(1).get(VAL_FIELD), is(true));
  }

  @Test
  public void shouldComputeEarliestTimestamp() {
    // Given:
    final KsqlAggregateFunction<Timestamp, Struct, Timestamp> udaf = earliestTimestamp();

    // When:
    final Struct res = udaf.aggregate(new Timestamp(123), KudafByOffsetUtils.createStruct(STRUCT_TIMESTAMP, new Timestamp(321)));

    // Then:
    assertThat(res.get(VAL_FIELD), is(new Timestamp(321)));
  }

  @Test
  public void shouldComputeEarliestNTimestamps() {
    // Given:
    final KsqlAggregateFunction<Timestamp, List<Struct>, List<Timestamp>> udaf = earliestTimestamps(3);

    // When:
    final List<Struct> res = udaf.aggregate(new Timestamp(123),
        Lists.newArrayList(KudafByOffsetUtils.createStruct(STRUCT_TIMESTAMP, new Timestamp(321)),
            KudafByOffsetUtils.createStruct(STRUCT_TIMESTAMP, new Timestamp(456)),
            KudafByOffsetUtils.createStruct(STRUCT_TIMESTAMP, new Timestamp(654))));

    // Then:
    assertThat(res.size(), is(3));
    assertThat(res.get(0).get(VAL_FIELD), is(new Timestamp(321)));
    assertThat(res.get(1).get(VAL_FIELD), is(new Timestamp(456)));
    assertThat(res.get(2).get(VAL_FIELD), is(new Timestamp(654)));
  }

  @Test
  public void shouldComputeEarliestDate() {
    // Given:
    final KsqlAggregateFunction<Date, Struct, Date> udaf = earliestDate();

    // When:
    final Struct res = udaf.aggregate(new Date(123), KudafByOffsetUtils.createStruct(STRUCT_DATE, new Date(321)));

    // Then:
    assertThat(res.get(VAL_FIELD), is(new Date(321)));
  }

  @Test
  public void shouldComputeEarliestNDates() {
    // Given:
    final KsqlAggregateFunction<Date, List<Struct>, List<Date>> udaf = earliestDates(3);

    // When:
    final List<Struct> res = udaf.aggregate(new Date(123),
        Lists.newArrayList(KudafByOffsetUtils.createStruct(STRUCT_DATE, new Date(321)),
            KudafByOffsetUtils.createStruct(STRUCT_DATE, new Date(456)),
            KudafByOffsetUtils.createStruct(STRUCT_DATE, new Date(654))));

    // Then:
    assertThat(res.size(), is(3));
    assertThat(res.get(0).get(VAL_FIELD), is(new Date(321)));
    assertThat(res.get(1).get(VAL_FIELD), is(new Date(456)));
    assertThat(res.get(2).get(VAL_FIELD), is(new Date(654)));
  }

  @Test
  public void shouldComputeEarliestTime() {
    // Given:
    final KsqlAggregateFunction<Time, Struct, Time> udaf = earliestTime();

    // When:
    final Struct res = udaf.aggregate(new Time(123), KudafByOffsetUtils.createStruct(STRUCT_TIME, new Time(321)));

    // Then:
    assertThat(res.get(VAL_FIELD), is(new Time(321)));
  }

  @Test
  public void shouldComputeEarliestNTimes() {
    // Given:
    final KsqlAggregateFunction<Time, List<Struct>, List<Time>> udaf = earliestTimes(3);

    // When:
    final List<Struct> res = udaf.aggregate(new Time(123),
        Lists.newArrayList(KudafByOffsetUtils.createStruct(STRUCT_TIME, new Time(321)),
            KudafByOffsetUtils.createStruct(STRUCT_TIME, new Time(456)),
            KudafByOffsetUtils.createStruct(STRUCT_TIME, new Time(654))));

    // Then:
    assertThat(res.size(), is(3));
    assertThat(res.get(0).get(VAL_FIELD), is(new Time(321)));
    assertThat(res.get(1).get(VAL_FIELD), is(new Time(456)));
    assertThat(res.get(2).get(VAL_FIELD), is(new Time(654)));
  }

  @Test
  public void shouldComputeEarliestBytes() {
    // Given:
    final KsqlAggregateFunction<Timestamp, Struct, Timestamp> udaf = earliestTimestamp();

    // When:
    final Struct res = udaf.aggregate(new Timestamp(123), KudafByOffsetUtils.createStruct(STRUCT_TIMESTAMP, new Timestamp(321)));

    // Then:
    assertThat(res.get(VAL_FIELD), is(new Timestamp(321)));
  }

  @Test
  public void shouldComputeEarliestNBytes() {
    // Given:
    final KsqlAggregateFunction<ByteBuffer, List<Struct>, List<ByteBuffer>> udaf = earliestBytes(3);

    // When:
    final List<Struct> res = udaf.aggregate(ByteBuffer.wrap(new byte[] { 123 }),
        Lists.newArrayList(KudafByOffsetUtils.createStruct(STRUCT_BYTES, ByteBuffer.wrap(new byte[] { 1 })),
            KudafByOffsetUtils.createStruct(STRUCT_BYTES, ByteBuffer.wrap(new byte[] { 2 })),
            KudafByOffsetUtils.createStruct(STRUCT_BYTES, ByteBuffer.wrap(new byte[] { 3 }))));

    // Then:
    assertThat(res.size(), is(3));
    assertThat(res.get(0).get(VAL_FIELD), is(ByteBuffer.wrap(new byte[] { 1 })));
    assertThat(res.get(1).get(VAL_FIELD), is(ByteBuffer.wrap(new byte[] { 2 })));
    assertThat(res.get(2).get(VAL_FIELD), is(ByteBuffer.wrap(new byte[] { 3 })));
  }

  @Test
  public void shouldComputeEarliestString() {
    // Given:
    final KsqlAggregateFunction<String, Struct, String> udaf = earliestString();

    // When:
    final Struct res = udaf.aggregate("foo", KudafByOffsetUtils.createStruct(STRUCT_STRING, "bar"));

    // Then:
    assertThat(res.getString(VAL_FIELD), is("bar"));
  }

  @Test
  public void shouldComputeEarliestNStrings() {
    // Given:
    final KsqlAggregateFunction<String, List<Struct>, List<String>> udaf = earliestStrings(3);

    // When:
    final List<Struct> res = udaf.aggregate("boo",
        Lists.newArrayList(KudafByOffsetUtils.createStruct(STRUCT_STRING, "foo"),
            KudafByOffsetUtils.createStruct(STRUCT_STRING, "bar"),
            KudafByOffsetUtils.createStruct(STRUCT_STRING, "baz")));

    // Then:
    assertThat(res.size(), is(3));
    assertThat(res.get(0).get(VAL_FIELD), is("foo"));
    assertThat(res.get(1).get(VAL_FIELD), is("bar"));
    assertThat(res.get(2).get(VAL_FIELD), is("baz"));
  }

  @Test
  public void shouldNotAcceptNullAsEarliest() {
    // Given:
    final KsqlAggregateFunction<String, Struct, String> udaf = earliestString();

    // When:
    final Struct res = udaf
        .aggregate(null, udaf.getInitialValueSupplier().get());

    // Then:
    assertThat(res, is(nullValue()));

    // When:
    final Struct res2 = udaf
        .aggregate("value", res);

    // Then:
    assertThat(res2.getString(VAL_FIELD), is("value"));
  }

  @Test
  public void shouldAcceptNullAsEarliest() {
    // Given:
    final KsqlAggregateFunction<String, Struct, String> udaf = earliestString(false);

    // When:
    final Struct res = udaf
        .aggregate(null, udaf.getInitialValueSupplier().get());

    // Then:
    assertThat(res.getString(VAL_FIELD), is(nullValue()));

    // When:
    final Struct res2 = udaf
        .aggregate("value", res);

    // Then:
    assertThat(res2.getString(VAL_FIELD), is(nullValue()));
  }

  @Test
  public void shouldNotAcceptNullAsEarliestN() {
    // Given:
    final KsqlAggregateFunction<String, List<Struct>, List<String>> udaf = earliestStrings(2);

    // When:
    final List<Struct> res = udaf
        .aggregate(null, udaf.getInitialValueSupplier().get());

    // Then:
    assertThat(res, is(empty()));
  }

  @Test
  public void shouldAcceptNullAsEarliestN() {
    // Given:
    final KsqlAggregateFunction<String, List<Struct>, List<String>> udaf = earliestStrings(2,
        false);

    // When:
    final List<Struct> res = udaf
        .aggregate(null, udaf.getInitialValueSupplier().get());

    // Then:
    assertThat(res, hasSize(1));
    assertThat(res.get(0).getString(VAL_FIELD), is(nullValue()));
  }

  @Test
  public void shouldMapInitialized() {
    // Given:
    final KsqlAggregateFunction<String, Struct, String> udaf = earliestString();

    final Struct init = udaf.getInitialValueSupplier().get();

    // When:
    final String result = udaf.getResultMapper().apply(init);

    // Then:
    assertThat(result, is(nullValue()));
  }

  @Test
  public void shouldMergeAndMapInitialized() {
    // Given:
    final KsqlAggregateFunction<String, Struct, String> udaf = earliestString();

    final Struct init1 = udaf.getInitialValueSupplier().get();
    final Struct init2 = udaf.getInitialValueSupplier().get();

    // When:
    final Struct merged = udaf.getMerger().apply(key, init1, init2);
    final String result = udaf.getResultMapper().apply(merged);

    // Then:
    assertThat(result, is(nullValue()));
  }

  @Test
  public void shouldMapInitializedN() {
    // Given:
    final KsqlAggregateFunction<String, List<Struct>, List<String>> udaf = earliestStrings(2);

    final List<Struct> init = udaf.getInitialValueSupplier().get();

    // When:
    final List<String> result = udaf.getResultMapper().apply(init);

    // Then:
    assertThat(result, is(empty()));
  }

  @Test
  public void shouldMergeAndMapInitializedN() {
    // Given:
    final KsqlAggregateFunction<String, List<Struct>, List<String>> udaf = earliestStrings(2);

    final List<Struct> init1 = udaf.getInitialValueSupplier().get();
    final List<Struct> init2 = udaf.getInitialValueSupplier().get();

    // When:
    final List<Struct> merged = udaf.getMerger().apply(key, init1, init2);
    final List<String> result = udaf.getResultMapper().apply(merged);

    // Then:
    assertThat(result, is(empty()));
  }

  private KsqlAggregateFunction<Boolean, Struct, Boolean> earliestBoolean() {
    return factory.createAggregateFunction(
        Collections.singletonList(SqlArgument.of(SqlTypes.BOOLEAN)),
        AggregateFunctionInitArguments.EMPTY_ARGS);
  }

  private KsqlAggregateFunction<Boolean, List<Struct>, List<Boolean>> earliestBooleans(int n) {
    final AggregateFunctionInitArguments args = new AggregateFunctionInitArguments(0, n);
    return factory.createAggregateFunction(
        Collections.singletonList(SqlArgument.of(SqlTypes.BOOLEAN)), args);
  }

  private KsqlAggregateFunction<ByteBuffer, List<Struct>, List<ByteBuffer>> earliestBytes(int n) {
    final AggregateFunctionInitArguments args = new AggregateFunctionInitArguments(0, n);
    return factory.createAggregateFunction(
        Collections.singletonList(SqlArgument.of(SqlTypes.BYTES)), args);
  }

  private KsqlAggregateFunction<Double, Struct, Double> earliestDouble() {
    return factory.createAggregateFunction(
        Collections.singletonList(SqlArgument.of(SqlTypes.DOUBLE)),
        AggregateFunctionInitArguments.EMPTY_ARGS);
  }

  private KsqlAggregateFunction<Date, Struct, Date> earliestDate() {
    return factory.createAggregateFunction(
        Collections.singletonList(SqlArgument.of(SqlTypes.DATE)),
        AggregateFunctionInitArguments.EMPTY_ARGS);
  }

  private KsqlAggregateFunction<Date, List<Struct>, List<Date>> earliestDates(int n) {
    final AggregateFunctionInitArguments args = new AggregateFunctionInitArguments(0, n);
    return factory.createAggregateFunction(
        Collections.singletonList(SqlArgument.of(SqlTypes.DATE)), args);
  }

  private KsqlAggregateFunction<Double, List<Struct>, List<Double>> earliestDoubles(int n) {
    final AggregateFunctionInitArguments args = new AggregateFunctionInitArguments(0, n);
    return factory.createAggregateFunction(
        Collections.singletonList(SqlArgument.of(SqlTypes.DOUBLE)), args);
  }

  private KsqlAggregateFunction<Integer, Struct, Integer> earliestInteger() {
    return factory.createAggregateFunction(
        Collections.singletonList(SqlArgument.of(SqlTypes.INTEGER)),
        AggregateFunctionInitArguments.EMPTY_ARGS);
  }

  private KsqlAggregateFunction<Integer, List<Struct>, List<Integer>> earliestIntegers(int n) {
    final AggregateFunctionInitArguments args = new AggregateFunctionInitArguments(0, n);
    return factory.createAggregateFunction(
        Collections.singletonList(SqlArgument.of(SqlTypes.INTEGER)), args);
  }

  private KsqlAggregateFunction<Long, Struct, Long> earliestLong() {
    return factory.createAggregateFunction(
        Collections.singletonList(SqlArgument.of(SqlTypes.BIGINT)),
        AggregateFunctionInitArguments.EMPTY_ARGS);
  }

  private KsqlAggregateFunction<Long, List<Struct>, List<Long>> earliestLongs(int n) {
    final AggregateFunctionInitArguments args = new AggregateFunctionInitArguments(0, n);
    return factory.createAggregateFunction(
        Collections.singletonList(SqlArgument.of(SqlTypes.BIGINT)), args);
  }

  private KsqlAggregateFunction<String, Struct, String> earliestString() {
    return factory.createAggregateFunction(
        Collections.singletonList(SqlArgument.of(SqlTypes.STRING)),
        AggregateFunctionInitArguments.EMPTY_ARGS);
  }

  private KsqlAggregateFunction<String, Struct, String> earliestString(final boolean ignoreNulls) {
    final AggregateFunctionInitArguments args = new AggregateFunctionInitArguments(0, ignoreNulls);
    return factory.createAggregateFunction(
        Collections.singletonList(SqlArgument.of(SqlTypes.STRING)), args);
  }

  private KsqlAggregateFunction<String, List<Struct>, List<String>> earliestStrings(int n) {
    final AggregateFunctionInitArguments args = new AggregateFunctionInitArguments(0, n);
    return factory.createAggregateFunction(
        Collections.singletonList(SqlArgument.of(SqlTypes.STRING)), args);
  }

  private KsqlAggregateFunction<String, List<Struct>, List<String>> earliestStrings(int n,
      boolean ignoreNulls) {
    final AggregateFunctionInitArguments args =
        new AggregateFunctionInitArguments(0, n, ignoreNulls);
    return factory.createAggregateFunction(
        Collections.singletonList(SqlArgument.of(SqlTypes.STRING)), args);
  }

  private KsqlAggregateFunction<Time, Struct, Time> earliestTime() {
    return factory.createAggregateFunction(
        Collections.singletonList(SqlArgument.of(SqlTypes.TIME)),
        AggregateFunctionInitArguments.EMPTY_ARGS);
  }

  private KsqlAggregateFunction<Time, List<Struct>, List<Time>> earliestTimes(int n) {
    final AggregateFunctionInitArguments args = new AggregateFunctionInitArguments(0, n);
    return factory.createAggregateFunction(
        Collections.singletonList(SqlArgument.of(SqlTypes.TIME)), args);
  }

  private KsqlAggregateFunction<Timestamp, Struct, Timestamp> earliestTimestamp() {
    return factory.createAggregateFunction(
        Collections.singletonList(SqlArgument.of(SqlTypes.TIMESTAMP)),
        AggregateFunctionInitArguments.EMPTY_ARGS);
  }

  private KsqlAggregateFunction<Timestamp, List<Struct>, List<Timestamp>> earliestTimestamps(int n) {
    final AggregateFunctionInitArguments args = new AggregateFunctionInitArguments(0, n);
    return factory.createAggregateFunction(
        Collections.singletonList(SqlArgument.of(SqlTypes.TIMESTAMP)), args);
  }
}
