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
import static io.confluent.ksql.function.udaf.offset.KudafByOffsetUtils.STRUCT_INTEGER;
import static io.confluent.ksql.function.udaf.offset.KudafByOffsetUtils.VAL_FIELD;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

import com.google.common.collect.Lists;
import io.confluent.ksql.function.udaf.Udaf;
import io.confluent.ksql.schema.ksql.SqlArgument;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.util.KsqlException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.kafka.connect.data.Struct;
import org.junit.Test;


public class EarliestByOffsetTest {

  @Test
  public void shouldInitialize() {
    // Given:
    final Udaf<Integer, Struct, Integer> udaf = EarliestByOffset
        .earliestT(true);

    // When:
    final Struct init = udaf.initialize();

    // Then:
    assertThat(init, is(nullValue()));
  }

  @Test
  public void shouldInitializeN() {
    // Given:
    final Udaf<Integer, List<Struct>, List<Integer>> udaf = EarliestByOffset
        .earliestTN( 2, false);

    // When:
    final List<Struct> init = udaf.initialize();

    // Then:
    assertThat(init, is(empty()));
  }
  
  @Test
  public void shouldThrowExceptionForInvalidN() {
    try {
      EarliestByOffset.earliestTN( -1, true);
    } catch (KsqlException e) {
      assertThat(e.getMessage(), is("earliestN must be 1 or greater"));
    }
  }

  @Test
  public void shouldComputeEarliestInteger() {
    // Given:
    final Udaf<Integer, Struct, Integer> udaf = EarliestByOffset.earliest();
    udaf.initializeTypeArguments(Collections.singletonList(SqlArgument.of(SqlTypes.INTEGER)));

    // When:
    final Struct res = udaf.aggregate(123, EarliestByOffset.createStruct(STRUCT_INTEGER, 321));

    // Then:
    assertThat(res.get(VAL_FIELD), is(321));
  }
  
  @Test
  public void shouldComputeEarliestNIntegers() {
    // Given:
    final Udaf<Integer, List<Struct>, List<Integer>> udaf = EarliestByOffset.earliest(2);
    udaf.initializeTypeArguments(Collections.singletonList(SqlArgument.of(SqlTypes.INTEGER)));

    // When:
    final List<Struct> res = udaf
        .aggregate(123, Lists.newArrayList(EarliestByOffset.createStruct(STRUCT_INTEGER, 321)));

    // Then:
    assertThat(res.get(0).get(VAL_FIELD), is(321));
    assertThat(res.get(1).get(VAL_FIELD), is(123));
  }

  @Test
  public void shouldCaptureValuesUpToN() {
    // Given:
    final Udaf<Integer, List<Struct>, List<Integer>> udaf = EarliestByOffset.earliest(2);
    udaf.initializeTypeArguments(Collections.singletonList(SqlArgument.of(SqlTypes.INTEGER)));

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
    final Udaf<Integer, List<Struct>, List<Integer>> udaf = EarliestByOffset.earliest(2);
    udaf.initializeTypeArguments(Collections.singletonList(SqlArgument.of(SqlTypes.INTEGER)));

    final List<Struct> aggregate = Lists.newArrayList(
        EarliestByOffset.createStruct(STRUCT_INTEGER, 10),
        EarliestByOffset.createStruct(STRUCT_INTEGER, 3)
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
    final Udaf<Integer, Struct, Integer> udaf = EarliestByOffset.earliest();
    udaf.initializeTypeArguments(Collections.singletonList(SqlArgument.of(SqlTypes.INTEGER)));

    final Struct agg1 = EarliestByOffset.createStruct(STRUCT_INTEGER, 123);
    final Struct agg2 = EarliestByOffset.createStruct(STRUCT_INTEGER, 321);

    // When:
    final Struct merged1 = udaf.merge(agg1, agg2);
    final Struct merged2 = udaf.merge(agg2, agg1);

    // Then:
    assertThat(merged1, is(agg1));
    assertThat(merged2, is(agg1));
  }

  @Test
  public void shouldMergeNIntegers() {
    // Given:
    final Udaf<Integer, List<Struct>, List<Integer>> udaf = EarliestByOffset.earliest(2);
    udaf.initializeTypeArguments(Collections.singletonList(SqlArgument.of(SqlTypes.INTEGER)));
    final Struct struct1 = EarliestByOffset.createStruct(STRUCT_INTEGER, 123);
    final Struct struct2 = EarliestByOffset.createStruct(STRUCT_INTEGER, 321);
    final Struct struct3 = EarliestByOffset.createStruct(STRUCT_INTEGER, 543);
    final Struct struct4 = EarliestByOffset.createStruct(STRUCT_INTEGER, 654);
    final List<Struct> agg1 = new ArrayList<>(Lists.newArrayList(struct1, struct4));
    final List<Struct> agg2 = new ArrayList<>(Lists.newArrayList(struct2, struct3));

    // When:
    final List<Struct> merged1 = udaf.merge(agg1, agg2);
    final List<Struct> merged2 = udaf.merge(agg2, agg1);

    // Then:
    assertThat(merged1, contains(struct1, struct2));
    assertThat(merged2, contains(struct1, struct2));
  }

  @Test
  public void shouldMergeNIntegersSmallerThanN() {
    // Given:
    final Udaf<Integer, List<Struct>, List<Integer>> udaf = EarliestByOffset.earliest(5);
    udaf.initializeTypeArguments(Collections.singletonList(SqlArgument.of(SqlTypes.INTEGER)));
    final Struct struct1 = EarliestByOffset.createStruct(STRUCT_INTEGER, 123);
    final Struct struct2 = EarliestByOffset.createStruct(STRUCT_INTEGER, 321);
    final Struct struct3 = EarliestByOffset.createStruct(STRUCT_INTEGER, 543);
    final Struct struct4 = EarliestByOffset.createStruct(STRUCT_INTEGER, 654);
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
    final Udaf<Integer, Struct, Integer> udaf = EarliestByOffset.earliest();
    udaf.initializeTypeArguments(Collections.singletonList(SqlArgument.of(SqlTypes.INTEGER)));

    EarliestByOffset.sequence.set(Long.MAX_VALUE);

    final Struct agg1 = EarliestByOffset.createStruct(STRUCT_INTEGER, 123);
    final Struct agg2 = EarliestByOffset.createStruct(STRUCT_INTEGER, 321);

    // When:
    final Struct merged1 = udaf.merge(agg1, agg2);
    final Struct merged2 = udaf.merge(agg2, agg1);

    // Then:
    assertThat(agg1.getInt64(SEQ_FIELD), is(Long.MAX_VALUE));
    assertThat(agg2.getInt64(SEQ_FIELD), is(Long.MIN_VALUE));
    assertThat(merged1, is(agg1));
    assertThat(merged2, is(agg1));
  }

  @Test
  public void shouldMergeWithOverflowNIntegers() {
    // Given:
    final Udaf<Integer, List<Struct>, List<Integer>> udaf = EarliestByOffset.earliest(2);
    udaf.initializeTypeArguments(Collections.singletonList(SqlArgument.of(SqlTypes.INTEGER)));

    EarliestByOffset.sequence.set(Long.MAX_VALUE - 1);

    final Struct struct1 = EarliestByOffset.createStruct(STRUCT_INTEGER, 123);
    final Struct struct2 = EarliestByOffset.createStruct(STRUCT_INTEGER, 321);
    final Struct struct3 = EarliestByOffset.createStruct(STRUCT_INTEGER, 543);
    final Struct struct4 = EarliestByOffset.createStruct(STRUCT_INTEGER, 654);

    final List<Struct> agg1 = Lists.newArrayList(struct1, struct2);
    final List<Struct> agg2 = Lists.newArrayList(struct3, struct4);

    // When:
    final List<Struct> merged1 = udaf.merge(agg1, agg2);
    final List<Struct> merged2 = udaf.merge(agg2, agg1);

    // Then:
    assertThat(agg1.get(0).getInt64(SEQ_FIELD), is(Long.MAX_VALUE - 1));
    assertThat(agg2.get(0).getInt64(SEQ_FIELD), is(Long.MIN_VALUE));
    assertThat(merged1, contains(struct1, struct2));
    assertThat(merged2, contains(struct1, struct2));
  }
}
