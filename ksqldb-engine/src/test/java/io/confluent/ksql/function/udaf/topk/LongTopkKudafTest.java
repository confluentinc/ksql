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

package io.confluent.ksql.function.udaf.topk;

import static io.confluent.ksql.function.udaf.topk.TopKTestUtil.checkAggregate;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.function.udaf.Udaf;
import io.confluent.ksql.function.udaf.VariadicArgs;
import io.confluent.ksql.schema.ksql.SqlArgument;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.util.Pair;
import io.confluent.ksql.util.Triple;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.Test;

@SuppressWarnings("unchecked")
public class LongTopkKudafTest {
  private static final VariadicArgs<Object> EMPTY_VARARGS = new VariadicArgs<>(
          Collections.emptyList()
  );
  private static final Schema STRUCT_SCHEMA = SchemaBuilder.struct().optional()
          .field("sort_col", Schema.OPTIONAL_INT64_SCHEMA)
          .field("col0", Schema.OPTIONAL_BOOLEAN_SCHEMA)
          .field("col1", Schema.OPTIONAL_STRING_SCHEMA)
          .build();
  private final List<Long> valuesArray = ImmutableList.of(10L, 30L, 10L, 45L, 50L, 60L, 20L, 60L, 80L, 35L,
      25L);
  private final List<Triple<Long, Boolean, String>> valuesWithOtherColsArray = ImmutableList.of(
          Triple.of(10L,  true, "hello"),
          Triple.of(30L, false, "world"),
          Triple.of(45L, false, "topk"),
          Triple.of(10L, true, "test"),
          Triple.of(50L, false, "aggregate"),
          Triple.of(60L, true, "function"),
          Triple.of(20L, true, "udaf"),
          Triple.of(60L, true, "testing"),
          Triple.of(80L, false, "ksql"),
          Triple.of(35L, true, "value"),
          Triple.of(25L, false, "streaming")
  );

  @Test
  public void shouldAggregateTopK() {
    final Udaf<Pair<Long, VariadicArgs<Object>>, List<Long>, List<Long>> longTopkKudaf = createUdaf();
    List<Long> window = new ArrayList<>();
    for (final Long value : valuesArray) {
      window = longTopkKudaf.aggregate(Pair.of(value, EMPTY_VARARGS), window);
    }
    assertThat("Invalid results.", window, equalTo(ImmutableList.of(80L, 60L, 60L)));
  }

  @Test
  public void shouldAggregateTopKWithLessThanKValues() {
    final Udaf<Pair<Long, VariadicArgs<Object>>, List<Long>, List<Long>> longTopkKudaf = createUdaf();
    final List<Long> window = longTopkKudaf.aggregate(Pair.of(80L, EMPTY_VARARGS), new ArrayList());
    assertThat("Invalid results.", window, equalTo(ImmutableList.of(80L)));
  }

  @Test
  public void shouldMergeTopK() {
    final Udaf<Pair<Long, VariadicArgs<Object>>, List<Long>, List<Long>> topkKudaf = createUdaf();
    final List<Long> array1 = ImmutableList.of(50L, 45L, 25L);
    final List<Long> array2 = ImmutableList.of(60L, 55L, 48L);

    assertThat("Invalid results.", topkKudaf.merge(array1, array2),
        equalTo(ImmutableList.of(60L, 55L, 50L)));
  }

  @Test
  public void shouldMergeTopKWithNulls() {
    final Udaf<Pair<Long, VariadicArgs<Object>>, List<Long>, List<Long>> topkKudaf = createUdaf();
    final List<Long> array1 = ImmutableList.of(50L, 45L);
    final List<Long> array2 = ImmutableList.of(60L);

    assertThat("Invalid results.", topkKudaf.merge(array1, array2),
        equalTo(ImmutableList.of(60L, 50L, 45L)));
  }

  @Test
  public void shouldMergeTopKWithMoreNulls() {
    final Udaf<Pair<Long, VariadicArgs<Object>>, List<Long>, List<Long>> topkKudaf = createUdaf();
    final List<Long> array1 = ImmutableList.of(50L);
    final List<Long> array2 = ImmutableList.of(60L);

    assertThat("Invalid results.", topkKudaf.merge(array1, array2),
        equalTo(ImmutableList.of(60L, 50L)));
  }

  @Test
  public void shouldAggregateTopKStruct() {
    final Udaf<Pair<Long, VariadicArgs<Object>>, List<Struct>, List<Struct>> longTopkKudaf = createStructUdaf();
    List<Struct> window = new ArrayList<>();
    for (final Triple<Long, Boolean, String> value : valuesWithOtherColsArray) {
      window = longTopkKudaf.aggregate(
              Pair.of(value.getLeft(), new VariadicArgs<>(Arrays.asList(value.getMiddle(), value.getRight()))),
              window
      );
    }

    checkAggregate(
            ImmutableList.of(80L, 60L, 60L),
            ImmutableList.of(false, true, true),
            ImmutableList.of("ksql", "function", "testing"),
            window
    );
  }

  @Test
  public void shouldAggregateTopKWithLessThanKValuesStruct() {
    final Udaf<Pair<Long, VariadicArgs<Object>>, List<Struct>, List<Struct>> longTopkKudaf = createStructUdaf();
    final List<Struct> window = longTopkKudaf.aggregate(
            Pair.of(80L, new VariadicArgs<>(Arrays.asList(true, "test"))),
            new ArrayList<>()
    );

    checkAggregate(
            ImmutableList.of(80L),
            ImmutableList.of(true),
            ImmutableList.of("test"),
            window
    );
  }

  @Test
  public void shouldMergeTopKStruct() {
    final Udaf<Pair<Long, VariadicArgs<Object>>, List<Struct>, List<Struct>> topkKudaf = createStructUdaf();

    final List<Struct> array1 = ImmutableList.of(
            new Struct(STRUCT_SCHEMA).put("sort_col", 50L).put("col0", true).put("col1", "hello"),
            new Struct(STRUCT_SCHEMA).put("sort_col", 45L).put("col0", false).put("col1", "test"),
            new Struct(STRUCT_SCHEMA).put("sort_col", 25L).put("col0", true).put("col1", "ksql")
    );
    final List<Struct> array2 = ImmutableList.of(
            new Struct(STRUCT_SCHEMA).put("sort_col", 60L).put("col0", false).put("col1", "one"),
            new Struct(STRUCT_SCHEMA).put("sort_col", 55L).put("col0", false).put("col1", "two"),
            new Struct(STRUCT_SCHEMA).put("sort_col", 48L).put("col0", true).put("col1", "three")
    );

    checkAggregate(
            ImmutableList.of(60L, 55L, 50L),
            ImmutableList.of(false, false, true),
            ImmutableList.of("one", "two", "hello"),
            topkKudaf.merge(array1, array2)
    );
  }

  @Test
  public void shouldMergeTopKWithNullsStruct() {
    final Udaf<Pair<Long, VariadicArgs<Object>>, List<Struct>, List<Struct>> topkKudaf = createStructUdaf();

    final List<Struct> array1 = ImmutableList.of(
            new Struct(STRUCT_SCHEMA).put("sort_col", 55L).put("col0", false).put("col1", "two"),
            new Struct(STRUCT_SCHEMA).put("sort_col", 45L).put("col0", false).put("col1", "test")
    );
    final List<Struct> array2 = ImmutableList.of(
            new Struct(STRUCT_SCHEMA).put("sort_col", 60L).put("col0", false).put("col1", "one")
    );

    checkAggregate(
            ImmutableList.of(60L, 55L, 45L),
            ImmutableList.of(false, false, false),
            ImmutableList.of("one", "two", "test"),
            topkKudaf.merge(array1, array2)
    );
  }

  @Test
  public void shouldMergeTopKWithMoreNullsStruct() {
    final Udaf<Pair<Long, VariadicArgs<Object>>, List<Struct>, List<Struct>> topkKudaf = createStructUdaf();

    final List<Struct> array1 = ImmutableList.of(new Struct(STRUCT_SCHEMA).put("sort_col", 50L).put("col0", true).put("col1", "hello"));
    final List<Struct> array2 = ImmutableList.of(new Struct(STRUCT_SCHEMA).put("sort_col", 60L).put("col0", false).put("col1", "one"));

    checkAggregate(
            ImmutableList.of(60L, 50L),
            ImmutableList.of(false, true),
            ImmutableList.of("one", "hello"),
            topkKudaf.merge(array1, array2)
    );
  }

  private Udaf<Pair<Long, VariadicArgs<Object>>, List<Long>, List<Long>> createUdaf() {
    Udaf<Pair<Long, VariadicArgs<Object>>, List<Long>, List<Long>> udaf = TopkKudaf.createTopKLong(3);
    udaf.initializeTypeArguments(Arrays.asList(SqlArgument.of(SqlTypes.BIGINT), SqlArgument.of(SqlTypes.INTEGER)));
    return udaf;
  }

  private Udaf<Pair<Long, VariadicArgs<Object>>, List<Struct>, List<Struct>> createStructUdaf() {
    Udaf<Pair<Long, VariadicArgs<Object>>, List<Struct>, List<Struct>> udaf = TopkKudaf.createTopKLong(3);
    udaf.initializeTypeArguments(Arrays.asList(
            SqlArgument.of(SqlTypes.BIGINT),
            SqlArgument.of(SqlTypes.BOOLEAN),
            SqlArgument.of(SqlTypes.STRING),
            SqlArgument.of(SqlTypes.INTEGER)
    ));
    return udaf;
  }
}