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

public class StringTopkKudafTest {
  private static final VariadicArgs<Object> EMPTY_VARARGS = new VariadicArgs<>(
          Collections.emptyList()
  );
  private static final Schema STRUCT_SCHEMA = SchemaBuilder.struct().optional()
          .field("sort_col", Schema.OPTIONAL_STRING_SCHEMA)
          .field("col0", Schema.OPTIONAL_BOOLEAN_SCHEMA)
          .field("col1", Schema.OPTIONAL_STRING_SCHEMA)
          .build();
  private final List<String> valueArray = ImmutableList.of("10", "ab", "cde", "efg", "aa", "32", "why", "How are you",
      "Test", "123", "432");
  private final List<Triple<String, Boolean, String>> valuesWithOtherColsArray = ImmutableList.of(
          Triple.of("10",  true, "hello"),
          Triple.of("ab", false, "world"),
          Triple.of("cde", false, "topk"),
          Triple.of("efg", true, "test"),
          Triple.of("aa", false, "aggregate"),
          Triple.of("32", true, "function"),
          Triple.of("why", true, "udaf"),
          Triple.of("How are you", true, "testing"),
          Triple.of("Test", false, "ksql"),
          Triple.of("123", true, "value"),
          Triple.of("432", false, "streaming")
  );

  @Test
  public void shouldAggregateTopK() {
    final Udaf<Pair<String, VariadicArgs<Object>>, List<String>, List<String>> topkKudaf = createUdaf();
    List<String> currentVal = new ArrayList<>();
    for (final String value : valueArray) {
      currentVal = topkKudaf.aggregate(Pair.of(value, EMPTY_VARARGS), currentVal);
    }

    assertThat("Invalid results.", currentVal, equalTo(ImmutableList.of("why", "efg", "cde")));
  }

  @Test
  public void shouldAggregateTopKWithLessThanKValues() {
    final Udaf<Pair<String, VariadicArgs<Object>>, List<String>, List<String>> topkKudaf = createUdaf();
    List<String> currentVal = new ArrayList<>();
    currentVal = topkKudaf.aggregate(Pair.of("why", EMPTY_VARARGS), currentVal);

    assertThat("Invalid results.", currentVal, equalTo(ImmutableList.of("why")));
  }

  @Test
  public void shouldMergeTopK() {
    final Udaf<Pair<String, VariadicArgs<Object>>, List<String>, List<String>> topkKudaf = createUdaf();
    final List<String> array1 = ImmutableList.of("paper", "Hello", "123");
    final List<String> array2 = ImmutableList.of("Zzz", "Hi", "456");

    assertThat("Invalid results.", topkKudaf.merge(array1, array2),
        equalTo(ImmutableList.of("paper", "Zzz", "Hi")));
  }

  @Test
  public void shouldMergeTopKWithNulls() {
    final Udaf<Pair<String, VariadicArgs<Object>>, List<String>, List<String>> topkKudaf = createUdaf();
    final List<String> array1 = ImmutableList.of("50", "45");
    final List<String> array2 = ImmutableList.of("60");

    assertThat("Invalid results.", topkKudaf.merge(array1, array2),
        equalTo(ImmutableList.of("60", "50", "45")));
  }

  @Test
  public void shouldMergeTopKWithMoreNulls() {
    final Udaf<Pair<String, VariadicArgs<Object>>, List<String>, List<String>> topkKudaf = createUdaf();
    final List<String> array1 = ImmutableList.of("50");
    final List<String> array2 = ImmutableList.of("60");

    assertThat("Invalid results.", topkKudaf.merge(array1, array2),
        equalTo(ImmutableList.of("60", "50")));
  }

  @Test
  public void shouldAggregateTopKStruct() {
    final Udaf<Pair<String, VariadicArgs<Object>>, List<Struct>, List<Struct>> topkKudaf = createStructUdaf();
    List<Struct> currentVal = new ArrayList<>();
    for (final Triple<String, Boolean, String> value : valuesWithOtherColsArray) {
      currentVal = topkKudaf.aggregate(
              Pair.of(value.getLeft(), new VariadicArgs<>(Arrays.asList(value.getMiddle(), value.getRight()))),
              currentVal
      );
    }

    checkAggregate(
            ImmutableList.of("why", "efg", "cde"),
            ImmutableList.of(true, true, false),
            ImmutableList.of("udaf", "test", "topk"),
            currentVal
    );
  }

  @Test
  public void shouldAggregateTopKWithLessThanKValuesStruct() {
    final Udaf<Pair<String, VariadicArgs<Object>>, List<Struct>, List<Struct>> topkKudaf = createStructUdaf();
    List<Struct> currentVal = new ArrayList<>();
    currentVal = topkKudaf.aggregate(
            Pair.of("why", new VariadicArgs<>(Arrays.asList(false, "ksql"))),
            currentVal
    );

    checkAggregate(
            ImmutableList.of("why"),
            ImmutableList.of(false),
            ImmutableList.of("ksql"),
            currentVal
    );
  }

  @Test
  public void shouldMergeTopKStruct() {
    final Udaf<Pair<String, VariadicArgs<Object>>, List<Struct>, List<Struct>> topkKudaf = createStructUdaf();

    final List<Struct> array1 = ImmutableList.of(
            new Struct(STRUCT_SCHEMA).put("sort_col", "paper").put("col0", true).put("col1", "hello"),
            new Struct(STRUCT_SCHEMA).put("sort_col", "Hello").put("col0", false).put("col1", "test"),
            new Struct(STRUCT_SCHEMA).put("sort_col", "123").put("col0", true).put("col1", "ksql")
    );
    final List<Struct> array2 = ImmutableList.of(
            new Struct(STRUCT_SCHEMA).put("sort_col", "Zzz").put("col0", false).put("col1", "one"),
            new Struct(STRUCT_SCHEMA).put("sort_col", "Hi").put("col0", false).put("col1", "two"),
            new Struct(STRUCT_SCHEMA).put("sort_col", "456").put("col0", true).put("col1", "three")
    );

    checkAggregate(
            ImmutableList.of("paper", "Zzz", "Hi"),
            ImmutableList.of(true, false, false),
            ImmutableList.of("hello", "one", "two"),
            topkKudaf.merge(array1, array2)
    );
  }

  @Test
  public void shouldMergeTopKWithNullsStruct() {
    final Udaf<Pair<String, VariadicArgs<Object>>, List<Struct>, List<Struct>> topkKudaf = createStructUdaf();

    final List<Struct> array1 = ImmutableList.of(
            new Struct(STRUCT_SCHEMA).put("sort_col", "50").put("col0", false).put("col1", "two"),
            new Struct(STRUCT_SCHEMA).put("sort_col", "45").put("col0", false).put("col1", "test")
    );
    final List<Struct> array2 = ImmutableList.of(
            new Struct(STRUCT_SCHEMA).put("sort_col", "60").put("col0", false).put("col1", "one")
    );

    checkAggregate(
            ImmutableList.of("60", "50", "45"),
            ImmutableList.of(false, false, false),
            ImmutableList.of("one", "two", "test"),
            topkKudaf.merge(array1, array2)
    );
  }

  @Test
  public void shouldMergeTopKWithMoreNullsStruct() {
    final Udaf<Pair<String, VariadicArgs<Object>>, List<Struct>, List<Struct>> topkKudaf = createStructUdaf();

    final List<Struct> array1 = ImmutableList.of(new Struct(STRUCT_SCHEMA).put("sort_col", "50").put("col0", true).put("col1", "hello"));
    final List<Struct> array2 = ImmutableList.of(new Struct(STRUCT_SCHEMA).put("sort_col", "60").put("col0", false).put("col1", "one"));

    checkAggregate(
            ImmutableList.of("60", "50"),
            ImmutableList.of(false, true),
            ImmutableList.of("one", "hello"),
            topkKudaf.merge(array1, array2)
    );
  }

  private Udaf<Pair<String, VariadicArgs<Object>>, List<String>, List<String>> createUdaf() {
    Udaf<Pair<String, VariadicArgs<Object>>, List<String>, List<String>> udaf = TopkKudaf.createTopKString(3);
    udaf.initializeTypeArguments(Arrays.asList(SqlArgument.of(SqlTypes.STRING), SqlArgument.of(SqlTypes.INTEGER)));
    return udaf;
  }

  private Udaf<Pair<String, VariadicArgs<Object>>, List<Struct>, List<Struct>> createStructUdaf() {
    Udaf<Pair<String, VariadicArgs<Object>>, List<Struct>, List<Struct>> udaf = TopkKudaf.createTopKString(3);
    udaf.initializeTypeArguments(Arrays.asList(
            SqlArgument.of(SqlTypes.STRING),
            SqlArgument.of(SqlTypes.BOOLEAN),
            SqlArgument.of(SqlTypes.STRING),
            SqlArgument.of(SqlTypes.INTEGER)
    ));
    return udaf;
  }
}