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
import static org.hamcrest.CoreMatchers.is;
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
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.Before;
import org.junit.Test;

public class IntTopkKudafTest {
  private static final VariadicArgs<Object> EMPTY_VARARGS = new VariadicArgs<>(
          Collections.emptyList()
  );
  private static final Schema STRUCT_SCHEMA = SchemaBuilder.struct().optional()
          .field("sort_col", Schema.OPTIONAL_INT32_SCHEMA)
          .field("col0", Schema.OPTIONAL_BOOLEAN_SCHEMA)
          .field("col1", Schema.OPTIONAL_STRING_SCHEMA)
          .build();
  private final List<Integer> valuesArray = ImmutableList.of(10, 30, 45, 10, 50, 60, 20, 60, 80, 35, 25);
  private final List<Triple<Integer, Boolean, String>> valuesWithOtherColsArray = ImmutableList.of(
          Triple.of(10,  true, "hello"),
          Triple.of(30, false, "world"),
          Triple.of(45, false, "topk"),
          Triple.of(10, true, "test"),
          Triple.of(50, false, "aggregate"),
          Triple.of(60, true, "function"),
          Triple.of(20, true, "udaf"),
          Triple.of(60, true, "testing"),
          Triple.of(80, false, "ksql"),
          Triple.of(35, true, "value"),
          Triple.of(25, false, "streaming")
  );
  private Udaf<Pair<Integer, VariadicArgs<Object>>, List<Integer>, List<Integer>>  topkKudaf;
  private Udaf<Pair<Integer, VariadicArgs<Object>>, List<Struct>, List<Struct>>  topkKudafStruct;

  @Before
  public void setup() {
    topkKudaf = TopkKudaf.createTopKInt(3);
    topkKudaf.initializeTypeArguments(Arrays.asList(
            SqlArgument.of(SqlTypes.INTEGER),
            SqlArgument.of(SqlTypes.INTEGER)
    ));
    topkKudafStruct = TopkKudaf.createTopKInt(3);
    topkKudafStruct.initializeTypeArguments(Arrays.asList(
            SqlArgument.of(SqlTypes.INTEGER),
            SqlArgument.of(SqlTypes.BOOLEAN),
            SqlArgument.of(SqlTypes.STRING),
            SqlArgument.of(SqlTypes.INTEGER)
    ));
  }

  @Test
  public void shouldAggregateTopK() {
    List<Integer> currentVal = new ArrayList<>();
    for (final Integer value : valuesArray) {
      currentVal = topkKudaf.aggregate(
              Pair.of(value, EMPTY_VARARGS),
              currentVal
      );
    }

    assertThat("Invalid results.", currentVal, equalTo(ImmutableList.of(80, 60, 60)));
  }

  @Test
  public void shouldAggregateTopKWithLessThanKValues() {
    List<Integer> currentVal = new ArrayList<>();
    currentVal = topkKudaf.aggregate(
            Pair.of(10, EMPTY_VARARGS),
            currentVal
    );

    assertThat("Invalid results.", currentVal, equalTo(ImmutableList.of(10)));
  }

  @Test
  public void shouldMergeTopK() {
    final List<Integer> array1 = ImmutableList.of(50, 45, 25);
    final List<Integer> array2 = ImmutableList.of(60, 55, 48);

    assertThat("Invalid results.", topkKudaf.merge(array1, array2),
        equalTo(ImmutableList.of(60, 55, 50)));
  }

  @Test
  public void shouldMergeTopKWithNulls() {
    final List<Integer> array1 = ImmutableList.of(50, 45);
    final List<Integer> array2 = ImmutableList.of(60);

    assertThat("Invalid results.", topkKudaf.merge(array1, array2),
        equalTo(ImmutableList.of(60, 50, 45)));
  }

  @Test
  public void shouldMergeTopKWithMoreNulls() {
    final List<Integer> array1 = ImmutableList.of(50);
    final List<Integer> array2 = ImmutableList.of(60);

    assertThat("Invalid results.", topkKudaf.merge(array1, array2),
        equalTo(ImmutableList.of(60, 50)));
  }

  @Test
  public void shouldAggregateAndProducedOrderedTopK() {
    final List<Integer> aggregate = topkKudaf.aggregate(
            Pair.of(1, EMPTY_VARARGS),
            new ArrayList<>()
    );
    assertThat(aggregate, equalTo(ImmutableList.of(1)));
    final List<Integer> agg2 = topkKudaf.aggregate(
            Pair.of(100, EMPTY_VARARGS),
            aggregate
    );
    assertThat(agg2, equalTo(ImmutableList.of(100, 1)));
  }

  @Test
  public void shouldWorkWithLargeValuesOfKay() {
    // Given:
    final int topKSize = 300;
    topkKudaf = createUdaf(topKSize);
    final List<Integer> initialAggregate = IntStream.range(0, topKSize)
            .boxed().sorted(Comparator.reverseOrder()).collect(Collectors.toList());

    // When:
    final List<Integer> result = topkKudaf.aggregate(
            Pair.of(10, EMPTY_VARARGS), 
            initialAggregate
    );
    final List<Integer> combined = topkKudaf.merge(result, initialAggregate);

    // Then:
    assertThat(combined.get(0), is(299));
    assertThat(combined.get(1), is(299));
    assertThat(combined.get(2), is(298));
  }

  @Test
  public void shouldBeThreadSafe() {
    // Given:
    topkKudaf = createUdaf(12);

    final List<Integer> values = ImmutableList.of(10, 30, 45, 10, 50, 60, 20, 70, 80, 35, 25);

    // When:
    final List<Integer> result = IntStream.range(0, 4)
        .parallel()
        .mapToObj(threadNum -> {
          List<Integer> aggregate = Arrays.asList(0, 0, 0, 0, 0, 0, 0, 0, 0,
              0, 0, 0);
          for (int value : values) {
            aggregate = topkKudaf.aggregate(
                    Pair.of(value + threadNum, EMPTY_VARARGS), 
                    aggregate
            );
          }
          return aggregate;
        })
        .reduce((agg1, agg2) -> topkKudaf.merge(agg1, agg2))
        .orElse(new ArrayList<>());

    // Then:
    assertThat(result, is(ImmutableList.of(83, 82, 81, 80, 73, 72, 71, 70, 63, 62, 61, 60)));
  }

  //@Test
  public void testAggregatePerformance() {
    final int iterations = 1_000_000_000;
    final int topX = 10;
    topkKudaf = createUdaf(topX);
    final List<Integer> aggregate = new ArrayList<>();
    final long start = System.currentTimeMillis();

    for(int i = 0; i != iterations; ++i) {
      topkKudaf.aggregate(Pair.of(i, EMPTY_VARARGS), aggregate);
    }

    final long took = System.currentTimeMillis() - start;
    System.out.println(took + "ms, " + ((double)took)/iterations);
  }

  //@Test
  public void testMergePerformance() {
    final int iterations = 1_000_000_000;
    final int topX = 10;
    topkKudaf = createUdaf(topX);

    final List<Integer> aggregate1 = IntStream.range(0, topX)
        .mapToObj(v -> v % 2 == 0 ? v + 1 : v)
        .collect(Collectors.toList());
    final List<Integer> aggregate2 = IntStream.range(0, topX)
        .mapToObj(v -> v % 2 == 0 ? v : v + 1)
        .collect(Collectors.toList());
    final long start = System.currentTimeMillis();

    for(int i = 0; i != iterations; ++i) {
      topkKudaf.merge(aggregate1, aggregate2);
    }

    final long took = System.currentTimeMillis() - start;
    System.out.println(took + "ms, " + ((double)took)/iterations);
  }

  @Test
  public void shouldAggregateTopKStruct() {
    List<Struct> currentVal = new ArrayList<>();
    for (final Triple<Integer, Boolean, String> value : valuesWithOtherColsArray) {
      currentVal = topkKudafStruct.aggregate(
              Pair.of(
                      value.getLeft(),
                      new VariadicArgs<>(Arrays.asList(value.getMiddle(), value.getRight()))
              ),
              currentVal
      );
    }
    checkAggregate(
            ImmutableList.of(80, 60, 60),
            ImmutableList.of(false, true, true),
            ImmutableList.of("ksql", "function", "testing"),
            currentVal
    );
  }

  @Test
  public void shouldAggregateTopKWithLessThanKValuesStruct() {
    List<Struct> currentVal = new ArrayList<>();
    currentVal = topkKudafStruct.aggregate(
            Pair.of(10, new VariadicArgs<>(Arrays.asList(true, "hello"))),
            currentVal
    );
    checkAggregate(
            ImmutableList.of(10),
            ImmutableList.of(true),
            ImmutableList.of("hello"),
            currentVal
    );
  }

  @Test
  public void shouldMergeTopKStruct() {
    final List<Struct> array1 = ImmutableList.of(
            new Struct(STRUCT_SCHEMA).put("sort_col", 50).put("col0", true).put("col1", "hello"),
            new Struct(STRUCT_SCHEMA).put("sort_col", 45).put("col0", false).put("col1", "test"),
            new Struct(STRUCT_SCHEMA).put("sort_col", 25).put("col0", true).put("col1", "ksql")
    );
    final List<Struct> array2 = ImmutableList.of(
            new Struct(STRUCT_SCHEMA).put("sort_col", 60).put("col0", false).put("col1", "one"),
            new Struct(STRUCT_SCHEMA).put("sort_col", 55).put("col0", false).put("col1", "two"),
            new Struct(STRUCT_SCHEMA).put("sort_col", 40).put("col0", true).put("col1", "three")
    );

    checkAggregate(
            ImmutableList.of(60, 55, 50),
            ImmutableList.of(false, false, true),
            ImmutableList.of("one", "two", "hello"),
            topkKudafStruct.merge(array1, array2)
    );
  }

  @Test
  public void shouldMergeTopKWithNullsStruct() {
    final List<Struct> array1 = ImmutableList.of(
            new Struct(STRUCT_SCHEMA).put("sort_col", 55).put("col0", false).put("col1", "two"),
            new Struct(STRUCT_SCHEMA).put("sort_col", 45).put("col0", false).put("col1", "test")
    );
    final List<Struct> array2 = ImmutableList.of(
            new Struct(STRUCT_SCHEMA).put("sort_col", 60).put("col0", false).put("col1", "one")
    );

    checkAggregate(
            ImmutableList.of(60, 55, 45),
            ImmutableList.of(false, false, false),
            ImmutableList.of("one", "two", "test"),
            topkKudafStruct.merge(array1, array2)
    );
  }

  @Test
  public void shouldMergeTopKWithMoreNullsStruct() {
    final List<Struct> array1 = ImmutableList.of(new Struct(STRUCT_SCHEMA).put("sort_col", 50).put("col0", true).put("col1", "hello"));
    final List<Struct> array2 = ImmutableList.of(new Struct(STRUCT_SCHEMA).put("sort_col", 60).put("col0", false).put("col1", "one"));

    checkAggregate(
            ImmutableList.of(60, 50),
            ImmutableList.of(false, true),
            ImmutableList.of("one", "hello"),
            topkKudafStruct.merge(array1, array2)
    );
  }

  @Test
  public void shouldAggregateAndProducedOrderedTopKStruct() {
    final List<Struct> aggregate = topkKudafStruct.aggregate(
            Pair.of(1, new VariadicArgs<>(Arrays.asList(false, "test"))),
            new ArrayList<>()
    );
    checkAggregate(
            ImmutableList.of(1),
            ImmutableList.of(false),
            ImmutableList.of("test"),
            aggregate
    );


    final List<Struct> agg2 = topkKudafStruct.aggregate(
            Pair.of(100, new VariadicArgs<>(Arrays.asList(true, "hello"))),
            aggregate
    );
    checkAggregate(
            ImmutableList.of(100, 1),
            ImmutableList.of(true, false),
            ImmutableList.of("hello", "test"),
            agg2
    );
  }

  @Test
  public void shouldWorkWithLargeValuesOfKayStruct() {
    // Given:
    final int topKSize = 300;
    topkKudafStruct = createStructUdaf(topKSize);
    final List<Struct> initialAggregate = IntStream.range(0, topKSize)
            .boxed().sorted(Comparator.reverseOrder())
            .map((sortCol) -> new Struct(STRUCT_SCHEMA)
                    .put("sort_col", sortCol)
                    .put("col0", sortCol % 2 == 0)
                    .put("col1", String.valueOf(sortCol))
            ).collect(Collectors.toList());

    // When:
    final List<Struct> result = topkKudafStruct.aggregate(
            Pair.of(10, new VariadicArgs<>(Arrays.asList(false, "test"))),
            initialAggregate
    );
    final List<Struct> combined = topkKudafStruct.merge(result, initialAggregate);

    // Then:
    checkAggregate(
            ImmutableList.of(299, 299, 298),
            ImmutableList.of(false, false, true),
            ImmutableList.of("299", "299", "298"),
            combined.subList(0, 3)
    );
  }

  @Test
  public void shouldBeThreadSafeStruct() {
    // Given:
    topkKudafStruct = createStructUdaf(12);

    final List<Triple<Integer, Boolean, String>> values = ImmutableList.of(
            Triple.of(10,  true, "hello"),
            Triple.of(30, false, "world"),
            Triple.of(45, false, "topk"),
            Triple.of(10, true, "test"),
            Triple.of(50, false, "aggregate"),
            Triple.of(60, true, "function"),
            Triple.of(20, true, "udaf"),
            Triple.of(70, true, "testing"),
            Triple.of(80, false, "ksql"),
            Triple.of(35, true, "value"),
            Triple.of(25, false, "streaming")
    );

    // When:
    final List<Struct> result = IntStream.range(0, 4)
            .parallel()
            .mapToObj(threadNum -> {
              List<Struct> aggregate = new ArrayList<>(Collections.nCopies(
                      12,
                      new Struct(STRUCT_SCHEMA).put("sort_col", 0).put("col0", true).put("col1", "test")
              ));
              for (Triple<Integer, Boolean, String> value : values) {
                aggregate = topkKudafStruct.aggregate(
                        Pair.of(
                                value.getLeft() + threadNum,
                                new VariadicArgs<>(Arrays.asList(value.getMiddle(), value.getRight()))
                        ),
                        aggregate
                );
              }
              return aggregate;
            })
            .reduce((agg1, agg2) -> topkKudafStruct.merge(agg1, agg2))
            .orElse(new ArrayList<>());

    // Then:
    checkAggregate(
            ImmutableList.of(
                    83, 82, 81, 80,
                    73, 72, 71, 70,
                    63, 62, 61, 60
            ),
            ImmutableList.of(
                    false, false, false, false,
                    true, true, true, true,
                    true, true, true, true
            ),
            ImmutableList.of(
                    "ksql", "ksql", "ksql", "ksql",
                    "testing", "testing", "testing", "testing",
                    "function", "function", "function", "function"
            ),
            result
    );
  }

  private Udaf<Pair<Integer, VariadicArgs<Object>>, List<Integer>, List<Integer>> createUdaf(final int k) {
    Udaf<Pair<Integer, VariadicArgs<Object>>, List<Integer>, List<Integer>> udaf = TopkKudaf.createTopKInt(k);
    udaf.initializeTypeArguments(Arrays.asList(
            SqlArgument.of(SqlTypes.INTEGER),
            SqlArgument.of(SqlTypes.INTEGER)
    ));
    return udaf;
  }

  private Udaf<Pair<Integer, VariadicArgs<Object>>, List<Struct>, List<Struct>> createStructUdaf(final int k) {
    Udaf<Pair<Integer, VariadicArgs<Object>>, List<Struct>, List<Struct>> udaf = TopkKudaf.createTopKInt(k);
    udaf.initializeTypeArguments(Arrays.asList(
            SqlArgument.of(SqlTypes.INTEGER),
            SqlArgument.of(SqlTypes.BOOLEAN),
            SqlArgument.of(SqlTypes.STRING),
            SqlArgument.of(SqlTypes.INTEGER)
    ));
    return udaf;
  }
}