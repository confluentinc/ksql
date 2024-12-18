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

public class DoubleTopkKudafTest {
  private static final VariadicArgs<Object> EMPTY_VARARGS = new VariadicArgs<>(
          Collections.emptyList()
  );
  private static final Schema STRUCT_SCHEMA = SchemaBuilder.struct().optional()
          .field("sort_col", Schema.OPTIONAL_FLOAT64_SCHEMA)
          .field("col0", Schema.OPTIONAL_BOOLEAN_SCHEMA)
          .field("col1", Schema.OPTIONAL_STRING_SCHEMA)
          .build();
  private final List<Double> valuesArray = ImmutableList.of(10.0, 30.0, 45.0, 10.0, 50.0, 60.0, 20.0, 60.0, 80.0, 35.0,
      25.0);
  private final List<Triple<Double, Boolean, String>> valuesWithOtherColsArray = ImmutableList.of(
          Triple.of(10.0,  true, "hello"),
          Triple.of(30.0, false, "world"),
          Triple.of(45.0, false, "topk"),
          Triple.of(10.0, true, "test"),
          Triple.of(50.0, false, "aggregate"),
          Triple.of(60.0, true, "function"),
          Triple.of(20.0, true, "udaf"),
          Triple.of(60.0, true, "testing"),
          Triple.of(80.0, false, "ksql"),
          Triple.of(35.0, true, "value"),
          Triple.of(25.0, false, "streaming")
  );

  @Test
  public void shouldAggregateTopK() {
    final Udaf<Pair<Double, VariadicArgs<Object>>, List<Double>, List<Double>> topkKudaf = createUdaf();
    List<Double> window = new ArrayList<>();
    for (final Double value : valuesArray) {
      window = topkKudaf.aggregate(Pair.of(value, EMPTY_VARARGS), window);
    }

    assertThat("Invalid results.", window, equalTo(ImmutableList.of(80.0, 60.0, 60.0)));
  }

  @Test
  public void shouldAggregateTopKWithLessThanKValues() {
    final Udaf<Pair<Double, VariadicArgs<Object>>, List<Double>, List<Double>> topkKudaf = createUdaf();
    List<Double> currentVal = new ArrayList<>();
    currentVal = topkKudaf.aggregate(Pair.of(10.0, EMPTY_VARARGS), currentVal);

    assertThat("Invalid results.", currentVal, equalTo(ImmutableList.of(10.0)));
  }

  @Test
  public void shouldMergeTopK() {
    final Udaf<Pair<Double, VariadicArgs<Object>>, List<Double>, List<Double>> topkKudaf = createUdaf();
    final List<Double> array1 = ImmutableList.of(50.0, 45.0, 25.0);
    final List<Double> array2 = ImmutableList.of(60.0, 55.0, 48.0);

    assertThat("Invalid results.", topkKudaf.merge(array1, array2),
        equalTo(ImmutableList.of(60.0, 55.0, 50.0)));
  }

  @Test
  public void shouldMergeTopKWithNulls() {
    final Udaf<Pair<Double, VariadicArgs<Object>>, List<Double>, List<Double>> topkKudaf = createUdaf();
    final List<Double> array1 = ImmutableList.of(50.0, 45.0);
    final List<Double> array2 = ImmutableList.of(60.0);

    assertThat("Invalid results.", topkKudaf.merge(array1, array2),
        equalTo(ImmutableList.of(60.0, 50.0, 45.0)));
  }

  @Test
  public void shouldMergeTopKWithMoreNulls() {
    final Udaf<Pair<Double, VariadicArgs<Object>>, List<Double>, List<Double>> topkKudaf = createUdaf();
    final List<Double> array1 = ImmutableList.of(50.0);
    final List<Double> array2 = ImmutableList.of(60.0);

    assertThat("Invalid results.", topkKudaf.merge(array1, array2),
        equalTo(ImmutableList.of(60.0, 50.0)));
  }

  @Test
  public void shouldAggregateTopKStruct() {
    final Udaf<Pair<Double, VariadicArgs<Object>>, List<Struct>, List<Struct>> topkKudaf = createStructUdaf();
    List<Struct> window = new ArrayList<>();
    for (final Triple<Double, Boolean, String> value : valuesWithOtherColsArray) {
      window = topkKudaf.aggregate(
              Pair.of(value.getLeft(), new VariadicArgs<>(Arrays.asList(value.getMiddle(), value.getRight()))),
              window
      );
    }

    checkAggregate(
            ImmutableList.of(80.0, 60.0, 60.0),
            ImmutableList.of(false, true, true),
            ImmutableList.of("ksql", "function", "testing"),
            window
    );
  }

  @Test
  public void shouldAggregateTopKWithLessThanKValuesStruct() {
    final Udaf<Pair<Double, VariadicArgs<Object>>, List<Struct>, List<Struct>> topkKudaf = createStructUdaf();
    List<Struct> currentVal = new ArrayList<>();
    currentVal = topkKudaf.aggregate(
            Pair.of(10.0, new VariadicArgs<>(Arrays.asList(true, "test"))),
            currentVal
    );

    checkAggregate(
            ImmutableList.of(10.0),
            ImmutableList.of(true),
            ImmutableList.of("test"),
            currentVal
    );
  }

  @Test
  public void shouldMergeTopKStruct() {
    final Udaf<Pair<Double, VariadicArgs<Object>>, List<Struct>, List<Struct>> topkKudaf = createStructUdaf();
    final List<Struct> array1 = ImmutableList.of(
            new Struct(STRUCT_SCHEMA).put("sort_col", 50.0).put("col0", true).put("col1", "hello"),
            new Struct(STRUCT_SCHEMA).put("sort_col", 45.0).put("col0", false).put("col1", "test"),
            new Struct(STRUCT_SCHEMA).put("sort_col", 25.0).put("col0", true).put("col1", "ksql")
    );
    final List<Struct> array2 = ImmutableList.of(
            new Struct(STRUCT_SCHEMA).put("sort_col", 60.0).put("col0", false).put("col1", "one"),
            new Struct(STRUCT_SCHEMA).put("sort_col", 55.0).put("col0", false).put("col1", "two"),
            new Struct(STRUCT_SCHEMA).put("sort_col", 48.0).put("col0", true).put("col1", "three")
    );

    checkAggregate(
            ImmutableList.of(60.0, 55.0, 50.0),
            ImmutableList.of(false, false, true),
            ImmutableList.of("one", "two", "hello"),
            topkKudaf.merge(array1, array2)
    );
  }

  @Test
  public void shouldMergeTopKWithNullsStruct() {
    final Udaf<Pair<Double, VariadicArgs<Object>>, List<Struct>, List<Struct>> topkKudaf = createStructUdaf();

    final List<Struct> array1 = ImmutableList.of(
            new Struct(STRUCT_SCHEMA).put("sort_col", 55.0).put("col0", false).put("col1", "two"),
            new Struct(STRUCT_SCHEMA).put("sort_col", 45.0).put("col0", false).put("col1", "test")
    );
    final List<Struct> array2 = ImmutableList.of(
            new Struct(STRUCT_SCHEMA).put("sort_col", 60.0).put("col0", false).put("col1", "one")
    );

    checkAggregate(
            ImmutableList.of(60.0, 55.0, 45.0),
            ImmutableList.of(false, false, false),
            ImmutableList.of("one", "two", "test"),
            topkKudaf.merge(array1, array2)
    );
  }

  @Test
  public void shouldMergeTopKWithMoreNullsStruct() {
    final Udaf<Pair<Double, VariadicArgs<Object>>, List<Struct>, List<Struct>> topkKudaf = createStructUdaf();

    final List<Struct> array1 = ImmutableList.of(new Struct(STRUCT_SCHEMA).put("sort_col", 50.0).put("col0", true).put("col1", "hello"));
    final List<Struct> array2 = ImmutableList.of(new Struct(STRUCT_SCHEMA).put("sort_col", 60.0).put("col0", false).put("col1", "one"));

    checkAggregate(
            ImmutableList.of(60.0, 50.0),
            ImmutableList.of(false, true),
            ImmutableList.of("one", "hello"),
            topkKudaf.merge(array1, array2)
    );
  }

  private Udaf<Pair<Double, VariadicArgs<Object>>, List<Double>, List<Double>> createUdaf() {
    Udaf<Pair<Double, VariadicArgs<Object>>, List<Double>, List<Double>> udaf = TopkKudaf.createTopKDouble(3);
    udaf.initializeTypeArguments(Arrays.asList(SqlArgument.of(SqlTypes.DOUBLE), SqlArgument.of(SqlTypes.INTEGER)));
    return udaf;
  }

  private Udaf<Pair<Double, VariadicArgs<Object>>, List<Struct>, List<Struct>> createStructUdaf() {
    Udaf<Pair<Double, VariadicArgs<Object>>, List<Struct>, List<Struct>> udaf = TopkKudaf.createTopKDouble(3);
    udaf.initializeTypeArguments(Arrays.asList(
            SqlArgument.of(SqlTypes.DOUBLE),
            SqlArgument.of(SqlTypes.BOOLEAN),
            SqlArgument.of(SqlTypes.STRING),
            SqlArgument.of(SqlTypes.INTEGER)
    ));
    return udaf;
  }

}