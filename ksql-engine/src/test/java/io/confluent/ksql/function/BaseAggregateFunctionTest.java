/*
 * Copyright 2019 Confluent Inc.
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

package io.confluent.ksql.function;

import static java.util.Collections.emptyList;
import static org.apache.kafka.connect.data.Schema.INT32_SCHEMA;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;

import io.confluent.ksql.schema.ksql.types.SqlTypes;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.streams.kstream.Merger;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class BaseAggregateFunctionTest {

  @Mock
  private Supplier<Integer> initialValueSupplier;

  @Test
  public void shouldThrowOnNonOptionalReturnType() {
    // When:
    final Exception e = assertThrows(
        IllegalArgumentException.class,
        () -> new TestAggFunc(
            "funcName",
            0,
            initialValueSupplier,
            INT32_SCHEMA, // <-- non-optional return type.
            emptyList(),
            "the description"
        )
    );

    // Then:
    assertThat(e.getMessage(), containsString("KSQL only supports optional field types"));
  }

  @Test
  public void shouldReturnSqlReturnType() {
    // When:
    final TestAggFunc aggFunc = new TestAggFunc(
        "funcName",
        0,
        initialValueSupplier,
        Schema.OPTIONAL_INT64_SCHEMA,
        Collections.emptyList(),
        "the description"
    );

    // Then:
    assertThat(aggFunc.returnType(), is(SqlTypes.BIGINT));
  }

  private static final class TestAggFunc extends BaseAggregateFunction<String, Integer, Integer> {

    TestAggFunc(
        final String functionName,
        final int argIndexInValue,
        final Supplier<Integer> initialValueSupplier,
        final Schema returnType,
        final List<Schema> arguments,
        final String description
    ) {
      super(functionName, argIndexInValue, initialValueSupplier, returnType, returnType,
            arguments, description);
    }

    @Override
    public Integer aggregate(final String currentValue, final Integer aggregateValue) {
      return null;
    }

    @Override
    public Merger<Struct, Integer> getMerger() {
      return null;
    }

    @Override
    public Function<Integer, Integer> getResultMapper() {
      return null;
    }
  }
}