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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertThrows;

import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.streams.kstream.Merger;
import org.junit.Test;
import org.mockito.Mock;

public class BaseAggregateFunctionTest {

  @Mock
  private Supplier<Integer> initialValueSupplier;

  @Test
  public void shouldThrowOnNonOptionalReturnType() {
    // When:
    final IllegalArgumentException e = assertThrows(
        (IllegalArgumentException.class),
        () -> new TestAggFunc(
            "funcName",
            0,
            initialValueSupplier,
            Schema.INT32_SCHEMA, // <-- non-optional return type.
            Collections.emptyList(),
            "the description"
        )
    );

    // Then:
    assertThat(e.getMessage(), containsString("KSQL only supports optional field types"));
  }

  private static final class TestAggFunc extends BaseAggregateFunction<String, Integer> {

    TestAggFunc(
        final String functionName,
        final int argIndexInValue,
        final Supplier<Integer> initialValueSupplier,
        final Schema returnType,
        final List<Schema> arguments,
        final String description
    ) {
      super(functionName, argIndexInValue, initialValueSupplier, returnType, arguments,
          description);
    }

    @Override
    public KsqlAggregateFunction<String, Integer> getInstance(
        final AggregateFunctionArguments args) {
      return null;
    }

    @Override
    public Integer aggregate(final String currentValue, final Integer aggregateValue) {
      return null;
    }

    @Override
    public Merger<String, Integer> getMerger() {
      return null;
    }
  }
}