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

import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.streams.kstream.Merger;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mock;

public class BaseAggregateFunctionTest {

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Mock
  private Supplier<Integer> initialValueSupplier;

  @Test
  public void shouldThrowOnNonOptionalReturnType() {
    // Then:
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("KSQL only supports optional field types");

    // When:
    new TestAggFunc(
        "funcName",
        0,
        initialValueSupplier,
        Schema.INT32_SCHEMA, // <-- non-optional return type.
        Collections.emptyList(),
        "the description"
    );
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
    public Merger<Struct, Integer> getMerger() {
      return null;
    }
  }
}