/**
 * Copyright 2017 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.confluent.ksql.function;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.streams.kstream.Merger;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class InternalFunctionRegistryTest {

  private final InternalFunctionRegistry functionRegistry = new InternalFunctionRegistry();
  private final KsqlFunction func = new KsqlFunction(Schema.STRING_SCHEMA,
      Collections.emptyList(),
      "func",
      Object.class);

  @Test
  public void shouldAddFunction() {
    functionRegistry.addFunction(
        func);
    final UdfFactory factory = functionRegistry.getUdfFactory("func");
    assertThat(factory.getReturnType(), equalTo(Schema.STRING_SCHEMA));
    assertThat(factory.getFunction(Collections.emptyList()), equalTo(this.func));
  }

  @Test
  public void shouldNotAddFunctionWithSameNameAsExistingFunctionAndOnDifferentClass() {
    final KsqlFunction func2 = new KsqlFunction(Schema.STRING_SCHEMA,
        Collections.emptyList(),
        "func",
        String.class);
    functionRegistry.addFunction(func);
    assertFalse(functionRegistry.addFunction(func2));
    assertThat(functionRegistry.getUdfFactory("func").getFunction(Collections.emptyList()),
        not(equalTo(func2)));
  }

  @Test
  public void shouldCreateCopyOfRegistry() {
    functionRegistry.addFunction(func);
    final FunctionRegistry copy = functionRegistry.copy();
    final KsqlFunction func2 = new KsqlFunction(Schema.STRING_SCHEMA,
        Collections.emptyList(),
        "func2",
        Object.class);

    copy.addFunction(func2);

    assertThat(copy.getUdfFactory("func").getFunction(Collections.emptyList()), equalTo(func));
    assertThat(copy.getUdfFactory("func2").getFunction(Collections.emptyList()), equalTo(func2));
    assertThat(functionRegistry.getUdfFactory("func2"), nullValue());
  }

  @Test
  public void shouldKnowIfFunctionIsAggregate() {
    assertFalse(functionRegistry.isAggregate("lcase"));
    assertTrue(functionRegistry.isAggregate("topk"));
  }

  @Test
  public void shouldAddAggregateFunction() {
    functionRegistry.addAggregateFunctionFactory(
        new AggregateFunctionFactory("my_aggregate", Collections.emptyList()) {
          @Override
          public KsqlAggregateFunction getProperAggregateFunction(List<Schema> argTypeList) {
            return new KsqlAggregateFunction() {
              @Override
              public KsqlAggregateFunction getInstance(AggregateFunctionArguments aggregateFunctionArguments) {
                return null;
              }

              @Override
              public String getFunctionName() {
                return "my_aggregate";
              }

              @Override
              public Supplier getInitialValueSupplier() {
                return null;
              }

              @Override
              public int getArgIndexInValue() {
                return 0;
              }

              @Override
              public Schema getReturnType() {
                return null;
              }

              @Override
              public boolean hasSameArgTypes(List argTypeList) {
                return false;
              }

              @Override
              public Object aggregate(Object currentValue, Object aggregateValue) {
                return null;
              }

              @Override
              public Merger getMerger() {
                return null;
              }
            };
          }
        });
    assertThat(functionRegistry.getAggregate("my_aggregate", Schema.INT32_SCHEMA), not(nullValue()));
  }

  @Test
  public void shouldNotAddFunctionWithSameNameButDifferentReturnTypes() {
    functionRegistry.addFunction(func);
    assertFalse(functionRegistry.addFunction(
        new KsqlFunction(Schema.INT64_SCHEMA, Collections.emptyList(), "func", Object.class)));
  }

  @Test
  public void shouldAddFunctionWithSameNameClassReturnTypeButDifferentArguments() {
    final KsqlFunction func2 = new KsqlFunction(Schema.STRING_SCHEMA,
        Collections.singletonList(Schema.INT64_SCHEMA), "func", Object.class);

    functionRegistry.addFunction(func);
    assertTrue(functionRegistry.addFunction(
        func2));
    assertThat(functionRegistry.getUdfFactory("func")
        .getFunction(Collections.singletonList(Schema.INT64_SCHEMA.type())), equalTo(func2));
    assertThat(functionRegistry.getUdfFactory("func")
        .getFunction(Collections.emptyList()), equalTo(func));
  }
}