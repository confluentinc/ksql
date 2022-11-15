/*
 * Copyright 2020 Confluent Inc.
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
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.function.udf.UdfMetadata;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import java.math.BigDecimal;
import java.util.Arrays;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class UdafAggregateFunctionFactoryTest {

  @Mock
  private UdfMetadata metadata;
  @Mock
  private UdfIndex<UdafFactoryInvoker> functionIndex;
  @Mock
  private UdafFactoryInvoker invoker;

  private UdafAggregateFunctionFactory functionFactory;

  @Before
  public void setUp() {
    functionFactory = new UdafAggregateFunctionFactory(metadata, functionIndex);

    when(functionIndex.getFunction(any())).thenReturn(invoker);
    when(metadata.getName()).thenReturn("BOB");
  }

  @Test
  public void shouldAppendInitParamTypesWhenLookingUpFunction() {
    // When:
    functionFactory.createAggregateFunction(
        ImmutableList.of(SqlTypes.STRING),
        new AggregateFunctionInitArguments(0, ImmutableList.of(1))
    );

    // Then:
    verify(functionIndex).getFunction(ImmutableList.of(SqlTypes.STRING, SqlTypes.INTEGER));
  }

  @Test
  public void shouldHandleNullLiteralParams() {
    // When:
    functionFactory.createAggregateFunction(
        ImmutableList.of(SqlTypes.STRING),
        new AggregateFunctionInitArguments(0, Arrays.asList(null, 5L))
    );

    // Then:
    verify(functionIndex).getFunction(Arrays.asList(SqlTypes.STRING, null, SqlTypes.BIGINT));
  }

  @Test
  public void shouldHandleInitParamsOfAllPrimitiveTypes() {
    // When:
    functionFactory.createAggregateFunction(
        ImmutableList.of(SqlTypes.STRING),
        new AggregateFunctionInitArguments(0, ImmutableList.of(true, 1, 1L, 1.0d, "s"))
    );

    // Then: did not throw.
  }

  @Test
  public void shouldThrowOnUnsupportedInitParamType() {
    // When:
    final Exception e = assertThrows(KsqlFunctionException.class,
        () -> functionFactory.createAggregateFunction(
            ImmutableList.of(SqlTypes.STRING),
            new AggregateFunctionInitArguments(0, ImmutableList.of(BigDecimal.ONE))
        )
    );

    // Then:
    assertThat(e.getMessage(), is("Only primitive init arguments are supported by UDAF BOB, "
        + "but got " + BigDecimal.ONE));
  }
}