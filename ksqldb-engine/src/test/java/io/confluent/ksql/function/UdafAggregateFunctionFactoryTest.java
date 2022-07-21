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
import static org.mockito.AdditionalMatchers.not;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.function.udf.UdfMetadata;
import io.confluent.ksql.schema.ksql.SqlArgument;
import io.confluent.ksql.schema.ksql.types.SqlDecimal;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.util.KsqlException;
import java.util.Arrays;
import java.util.Collections;
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
    when(metadata.getName()).thenReturn("BOB");

    when(functionIndex.getFunction(
            not(eq(ImmutableList.of(
              SqlArgument.of(SqlTypes.STRING),
              SqlArgument.of(SqlDecimal.of(1, 0))
            )))
    )).thenReturn(invoker);
  }

  @Test
  public void shouldNotAppendInitParamTypesWhenLookingUpFunction() {
    // When:
    functionFactory.getFunction(
        ImmutableList.of(SqlTypes.STRING, SqlTypes.INTEGER)
    );

    // Then:
    verify(functionIndex).getFunction(ImmutableList.of(SqlArgument.of(SqlTypes.STRING), (SqlArgument.of(SqlTypes.INTEGER))));
  }

  @Test
  public void shouldHandleNullLiteralParams() {
    // When:
    functionFactory.getFunction(
        Arrays.asList(SqlTypes.STRING, null, SqlTypes.BIGINT)
    ).getRight().apply(new AggregateFunctionInitArguments(
            Collections.singletonList(0),
            ImmutableMap.of(),
            Arrays.asList(null, 5L)
    ));

    // Then:
    verify(functionIndex).getFunction(Arrays.asList(SqlArgument.of(SqlTypes.STRING), null, SqlArgument.of(SqlTypes.BIGINT)));
  }

  @Test
  public void shouldHandleInitParamsOfAllPrimitiveTypes() {
    // When:
    functionFactory.getFunction(
        ImmutableList.of(SqlTypes.STRING, SqlTypes.BOOLEAN, SqlTypes.INTEGER, SqlTypes.BIGINT,
                SqlTypes.DOUBLE, SqlTypes.STRING)
    ).getRight().apply(new AggregateFunctionInitArguments(
            Collections.singletonList(0),
            ImmutableMap.of(),
            ImmutableList.of(true, 1, 1L, 1.0d, "s")
    ));

    // Then: did not throw.
  }

  @Test
  public void shouldThrowOnUnsupportedInitParamType() {
    // When:
    final Exception e = assertThrows(KsqlException.class,
        () -> functionFactory.getFunction(
            ImmutableList.of(SqlTypes.STRING, SqlDecimal.of(1, 0))
        )
    );

    // Then:
    assertThat(e.getMessage(), is("There is no aggregate function with name='BOB' that has "
            + "arguments of type=STRING,DECIMAL"));
  }
}