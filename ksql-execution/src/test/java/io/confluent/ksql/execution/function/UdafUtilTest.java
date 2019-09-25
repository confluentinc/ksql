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

package io.confluent.ksql.execution.function;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.execution.expression.tree.ColumnReferenceExp;
import io.confluent.ksql.execution.expression.tree.FunctionCall;
import io.confluent.ksql.function.AggregateFunctionArguments;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.function.KsqlAggregateFunction;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.FunctionName;
import io.confluent.ksql.schema.ksql.ColumnRef;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import org.apache.kafka.connect.data.Schema;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class UdafUtilTest {
  private static final LogicalSchema SCHEMA = LogicalSchema.builder()
      .valueColumn(ColumnName.of("FOO"), SqlTypes.INTEGER)
      .valueColumn(ColumnName.of("BAR"), SqlTypes.BIGINT)
      .build();
  private static final FunctionCall FUNCTION_CALL = new FunctionCall(
      FunctionName.of("AGG"),
      ImmutableList.of(new ColumnReferenceExp(ColumnRef.of("BAR")))
  );

  @Mock
  private FunctionRegistry functionRegistry;
  @Mock
  private KsqlAggregateFunction function;
  @Mock
  private KsqlAggregateFunction resolved;
  @Captor
  private ArgumentCaptor<AggregateFunctionArguments> argumentsCaptor;

  @Before
  @SuppressWarnings("unchecked")
  public void init() {
    when(functionRegistry.getAggregate(any(), any())).thenReturn(function);
    when(function.getInstance(any())).thenReturn(resolved);
  }

  @Test
  public void shouldResolveUDAF() {
    // When:
    final KsqlAggregateFunction returned =
        UdafUtil.resolveAggregateFunction(functionRegistry, FUNCTION_CALL, SCHEMA);

    // Then:
    assertThat(returned, is(resolved));
  }

  @Test
  public void shouldGetAggregateWithCorrectName() {
    // When:
    UdafUtil.resolveAggregateFunction(functionRegistry, FUNCTION_CALL, SCHEMA);

    // Then:
    verify(functionRegistry).getAggregate(eq("AGG"), any());
  }

  @Test
  public void shouldGetAggregateWithCorrectType() {
    // When:
    UdafUtil.resolveAggregateFunction(functionRegistry, FUNCTION_CALL, SCHEMA);

    // Then:
    verify(functionRegistry).getAggregate(any(), eq(Schema.OPTIONAL_INT64_SCHEMA));
  }

  @Test
  public void shouldResolveWithCorrectArgs() {
    // When:
    UdafUtil.resolveAggregateFunction(functionRegistry, FUNCTION_CALL, SCHEMA);

    // Then:
    verify(function).getInstance(argumentsCaptor.capture());
    final AggregateFunctionArguments arguments = argumentsCaptor.getValue();
    assertThat(arguments.udafIndex(), equalTo(1));
  }
}