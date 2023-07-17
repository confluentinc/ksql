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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.execution.expression.tree.DoubleLiteral;
import io.confluent.ksql.execution.expression.tree.FunctionCall;
import io.confluent.ksql.execution.expression.tree.LongLiteral;
import io.confluent.ksql.execution.expression.tree.StringLiteral;
import io.confluent.ksql.execution.expression.tree.UnqualifiedColumnReferenceExp;
import io.confluent.ksql.function.AggregateFunctionInitArguments;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.function.KsqlAggregateFunction;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.FunctionName;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.SystemColumns;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.util.KsqlException;
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
      .keyColumn(SystemColumns.ROWKEY_NAME, SqlTypes.STRING)
      .valueColumn(ColumnName.of("FOO"), SqlTypes.INTEGER)
      .valueColumn(ColumnName.of("BAR"), SqlTypes.BIGINT)
      .build();

  private static final FunctionName FUNCTION_NAME = FunctionName.of("AGG");

  private static final FunctionCall FUNCTION_CALL = new FunctionCall(
      FUNCTION_NAME,
      ImmutableList.of(new UnqualifiedColumnReferenceExp(ColumnName.of("BAR")))
  );

  @Mock
  private FunctionRegistry functionRegistry;
  @Mock
  private KsqlAggregateFunction function;
  @Mock
  private FunctionCall functionCall;
  @Captor
  private ArgumentCaptor<AggregateFunctionInitArguments> argumentsCaptor;

  @Before
  @SuppressWarnings("unchecked")
  public void init() {
    when(functionCall.getName()).thenReturn(FUNCTION_NAME);
    when(functionRegistry.getAggregateFunction(any(), any(), any())).thenReturn(function);
  }

  @Test
  public void shouldResolveUDAF() {
    // When:
    final KsqlAggregateFunction returned =
        UdafUtil.resolveAggregateFunction(functionRegistry, FUNCTION_CALL, SCHEMA);

    // Then:
    assertThat(returned, is(function));
  }

  @Test
  public void shouldGetAggregateWithCorrectName() {
    // When:
    UdafUtil.resolveAggregateFunction(functionRegistry, FUNCTION_CALL, SCHEMA);

    // Then:
    verify(functionRegistry).getAggregateFunction(eq(FUNCTION_NAME), any(), any());
  }

  @Test
  public void shouldGetAggregateWithCorrectType() {
    // When:
    UdafUtil.resolveAggregateFunction(functionRegistry, FUNCTION_CALL, SCHEMA);

    // Then:
    verify(functionRegistry).getAggregateFunction(any(), eq(SqlTypes.BIGINT), any());
  }


  @Test
  public void shouldNotThrowIfFirstParamNotALiteral() {
    // Given:
    when(functionCall.getArguments()).thenReturn(ImmutableList.of(
        new UnqualifiedColumnReferenceExp(ColumnName.of("Bob")),
        new StringLiteral("No issue here")
    ));

    // When:
    UdafUtil.createAggregateFunctionInitArgs(0, functionCall);

    // Then: did not throw.
  }

  @Test
  public void shouldThrowIfSecondParamIsNotALiteral() {
    // Given:
    when(functionCall.getArguments()).thenReturn(ImmutableList.of(
        new UnqualifiedColumnReferenceExp(ColumnName.of("Bob")),
        new UnqualifiedColumnReferenceExp(ColumnName.of("Not good!")),
        new StringLiteral("No issue here")
    ));

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> UdafUtil.createAggregateFunctionInitArgs(0, functionCall)
    );

    // Then:
    assertThat(e.getMessage(), is("Parameter 2 passed to function AGG must be a literal constant, "
        + "but was expression: 'Not good!'"));
  }

  @Test
  public void shouldThrowIfSubsequentParamsAreNotLiteral() {
    // Given:
    when(functionCall.getArguments()).thenReturn(ImmutableList.of(
        new UnqualifiedColumnReferenceExp(ColumnName.of("Bob")),
        new LongLiteral(10),
        new DoubleLiteral(1.0),
        new UnqualifiedColumnReferenceExp(ColumnName.of("Not good!"))
    ));

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> UdafUtil.createAggregateFunctionInitArgs(0, functionCall)
    );

    // Then:
    assertThat(e.getMessage(), is("Parameter 4 passed to function AGG must be a literal constant, "
        + "but was expression: 'Not good!'"));
  }
}