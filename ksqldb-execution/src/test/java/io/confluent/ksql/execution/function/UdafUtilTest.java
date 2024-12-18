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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.AdditionalMatchers.not;
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
import io.confluent.ksql.function.AggregateFunctionFactory;
import io.confluent.ksql.function.AggregateFunctionInitArguments;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.function.KsqlAggregateFunction;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.FunctionName;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.SystemColumns;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import java.util.Arrays;
import java.util.Collections;
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
  private AggregateFunctionFactory functionFactory;
  @Mock
  private FunctionCall functionCall;
  @Captor
  private ArgumentCaptor<AggregateFunctionInitArguments> argumentsCaptor;

  @Before
  @SuppressWarnings("unchecked")
  public void init() {
    when(functionCall.getName()).thenReturn(FUNCTION_NAME);
    when(functionRegistry.getAggregateFactory(any())).thenReturn(functionFactory);
    when(functionFactory.getFunction(not(eq(Arrays.asList(SqlTypes.INTEGER, SqlTypes.STRING, SqlTypes.STRING)))))
            .thenReturn(new AggregateFunctionFactory.FunctionSource(0, (initArgs) -> function));
    when(functionFactory.getFunction(eq(Arrays.asList(SqlTypes.INTEGER, SqlTypes.STRING, SqlTypes.STRING))))
            .thenReturn(new AggregateFunctionFactory.FunctionSource(1, (initArgs) -> function));
  }

  @Test
  public void shouldResolveUDAF() {
    // When:
    final KsqlAggregateFunction returned =
        UdafUtil.resolveAggregateFunction(functionRegistry, FUNCTION_CALL, SCHEMA, KsqlConfig.empty());

    // Then:
    assertThat(returned, is(function));
  }

  @Test
  public void shouldGetAggregateWithCorrectName() {
    // When:
    UdafUtil.resolveAggregateFunction(functionRegistry, FUNCTION_CALL, SCHEMA, KsqlConfig.empty());

    // Then:
    verify(functionRegistry).getAggregateFactory(eq(FUNCTION_NAME));
  }

  @Test
  public void shouldGetAggregateWithCorrectType() {
    // When:
    UdafUtil.resolveAggregateFunction(functionRegistry, FUNCTION_CALL, SCHEMA, KsqlConfig.empty());

    // Then:
    verify(functionFactory).getFunction(
            eq(Collections.singletonList(SqlTypes.BIGINT))
    );
  }


  @Test
  public void shouldNotThrowIfFirstParamNotALiteral() {
    // Given:
    when(functionCall.getArguments()).thenReturn(ImmutableList.of(
        new UnqualifiedColumnReferenceExp(ColumnName.of("Bob")),
        new StringLiteral("No issue here")
    ));

    // When:
    UdafUtil.createAggregateFunctionInitArgs(
            Math.max(0, functionCall.getArguments().size() - 1),
            Collections.singletonList(0),
            functionCall,
            KsqlConfig.empty()
    );

    // Then: did not throw.
  }

  @Test
  public void shouldNotThrowIfSecondParamIsColArgAndIsNotALiteral() {
    // Given:
    when(functionCall.getArguments()).thenReturn(ImmutableList.of(
            new UnqualifiedColumnReferenceExp(ColumnName.of("Bob")),
            new UnqualifiedColumnReferenceExp(ColumnName.of("Col2")),
            new StringLiteral("No issue here")
    ));

    // When:
    UdafUtil.createAggregateFunctionInitArgs(
            Math.max(0, functionCall.getArguments().size() - 2),
            Arrays.asList(0, 1),
            functionCall,
            KsqlConfig.empty()
    );

    // Then: did not throw.
  }

  @Test
  public void shouldThrowIfSecondParamIsInitArgAndNotALiteral() {
    // Given:
    when(functionCall.getArguments()).thenReturn(ImmutableList.of(
        new UnqualifiedColumnReferenceExp(ColumnName.of("Bob")),
        new UnqualifiedColumnReferenceExp(ColumnName.of("Not good!")),
        new StringLiteral("No issue here")
    ));

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> UdafUtil.createAggregateFunctionInitArgs(
                Math.max(0, functionCall.getArguments().size() - 1),
                Collections.singletonList(0),
                functionCall, KsqlConfig.empty()
        )
    );

    // Then:
    assertThat(e.getMessage(), is("Parameter 2 passed to function AGG must be a literal constant, "
        + "but was expression: 'Not good!'"));
  }

  @Test
  public void shouldThrowIfSecondParamIsColArgAndNotACol() {
    // Given:
    when(functionCall.getArguments()).thenReturn(ImmutableList.of(
            new UnqualifiedColumnReferenceExp(ColumnName.of("FOO")),
            new StringLiteral("Not good!"),
            new StringLiteral("No issue here")
    ));

    // When:
    final Exception e = assertThrows(
            KsqlException.class,
            () -> UdafUtil.resolveAggregateFunction(
                    functionRegistry,
                    functionCall,
                    SCHEMA,
                    KsqlConfig.empty()
            )
    );

    // Then:
    assertThat(e.getMessage(), is("Failed to create aggregate function: functionCall"));
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
        () -> UdafUtil.createAggregateFunctionInitArgs(
                Math.max(0, functionCall.getArguments().size() - 1),
                Collections.singletonList(0),
                functionCall,
                KsqlConfig.empty()
        )
    );

    // Then:
    assertThat(e.getMessage(), is("Parameter 4 passed to function AGG must be a literal constant, "
        + "but was expression: 'Not good!'"));
  }

  @Test
  public void shouldCreateDummyArgs() {
    // Given:
    when(functionCall.getArguments()).thenReturn(ImmutableList.of(
            new UnqualifiedColumnReferenceExp(ColumnName.of("FOO")),
            new UnqualifiedColumnReferenceExp(ColumnName.of("Bob")),
            new StringLiteral("No issue here")
    ));

    // When:
    AggregateFunctionInitArguments initArgs = UdafUtil.createAggregateFunctionInitArgs(
            1,
            functionCall
    );

    // Then:
    assertEquals(0, initArgs.udafIndices().size());
    assertEquals(1, initArgs.args().size());
    assertEquals("No issue here", initArgs.arg(0));
    assertTrue(initArgs.config().isEmpty());
  }
}