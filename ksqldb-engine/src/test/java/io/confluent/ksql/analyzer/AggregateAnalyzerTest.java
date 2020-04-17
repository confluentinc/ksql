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

package io.confluent.ksql.analyzer;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.mock;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.expression.tree.FunctionCall;
import io.confluent.ksql.execution.expression.tree.QualifiedColumnReferenceExp;
import io.confluent.ksql.execution.expression.tree.UnqualifiedColumnReferenceExp;
import io.confluent.ksql.function.InternalFunctionRegistry;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.FunctionName;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.SchemaUtil;
import java.util.ArrayList;
import org.junit.Before;
import org.junit.Test;

public class AggregateAnalyzerTest {

  private static final SourceName ORDERS = SourceName.of("ORDERS");

  private static final QualifiedColumnReferenceExp DEFAULT_ARGUMENT =
      new QualifiedColumnReferenceExp(ORDERS, SchemaUtil.ROWTIME_NAME);

  private static final UnqualifiedColumnReferenceExp COL0 =
      new UnqualifiedColumnReferenceExp(ColumnName.of("COL0"));

  private static final UnqualifiedColumnReferenceExp COL1 =
      new UnqualifiedColumnReferenceExp(ColumnName.of("COL1"));

  private static final UnqualifiedColumnReferenceExp COL2 =
      new UnqualifiedColumnReferenceExp(ColumnName.of("COL2"));

  private static final FunctionCall FUNCTION_CALL = new FunctionCall(FunctionName.of("UCASE"),
      ImmutableList.of(COL0));

  private static final FunctionCall AGG_FUNCTION_CALL = new FunctionCall(FunctionName.of("MAX"),
      ImmutableList.of(COL0, COL1));

  private final InternalFunctionRegistry functionRegistry = new InternalFunctionRegistry();
  private MutableAggregateAnalysis analysis;
  private AggregateAnalyzer analyzer;

  @Before
  public void init() {
    analysis = new MutableAggregateAnalysis();
    analyzer = new AggregateAnalyzer(analysis, DEFAULT_ARGUMENT, false, functionRegistry);
  }

  @Test
  public void shouldCaptureSelectNonAggregateFunctionArguments() {
    // When:
    analyzer.processSelect(FUNCTION_CALL);

    // Then:
    assertThat(analysis.getNonAggregateSelectExpressions().get(FUNCTION_CALL), contains(COL0));
  }

  @Test
  public void shouldCaptureSelectDereferencedExpression() {
    // When:
    analyzer.processSelect(COL0);

    // Then:
    assertThat(analysis.getNonAggregateSelectExpressions().get(COL0), contains(COL0));
  }

  @Test
  public void shouldCaptureOtherSelectsWithEmptySet() {
    // Given:
    final Expression someExpression = mock(Expression.class);

    // When:
    analyzer.processSelect(someExpression);

    // Then:
    assertThat(analysis.getNonAggregateSelectExpressions().get(someExpression), is(empty()));
  }

  @Test
  public void shouldNotCaptureOtherNonAggregateFunctionArgumentsAsNonAggSelectColumns() {
    // When:
    analyzer.processGroupBy(FUNCTION_CALL);
    analyzer.processHaving(FUNCTION_CALL);

    // Then:
    assertThat(analysis.getNonAggregateSelectExpressions().keySet(), is(empty()));
  }

  @Test
  public void shouldNotCaptureAggregateFunctionArgumentsAsNonAggSelectColumns() {
    // When:
    analyzer.processSelect(AGG_FUNCTION_CALL);
    analyzer.processHaving(AGG_FUNCTION_CALL);

    // Then:
    assertThat(analysis.getNonAggregateSelectExpressions().keySet(), is(empty()));
  }

  @Test
  public void shouldThrowOnGroupByAggregateFunction() {
    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> analyzer.processGroupBy(AGG_FUNCTION_CALL)
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "GROUP BY does not support aggregate functions: MAX is an aggregate function."));
  }

  @Test
  public void shouldCaptureSelectNonAggregateFunctionArgumentsAsRequired() {
    // When:
    analyzer.processSelect(FUNCTION_CALL);

    // Then:
    assertThat(analysis.getRequiredColumns(), contains(COL0));
  }

  @Test
  public void shouldCaptureHavingNonAggregateFunctionArgumentsAsRequired() {
    // When:
    analyzer.processHaving(FUNCTION_CALL);

    // Then:
    assertThat(analysis.getRequiredColumns(), contains(COL0));
  }

  @Test
  public void shouldCaptureGroupByNonAggregateFunctionArgumentsAsRequired() {
    // When:
    analyzer.processGroupBy(FUNCTION_CALL);

    // Then:
    assertThat(analysis.getRequiredColumns(), contains(COL0));
  }

  @Test
  public void shouldCaptureSelectAggregateFunctionArgumentsAsRequired() {
    // When:
    analyzer.processSelect(AGG_FUNCTION_CALL);

    // Then:
    assertThat(analysis.getRequiredColumns(), contains(COL0, COL1));
  }

  @Test
  public void shouldCaptureHavingAggregateFunctionArgumentsAsRequired() {
    // When:
    analyzer.processHaving(AGG_FUNCTION_CALL);

    // Then:
    assertThat(analysis.getRequiredColumns(), contains(COL0, COL1));
  }

  @Test
  public void shouldNotCaptureNonAggregateFunction() {
    // When:
    analyzer.processSelect(FUNCTION_CALL);
    analyzer.processHaving(FUNCTION_CALL);
    analyzer.processGroupBy(FUNCTION_CALL);

    // Then:
    assertThat(analysis.getAggregateFunctions(), is(empty()));
  }

  @Test
  public void shouldCaptureSelectAggregateFunction() {
    // When:
    analyzer.processSelect(AGG_FUNCTION_CALL);

    // Then:
    assertThat(analysis.getAggregateFunctions(), contains(AGG_FUNCTION_CALL));
  }

  @Test
  public void shouldCaptureHavingAggregateFunction() {
    // When:
    analyzer.processHaving(AGG_FUNCTION_CALL);

    // Then:
    assertThat(analysis.getAggregateFunctions(), contains(AGG_FUNCTION_CALL));
  }

  @Test
  public void shouldThrowOnNestedSelectAggFunctions() {
    // Given:
    final FunctionCall nestedCall = new FunctionCall(FunctionName.of("MIN"),
        ImmutableList.of(AGG_FUNCTION_CALL, COL2));

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> analyzer.processSelect(nestedCall)
    );

    // Then:
    assertThat(e.getMessage(), containsString("Aggregate functions can not be nested: MIN(MAX())"));
  }

  @Test
  public void shouldThrowOnNestedHavingAggFunctions() {
    // Given:
    final FunctionCall nestedCall = new FunctionCall(FunctionName.of("MIN"),
        ImmutableList.of(AGG_FUNCTION_CALL, COL2));

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> analyzer.processHaving(nestedCall)
    );

    // Then:
    assertThat(e.getMessage(), containsString("Aggregate functions can not be nested: MIN(MAX())"));
  }

  @Test
  public void shouldCaptureNonAggregateFunctionArgumentsWithNestedAggFunction() {
    // Given:
    final FunctionCall nonAggWithNestedAggFunc = new FunctionCall(FunctionName.of("SUBSTRING"),
        ImmutableList.of(COL2, AGG_FUNCTION_CALL, COL1));

    // When:
    analyzer.processSelect(nonAggWithNestedAggFunc);

    // Then:
    assertThat(analysis.getAggregateSelectFields(), containsInAnyOrder(COL1, COL2));
  }

  @Test
  public void shouldNotCaptureNonAggregateFunctionArgumentsWhenNestedInsideAggFunction() {
    // Given:
    final FunctionCall nonAggFunc = new FunctionCall(FunctionName.of("ROUND"),
        ImmutableList.of(COL0));

    final FunctionCall aggFuncWithNestedNonAgg = new FunctionCall(FunctionName.of("MAX"),
        ImmutableList.of(COL1, nonAggFunc));

    // When:
    analyzer.processSelect(aggFuncWithNestedNonAgg);

    // Then:
    assertThat(analysis.getNonAggregateSelectExpressions().keySet(), is(empty()));
  }

  @Test
  public void shouldCaptureDefaultFunctionArguments() {
    // Given:
    final FunctionCall emptyFunc = new FunctionCall(FunctionName.of("COUNT"), new ArrayList<>());

    // When:
    analyzer.processSelect(emptyFunc);

    // Then:
    assertThat(analysis.getRequiredColumns(), contains(DEFAULT_ARGUMENT));
    assertThat(analysis.getAggregateFunctionArguments(), contains(DEFAULT_ARGUMENT));
  }

  @Test
  public void shouldAddDefaultArgToFunctionCallWithNoArgs() {
    // Given:
    final FunctionCall emptyFunc = new FunctionCall(FunctionName.of("COUNT"), new ArrayList<>());

    // When:
    analyzer.processSelect(emptyFunc);

    // Then:
    assertThat(analysis.getAggregateFunctions(), hasSize(1));
    assertThat(analysis.getAggregateFunctions().get(0).getName(), is(emptyFunc.getName()));
    assertThat(analysis.getAggregateFunctions().get(0).getArguments(), contains(DEFAULT_ARGUMENT));
  }

  @Test
  public void shouldNotCaptureWindowStartAsRequiredColumn() {
    // When:
    analyzer.processSelect(new QualifiedColumnReferenceExp(
        ORDERS,
        SchemaUtil.WINDOWSTART_NAME
    ));

    // Then:
    assertThat(analysis.getRequiredColumns(), is(empty()));
  }

  @Test
  public void shouldNotCaptureWindowEndAsRequiredColumn() {
    // When:
    analyzer.processSelect(new QualifiedColumnReferenceExp(
        ORDERS,
        SchemaUtil.WINDOWEND_NAME
    ));

    // Then:
    assertThat(analysis.getRequiredColumns(), is(empty()));
  }
}
