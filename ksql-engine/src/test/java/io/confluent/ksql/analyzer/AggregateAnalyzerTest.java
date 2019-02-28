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
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.function.InternalFunctionRegistry;
import io.confluent.ksql.parser.tree.DereferenceExpression;
import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.parser.tree.FunctionCall;
import io.confluent.ksql.parser.tree.QualifiedName;
import io.confluent.ksql.parser.tree.QualifiedNameReference;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.SchemaUtil;
import java.util.ArrayList;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

@SuppressWarnings("unchecked")
public class AggregateAnalyzerTest {

  private static final DereferenceExpression DEFAULT_ARGUMENT = new DereferenceExpression(
      new QualifiedNameReference(QualifiedName.of("ORDERS")), SchemaUtil.ROWTIME_NAME);

  private static final DereferenceExpression COL0 = new DereferenceExpression(
      new QualifiedNameReference(QualifiedName.of("ORDERS")), "COL0");

  private static final DereferenceExpression COL1 = new DereferenceExpression(
      new QualifiedNameReference(QualifiedName.of("ORDERS")), "COL1");

  private static final DereferenceExpression COL2 = new DereferenceExpression(
      new QualifiedNameReference(QualifiedName.of("ORDERS")), "COL2");

  private static final FunctionCall FUNCTION_CALL = new FunctionCall(QualifiedName.of("UCASE"),
      ImmutableList.of(COL0));

  private static final FunctionCall AGG_FUNCTION_CALL = new FunctionCall(QualifiedName.of("MAX"),
      ImmutableList.of(COL0, COL1));

  private final InternalFunctionRegistry functionRegistry = new InternalFunctionRegistry();
  private MutableAggregateAnalysis analysis;
  private AggregateAnalyzer analyzer;

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Before
  public void init() {
    analysis = new MutableAggregateAnalysis();
    analyzer = new AggregateAnalyzer(analysis, DEFAULT_ARGUMENT, functionRegistry);
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
    // Then:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage(
        "GROUP BY does not support aggregate functions: MAX is an aggregate function.");

    // When:
    analyzer.processGroupBy(AGG_FUNCTION_CALL);
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
    final FunctionCall nestedCall = new FunctionCall(QualifiedName.of("MIN"),
        ImmutableList.of(AGG_FUNCTION_CALL, COL2));

    // Then:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("Aggregate functions can not be nested: MIN(MAX())");

    // When:
    analyzer.processSelect(nestedCall);
  }

  @Test
  public void shouldThrowOnNestedHavingAggFunctions() {
    // Given:
    final FunctionCall nestedCall = new FunctionCall(QualifiedName.of("MIN"),
        ImmutableList.of(AGG_FUNCTION_CALL, COL2));

    // Then:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("Aggregate functions can not be nested: MIN(MAX())");

    // When:
    analyzer.processHaving(nestedCall);
  }

  @Test
  public void shouldCaptureNonAggregateFunctionArgumentsWithNestedAggFunction() {
    // Given:
    final FunctionCall nonAggWithNestedAggFunc = new FunctionCall(QualifiedName.of("SUBSTRING"),
        ImmutableList.of(COL2, AGG_FUNCTION_CALL, COL1));

    // When:
    analyzer.processSelect(nonAggWithNestedAggFunc);

    // Then:
    assertThat(analysis.getAggregateSelectFields(), containsInAnyOrder(COL1, COL2));
  }

  @Test
  public void shouldNotCaptureNonAggregateFunctionArgumentsWhenNestedInsideAggFunction() {
    // Given:
    final FunctionCall nonAggFunc = new FunctionCall(QualifiedName.of("ROUND"),
        ImmutableList.of(COL0));

    final FunctionCall aggFuncWithNestedNonAgg = new FunctionCall(QualifiedName.of("MAX"),
        ImmutableList.of(COL1, nonAggFunc));

    // When:
    analyzer.processSelect(aggFuncWithNestedNonAgg);

    // Then:
    assertThat(analysis.getNonAggregateSelectExpressions().keySet(), is(empty()));
  }

  @Test
  public void shouldCaptureDefaultFunctionArguments() {
    // Given:
    final FunctionCall emptyFunc = new FunctionCall(QualifiedName.of("COUNT"), new ArrayList<>());

    // When:
    analyzer.processSelect(emptyFunc);

    // Then:
    assertThat(analysis.getAggregateFunctions(), containsInAnyOrder(emptyFunc));
    assertThat(analysis.getRequiredColumns(), contains(DEFAULT_ARGUMENT));
  }

  @Test
  public void shouldAddDefaultArgToFunctionCallWithNoArgs() {
    // Given:
    final FunctionCall emptyFunc = new FunctionCall(QualifiedName.of("COUNT"), new ArrayList<>());

    // When:
    analyzer.processSelect(emptyFunc);

    // Then:
    assertThat(emptyFunc.getArguments(), contains(DEFAULT_ARGUMENT));
  }
}
