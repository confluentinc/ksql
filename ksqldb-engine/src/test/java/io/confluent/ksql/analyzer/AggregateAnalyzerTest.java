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
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.execution.expression.tree.ColumnReferenceExp;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.expression.tree.FunctionCall;
import io.confluent.ksql.execution.expression.tree.QualifiedColumnReferenceExp;
import io.confluent.ksql.execution.expression.tree.UnqualifiedColumnReferenceExp;
import io.confluent.ksql.execution.plan.SelectExpression;
import io.confluent.ksql.function.InternalFunctionRegistry;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.FunctionName;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.parser.tree.WindowExpression;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.SchemaUtil;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class AggregateAnalyzerTest {

  private static final UnqualifiedColumnReferenceExp DEFAULT_ARGUMENT =
      new UnqualifiedColumnReferenceExp(SchemaUtil.ROWTIME_NAME);

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

  private static final FunctionCall REQUIRED_AGG_FUNC_CALL = new FunctionCall(
      FunctionName.of("MAX"),
      ImmutableList.of(new UnqualifiedColumnReferenceExp(ColumnName.of("AGG_COL")))
  );

  @Mock
  private ImmutableAnalysis analysis;

  private final InternalFunctionRegistry functionRegistry = new InternalFunctionRegistry();
  private AggregateAnalyzer analyzer;

  private List<SelectExpression> selects;

  @Before
  public void init() {
    analyzer = new AggregateAnalyzer(functionRegistry);

    givenGroupByExpressions(COL0, COL1);

    selects = new ArrayList<>();
    // Aggregate requires at least one aggregation column:
    selects.add(SelectExpression.of(ColumnName.of("AGG_COLUMN"), REQUIRED_AGG_FUNC_CALL));

    when(analysis.getDefaultArgument()).thenReturn(DEFAULT_ARGUMENT);
  }

  @Test
  public void shouldCaptureSelectNonAggregateFunctionArguments() {
    // Given:
    givenSelectExpression(FUNCTION_CALL);

    // When:
    final MutableAggregateAnalysis result = (MutableAggregateAnalysis) analyzer
        .analyze(analysis, selects);

    // Then:
    assertThat(result.getNonAggregateSelectExpressions().get(FUNCTION_CALL), contains(COL0));
  }

  @Test
  public void shouldCaptureSelectDereferencedExpression() {
    // Given:
    givenSelectExpression(COL0);

    // When:
    final MutableAggregateAnalysis result = (MutableAggregateAnalysis) analyzer
        .analyze(analysis, selects);

    // Then:
    assertThat(result.getNonAggregateSelectExpressions().get(COL0), contains(COL0));
  }

  @Test
  public void shouldCaptureOtherSelectsWithEmptySet() {
    // Given:
    final Expression someExpression = mock(Expression.class);
    givenSelectExpression(someExpression);

    // When:
    final MutableAggregateAnalysis result = (MutableAggregateAnalysis) analyzer
        .analyze(analysis, selects);

    // Then:
    assertThat(result.getNonAggregateSelectExpressions().get(someExpression), is(empty()));
  }

  @Test
  public void shouldNotCaptureOtherNonAggregateFunctionArgumentsAsNonAggSelectColumns() {
    // Given:
    givenGroupByExpressions(COL0);

    givenHavingExpression(FUNCTION_CALL);

    // When:
    final MutableAggregateAnalysis result = (MutableAggregateAnalysis) analyzer
        .analyze(analysis, selects);

    // Then:
    assertThat(result.getNonAggregateSelectExpressions().keySet(), is(empty()));
  }

  @Test
  public void shouldNotCaptureAggregateFunctionArgumentsAsNonAggSelectColumns() {
    // Given:
    givenHavingExpression(AGG_FUNCTION_CALL);

    // When:
    final MutableAggregateAnalysis result = (MutableAggregateAnalysis) analyzer
        .analyze(analysis, selects);

    // Then:
    assertThat(result.getNonAggregateSelectExpressions().keySet(), is(empty()));
  }

  @Test
  public void shouldThrowOnGroupByAggregateFunction() {
    // Given:
    givenGroupByExpressions(AGG_FUNCTION_CALL);

    // When:
    final KsqlException e = assertThrows(KsqlException.class,
        () -> analyzer.analyze(analysis, selects));

    // Then:
    assertThat(e.getMessage(), containsString(
        "GROUP BY does not support aggregate functions: MAX is an aggregate function."));
  }

  @Test
  public void shouldCaptureHavingNonAggregateFunctionArgumentsAsRequired() {
    // Given:
    when(analysis.getHavingExpression()).thenReturn(Optional.of(
        new FunctionCall(FunctionName.of("MAX"),
            ImmutableList.of(COL2))
    ));

    // When:
    final AggregateAnalysisResult result = analyzer
        .analyze(analysis, selects);

    // Then:
    assertThat(result.getRequiredColumns(), hasItem(COL2));
  }

  @Test
  public void shouldCaptureGroupByNonAggregateFunctionArgumentsAsRequired() {
    // Given:
    givenGroupByExpressions(COL0, COL1);

    // When:
    final AggregateAnalysisResult result = analyzer.analyze(analysis, selects);

    // Then:
    assertThat(result.getRequiredColumns(), hasItems(COL0, COL1));
  }

  @Test
  public void shouldCaptureSelectAggregateFunctionArgumentsAsRequired() {
    // Given:
    givenSelectExpression(AGG_FUNCTION_CALL);

    // When:
    final AggregateAnalysisResult result = analyzer.analyze(analysis, selects);

    // Then:
    assertThat(result.getRequiredColumns(), hasItems(COL0, COL1));
  }

  @Test
  public void shouldCaptureHavingAggregateFunctionArgumentsAsRequired() {
    // Given:
    givenHavingExpression(AGG_FUNCTION_CALL);

    // When:
    final AggregateAnalysisResult result = analyzer.analyze(analysis, selects);

    // Then:
    assertThat(result.getRequiredColumns(), hasItems(COL0, COL1));
  }

  @Test
  public void shouldNotCaptureNonAggregateFunction() {
    // given:
    givenSelectExpression(FUNCTION_CALL);
    givenHavingExpression(FUNCTION_CALL);

    // When:
    final AggregateAnalysisResult result = analyzer.analyze(analysis, selects);

    // Then:
    assertThat(result.getAggregateFunctions(), contains(REQUIRED_AGG_FUNC_CALL));
  }

  @Test
  public void shouldNotCaptureNonAggregateGroupByFunction() {
    // given:
    givenGroupByExpressions(FUNCTION_CALL);

    // When:
    final AggregateAnalysisResult result = analyzer.analyze(analysis, selects);

    // Then:
    assertThat(result.getAggregateFunctions(), contains(REQUIRED_AGG_FUNC_CALL));
  }

  @Test
  public void shouldCaptureSelectAggregateFunction() {
    // Given:
    givenSelectExpression(AGG_FUNCTION_CALL);

    // When:
    final AggregateAnalysisResult result = analyzer.analyze(analysis, selects);

    // Then:
    assertThat(result.getAggregateFunctions(), hasItem(AGG_FUNCTION_CALL));
  }

  @Test
  public void shouldCaptureHavingAggregateFunction() {
    // Given:
    givenHavingExpression(AGG_FUNCTION_CALL);

    // When:
    final AggregateAnalysisResult result = analyzer.analyze(analysis, selects);

    // Then:
    assertThat(result.getAggregateFunctions(), hasItem(AGG_FUNCTION_CALL));
  }

  @Test
  public void shouldThrowOnNestedSelectAggFunctions() {
    // Given:
    final FunctionCall nestedCall = new FunctionCall(FunctionName.of("MIN"),
        ImmutableList.of(AGG_FUNCTION_CALL, COL2));

    givenSelectExpression(nestedCall);

    // When:
    final KsqlException e = assertThrows(KsqlException.class,
        () -> analyzer.analyze(analysis, selects));

    // Then:
    assertThat(e.getMessage(),
        containsString("Aggregate functions can not be nested: MIN(MAX())"));
  }

  @Test
  public void shouldThrowOnNestedHavingAggFunctions() {
    // Given:
    final FunctionCall nestedCall = new FunctionCall(FunctionName.of("MIN"),
        ImmutableList.of(AGG_FUNCTION_CALL, COL2));

    givenHavingExpression(nestedCall);

    // When:
    final KsqlException e = assertThrows(KsqlException.class,
        () -> analyzer.analyze(analysis, selects));

    // Then:
    assertThat(e.getMessage(),
        containsString("Aggregate functions can not be nested: MIN(MAX())"));
  }

  @Test
  public void shouldNotCaptureNonAggregateFunctionArgumentsWhenNestedInsideAggFunction() {
    // Given:
    final FunctionCall nonAggFunc = new FunctionCall(FunctionName.of("ROUND"),
        ImmutableList.of(COL0));

    final FunctionCall aggFuncWithNestedNonAgg = new FunctionCall(FunctionName.of("MAX"),
        ImmutableList.of(COL1, nonAggFunc));

    givenSelectExpression(aggFuncWithNestedNonAgg);

    // When:
    final MutableAggregateAnalysis result = (MutableAggregateAnalysis) analyzer
        .analyze(analysis, selects);

    // Then:
    assertThat(result.getNonAggregateSelectExpressions().keySet(), is(empty()));
  }

  @Test
  public void shouldCaptureDefaultFunctionArguments() {
    // Given:
    final FunctionCall emptyFunc = new FunctionCall(FunctionName.of("COUNT"), new ArrayList<>());

    givenSelectExpression(emptyFunc);

    // When:
    final AggregateAnalysisResult result = analyzer.analyze(analysis, selects);

    // Then:
    assertThat(result.getRequiredColumns(), hasItem(DEFAULT_ARGUMENT));
    assertThat(result.getAggregateFunctionArguments(), hasItem(DEFAULT_ARGUMENT));
  }

  @Test
  public void shouldAddDefaultArgToFunctionCallWithNoArgs() {
    // Given:
    final FunctionCall emptyFunc = new FunctionCall(FunctionName.of("COUNT"), new ArrayList<>());

    givenSelectExpression(emptyFunc);

    // When:
    final AggregateAnalysisResult result = analyzer.analyze(analysis, selects);

    // Then:
    assertThat(result.getAggregateFunctions(), hasSize(2));
    assertThat(result.getAggregateFunctions().get(1).getName(), is(emptyFunc.getName()));
    assertThat(result.getAggregateFunctions().get(1).getArguments(), contains(DEFAULT_ARGUMENT));
  }

  @Test
  public void shouldNotCaptureWindowStartAsRequiredColumn() {
    // Given:
    givenWindowExpression();
    givenSelectExpression(new UnqualifiedColumnReferenceExp(SchemaUtil.WINDOWSTART_NAME));

    // When:
    final AggregateAnalysisResult result = analyzer.analyze(analysis, selects);

    // Then:
    final List<ColumnName> requiredColumnNames = result.getRequiredColumns().stream()
        .map(ColumnReferenceExp::getColumnName)
        .collect(Collectors.toList());

    assertThat(requiredColumnNames, not(hasItem(SchemaUtil.WINDOWSTART_NAME)));
  }

  @Test
  public void shouldNotCaptureWindowEndAsRequiredColumn() {
    // Given:
    givenWindowExpression();
    givenSelectExpression(new UnqualifiedColumnReferenceExp(SchemaUtil.WINDOWEND_NAME));

    // When:
    final AggregateAnalysisResult result = analyzer.analyze(analysis, selects);

    // Then:
    final List<ColumnName> requiredColumnNames = result.getRequiredColumns().stream()
        .map(ColumnReferenceExp::getColumnName)
        .collect(Collectors.toList());

    assertThat(requiredColumnNames, not(hasItem(SchemaUtil.WINDOWEND_NAME)));
  }

  @Test
  public void shouldThrowOnQualifiedColumnReference() {
    // Given:
    givenSelectExpression(new QualifiedColumnReferenceExp(
        SourceName.of("Fred"),
        SchemaUtil.WINDOWEND_NAME
    ));

    // When:
    assertThrows(UnsupportedOperationException.class,
        () -> analyzer.analyze(analysis, selects));
  }

  private void givenSelectExpression(final Expression expression) {
    selects.add(SelectExpression.of(ColumnName.of("x"), expression));
  }

  private void givenGroupByExpressions(final Expression... expressions) {
    when(analysis.getGroupByExpressions())
        .thenReturn(ImmutableList.copyOf(expressions));
  }

  private void givenHavingExpression(final Expression expression) {
    when(analysis.getHavingExpression())
        .thenReturn(Optional.of(expression));
  }

  private void givenWindowExpression() {
    final WindowExpression windowExpression = mock(WindowExpression.class);
    when(analysis.getWindowExpression())
        .thenReturn(Optional.of(windowExpression));
  }
}
