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

package io.confluent.ksql.analyzer;

import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.when;

import io.confluent.ksql.engine.rewrite.ExpressionTreeRewriter;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.windows.HoppingWindowExpression;
import io.confluent.ksql.execution.windows.KsqlWindowExpression;
import io.confluent.ksql.execution.windows.SessionWindowExpression;
import io.confluent.ksql.execution.windows.TumblingWindowExpression;
import io.confluent.ksql.execution.windows.WindowTimeClause;
import io.confluent.ksql.parser.NodeLocation;
import io.confluent.ksql.parser.OutputRefinement;
import io.confluent.ksql.parser.tree.WindowExpression;
import io.confluent.ksql.serde.RefinementInfo;
import io.confluent.ksql.util.KsqlException;
import java.util.Optional;
import java.util.function.BiFunction;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

public class RewrittenAnalysisTest {

  @Mock
  private Analysis analysis;
  @Mock
  private BiFunction<Expression, ExpressionTreeRewriter.Context<Void>, Optional<Expression>> rewriter;
  @Mock
  private WindowTimeClause size;
  @Mock
  private WindowTimeClause advanceBy;
  @Mock
  private WindowTimeClause gap;
  @Mock
  private Optional<NodeLocation> location;
  @Mock
  private Optional<WindowTimeClause> retention;
  @Mock
  private HoppingWindowExpression hoppingWindow;
  @Mock
  private TumblingWindowExpression tumblingWindow;
  @Mock
  private SessionWindowExpression sessionWindow;
  @Mock
  private KsqlWindowExpression unsupportedWindowType;
  @Mock
  private Optional<WindowExpression> windowExpressionOptional;
  @Mock
  private WindowExpression windowExpression;
  @Mock
  private Optional<RefinementInfo> refinementInfoOptional;
  @Mock
  private RefinementInfo refinementInfo;
  @Mock
  private Optional<WindowTimeClause> gracePeriodOptional;

  private RewrittenAnalysis rewrittenAnalysis;
  private String windowName = "windowName";

  @Rule
  public final MockitoRule mockitoRule = MockitoJUnit.rule();

  @Before
  public void setUp() {
    rewrittenAnalysis = new RewrittenAnalysis(analysis, rewriter);

    when(analysis.getRefinementInfo()).thenReturn(refinementInfoOptional);
    when(refinementInfoOptional.isPresent()).thenReturn(true);
    when(refinementInfoOptional.get()).thenReturn(refinementInfo);
    when(refinementInfo.getOutputRefinement()).thenReturn(OutputRefinement.FINAL);
    when(analysis.getWindowExpression()).thenReturn(windowExpressionOptional);
    when(windowExpressionOptional.isPresent()).thenReturn(true);
    when(windowExpressionOptional.get()).thenReturn(windowExpression);
  }

  @Test
  public void shouldCreateNewTumblingWindowWithZeroGracePeriodDefault() {
    // Given:
    when(windowExpression.getKsqlWindowExpression()).thenReturn(tumblingWindow);
    when(tumblingWindow.getGracePeriod()).thenReturn(gracePeriodOptional);
    when(gracePeriodOptional.isPresent()).thenReturn(false);
    when(tumblingWindow.getLocation()).thenReturn(location);
    when(tumblingWindow.getRetention()).thenReturn(retention);
    when(tumblingWindow.getSize()).thenReturn(size);
    when(windowExpression.getWindowName()).thenReturn(windowName);


    // When:
    Optional<WindowExpression> result = rewrittenAnalysis.getWindowExpression();

    // Then:
    assertThat(result.get().getKsqlWindowExpression().getGracePeriod(), not(Optional.empty()));
  }

  @Test
  public void shouldCreateNewHoppingWindowWithZeroGracePeriodDefault() {
    // Given:
    when(windowExpression.getKsqlWindowExpression()).thenReturn(hoppingWindow);
    when(hoppingWindow.getGracePeriod()).thenReturn(gracePeriodOptional);
    when(gracePeriodOptional.isPresent()).thenReturn(false);
    when(hoppingWindow.getLocation()).thenReturn(location);
    when(hoppingWindow.getRetention()).thenReturn(retention);
    when(hoppingWindow.getSize()).thenReturn(size);
    when(hoppingWindow.getAdvanceBy()).thenReturn(advanceBy);
    when(windowExpression.getWindowName()).thenReturn(windowName);

    // When:
    Optional<WindowExpression> result = rewrittenAnalysis.getWindowExpression();

    // Then:
    assertThat(result.get().getKsqlWindowExpression().getGracePeriod(), not(Optional.empty()));
  }

  @Test
  public void shouldCreateNewSessionWindowWithZeroGracePeriodDefault() {
    // Given:
    when(windowExpression.getKsqlWindowExpression()).thenReturn(sessionWindow);
    when(sessionWindow.getGracePeriod()).thenReturn(gracePeriodOptional);
    when(gracePeriodOptional.isPresent()).thenReturn(false);
    when(sessionWindow.getLocation()).thenReturn(location);
    when(sessionWindow.getRetention()).thenReturn(retention);
    when(sessionWindow.getGap()).thenReturn(gap);
    when(windowExpression.getWindowName()).thenReturn(windowName);

    // When:
    Optional<WindowExpression> result = rewrittenAnalysis.getWindowExpression();

    // Then:
    assertThat(result.get().getKsqlWindowExpression().getGracePeriod(), not(Optional.empty()));
  }

  @Test
  public void shouldThrowIfUnsupportedWindowType() {
    // Given:
    when(windowExpression.getKsqlWindowExpression()).thenReturn(unsupportedWindowType);
    when(unsupportedWindowType.getGracePeriod()).thenReturn(gracePeriodOptional);
    when(gracePeriodOptional.isPresent()).thenReturn(false);

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> rewrittenAnalysis.getWindowExpression()
    );

    // Then
    assertThat(e.getMessage(), containsString("WINDOW type must be HOPPING, TUMBLING, or SESSION"));
  }
}
