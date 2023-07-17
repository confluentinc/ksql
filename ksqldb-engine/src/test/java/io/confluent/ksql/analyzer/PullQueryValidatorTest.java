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

package io.confluent.ksql.analyzer;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.analyzer.Analysis.Into;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.parser.OutputRefinement;
import io.confluent.ksql.parser.tree.GroupBy;
import io.confluent.ksql.parser.tree.PartitionBy;
import io.confluent.ksql.parser.tree.WindowExpression;
import io.confluent.ksql.serde.RefinementInfo;
import io.confluent.ksql.util.KsqlException;
import java.util.Optional;
import java.util.OptionalInt;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class PullQueryValidatorTest {

  private static final Expression AN_EXPRESSION = mock(Expression.class);

  @Mock
  private Analysis analysis;
  @Mock
  private WindowExpression windowExpression;
  @Mock
  private Into into;
  @Mock
  private RefinementInfo refinementInfo;

  private QueryValidator validator;

  @Before
  public void setUp() {
    validator = new PullQueryValidator();
    when(analysis.getRefinementInfo()).thenReturn(Optional.of(RefinementInfo.of(OutputRefinement.FINAL)));
  }

  @Test
  public void shouldThrowOnPullQueryThatHasRefinement() {
    // Given:
    when(analysis.getRefinementInfo()).thenReturn(Optional.of(refinementInfo));

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> validator.validate(analysis)
    );

    // Then:
    assertThat(e.getMessage(), containsString("Pull queries don't support EMIT clauses."));
  }

  @Test(expected = KsqlException.class)
  public void shouldThrowOnPullQueryIfSinkSupplied() {
    // Given:
    when(analysis.getInto()).thenReturn(Optional.of(into));

    // When:
    validator.validate(analysis);
  }

  @Test
  public void shouldThrowOnPullQueryThatIsJoin() {
    // Given:
    when(analysis.isJoin()).thenReturn(true);

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> validator.validate(analysis)
    );

    // Then:
    assertThat(e.getMessage(), containsString("Pull queries don't support JOIN clauses."));
  }

  @Test
  public void shouldThrowOnPullQueryThatIsWindowed() {
    // Given:

    when(analysis.getWindowExpression()).thenReturn(Optional.of(windowExpression));

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> validator.validate(analysis)
    );

    // Then:
    assertThat(e.getMessage(), containsString("Pull queries don't support WINDOW clauses."));
  }

  @Test
  public void shouldThrowOnGroupBy() {
    // Given:
    when(analysis.getGroupBy()).thenReturn(Optional.of(new GroupBy(
        Optional.empty(),
        ImmutableList.of(AN_EXPRESSION)
    )));

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> validator.validate(analysis)
    );

    // Then:
    assertThat(e.getMessage(), containsString("Pull queries don't support GROUP BY clauses."));
  }

  @Test
  public void shouldThrowOnPartitionBy() {
    // Given:
    when(analysis.getPartitionBy()).thenReturn(Optional.of(new PartitionBy(
        Optional.empty(),
        ImmutableList.of(AN_EXPRESSION)
    )));

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> validator.validate(analysis)
    );

    // Then:
    assertThat(e.getMessage(), containsString("Pull queries don't support PARTITION BY clauses."));
  }

  @Test
  public void shouldThrowOnHavingClause() {
    // Given:
    when(analysis.getHavingExpression()).thenReturn(Optional.of(AN_EXPRESSION));

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> validator.validate(analysis)
    );

    // Then:
    assertThat(e.getMessage(), containsString("Pull queries don't support HAVING clauses."));
  }

  @Test
  public void shouldThrowOnLimitClause() {
    // Given:
    when(analysis.getLimitClause()).thenReturn(OptionalInt.of(1));

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> validator.validate(analysis)
    );

    // Then:
    assertThat(e.getMessage(), containsString("Pull queries don't support LIMIT clauses."));
  }
}