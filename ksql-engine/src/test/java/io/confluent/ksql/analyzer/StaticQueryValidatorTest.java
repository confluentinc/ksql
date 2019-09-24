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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.analyzer.Analysis.Into;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.parser.tree.ResultMaterialization;
import io.confluent.ksql.parser.tree.WindowExpression;
import io.confluent.ksql.util.KsqlException;
import java.util.Optional;
import java.util.OptionalInt;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class StaticQueryValidatorTest {

  private static final Expression AN_EXPRESSION = mock(Expression.class);

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Mock
  private Analysis analysis;
  @Mock
  private WindowExpression windowExpression;
  @Mock
  private Into into;

  private QueryValidator validator;

  @Before
  public void setUp() {
    validator = new StaticQueryValidator();

    when(analysis.getResultMaterialization()).thenReturn(ResultMaterialization.FINAL);
  }

  @Test
  public void shouldThrowOnStaticQueryThatIsNotFinal() {
    // Given:
    when(analysis.getResultMaterialization()).thenReturn(ResultMaterialization.CHANGES);

    // Then:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("Static queries don't support `EMIT CHANGES`");

    // When:
    validator.validate(analysis);
  }

  @Test(expected = KsqlException.class)
  public void shouldThrowOnStaticQueryIfSinkSupplied() {
    // Given:
    when(analysis.getInto()).thenReturn(Optional.of(into));

    // When:
    validator.validate(analysis);
  }

  @Test
  public void shouldThrowOnStaticQueryThatIsJoin() {
    // Given:
    when(analysis.isJoin()).thenReturn(true);

    // Then:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("Static queries don't support JOIN clauses.");

    // When:
    validator.validate(analysis);
  }

  @Test
  public void shouldThrowOnStaticQueryThatIsWindowed() {
    // Given:

    when(analysis.getWindowExpression()).thenReturn(Optional.of(windowExpression));

    // Then:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("Static queries don't support WINDOW clauses.");

    // When:
    validator.validate(analysis);
  }

  @Test
  public void shouldThrowOnStaticQueryThatHasGroupBy() {
    // Given:
    when(analysis.getGroupByExpressions()).thenReturn(ImmutableList.of(AN_EXPRESSION));

    // Then:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("Static queries don't support GROUP BY clauses.");

    // When:
    validator.validate(analysis);
  }

  @Test
  public void shouldThrowOnStaticQueryThatHasPartitionBy() {
    // Given:
    when(analysis.getPartitionBy()).thenReturn(Optional.of("Something"));

    // Then:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("Static queries don't support PARTITION BY clauses.");

    // When:
    validator.validate(analysis);
  }

  @Test
  public void shouldThrowOnStaticQueryThatHasHavingClause() {
    // Given:
    when(analysis.getHavingExpression()).thenReturn(Optional.of(AN_EXPRESSION));

    // Then:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("Static queries don't support HAVING clauses.");

    // When:
    validator.validate(analysis);
  }

  @Test
  public void shouldThrowOnStaticQueryThatHasLimitClause() {
    // Given:
    when(analysis.getLimitClause()).thenReturn(OptionalInt.of(1));

    // Then:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("Static queries don't support LIMIT clauses.");

    // When:
    validator.validate(analysis);
  }
}