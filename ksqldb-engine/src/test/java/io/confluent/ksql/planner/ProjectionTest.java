/*
 * Copyright 2021 Confluent Inc.
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

package io.confluent.ksql.planner;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

import com.google.common.collect.ImmutableList;
import com.google.common.testing.EqualsTester;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.expression.tree.QualifiedColumnReferenceExp;
import io.confluent.ksql.execution.expression.tree.UnqualifiedColumnReferenceExp;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.parser.tree.AllColumns;
import io.confluent.ksql.parser.tree.SelectItem;
import io.confluent.ksql.parser.tree.SingleColumn;
import java.util.Optional;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@SuppressWarnings("UnstableApiUsage")
@RunWith(MockitoJUnitRunner.class)
public class ProjectionTest {

  private static final SourceName A = SourceName.of("A");
  private static final SourceName B = SourceName.of("B");

  private static final ColumnName COL0 = ColumnName.of("COL0");
  private static final ColumnName COL1 = ColumnName.of("COL1");

  private static final AllColumns ALL_COLUMNS = new AllColumns(Optional.empty());
  private static final AllColumns ALL_A_COLUMNS = new AllColumns(Optional.of(A));

  @Mock
  private Expression expression;
  @Mock
  private Expression expression2;

  @Test
  public void shouldImplementEqualsAndHashCoe() {
    new EqualsTester()
        .addEqualityGroup(
            Projection.of(ImmutableList.of(ALL_COLUMNS)),
            Projection.of(ImmutableList.of(ALL_COLUMNS))
        )
        .addEqualityGroup(
            Projection.of(ImmutableList.of(ALL_A_COLUMNS))
        )
        .testEquals();
  }

  @Test(expected = UnsupportedOperationException.class)
  public void shouldThrowOnUnsupportedColumnType() {
    Projection.of(ImmutableList.of(mock(SelectItem.class)));
  }

  @Test
  public void shouldMatchEverythingOnUnqualifiedAllColumns() {
    // Given:
    final Projection projection = Projection.of(ImmutableList.of(ALL_COLUMNS));

    // Then:
    assertThat(projection.containsExpression(expression), is(true));
  }

  @Test
  public void shouldMatchQualifiedColumnToMatchingQualifiedAllColumns() {
    // Given:
    final Projection projection = Projection.of(ImmutableList.of(ALL_A_COLUMNS));

    // Then:
    assertThat(projection.containsExpression(new QualifiedColumnReferenceExp(A, COL0)), is(true));
  }

  @Test
  public void shouldNotMatchQualifiedColumnToQualifiedAllColumnsIfDifferentSource() {
    // Given:
    final Projection projection = Projection.of(ImmutableList.of(ALL_A_COLUMNS));

    // Then:
    assertThat(projection.containsExpression(new QualifiedColumnReferenceExp(B, COL0)), is(false));
  }

  @Test
  public void shouldNotMatchUnqualifiedColumnToQualifiedAllColumns() {
    // Given:
    final Projection projection = Projection.of(ImmutableList.of(ALL_A_COLUMNS));

    // Then:
    assertThat(projection.containsExpression(new UnqualifiedColumnReferenceExp(COL0)), is(false));
  }

  @Test
  public void shouldMatchQualifiedColumnToMatchingSingleColumn() {
    // Given:
    final Projection projection = Projection.of(ImmutableList.of(
        new SingleColumn(new QualifiedColumnReferenceExp(A, COL0), Optional.empty())
    ));

    // Then:
    assertThat(projection.containsExpression(new QualifiedColumnReferenceExp(A, COL0)), is(true));
  }

  @Test
  public void shouldNotMatchQualifiedColumnToUnqualifiedSingleColumn() {
    // Given:
    final Projection projection = Projection.of(ImmutableList.of(
        new SingleColumn(new QualifiedColumnReferenceExp(A, COL0), Optional.empty())
    ));

    // Then:
    assertThat(projection.containsExpression(new UnqualifiedColumnReferenceExp(COL0)), is(false));
  }

  @Test
  public void shouldNotMatchQualifiedColumnToDifferentQualifiedSingleColumn() {
    // Given:
    final Projection projection = Projection.of(ImmutableList.of(
        new SingleColumn(new QualifiedColumnReferenceExp(A, COL0), Optional.empty())
    ));

    // Then:
    assertThat(projection.containsExpression(new QualifiedColumnReferenceExp(B, COL0)), is(false));
  }

  @Test
  public void shouldMatchUnqualifiedColumnToMatchingSingleColumn() {
    // Given:
    final Projection projection = Projection.of(ImmutableList.of(
        new SingleColumn(new UnqualifiedColumnReferenceExp(COL0), Optional.empty())
    ));

    // Then:
    assertThat(projection.containsExpression(new UnqualifiedColumnReferenceExp(COL0)), is(true));
  }

  @Test
  public void shouldNotMatchUnqualifiedColumnToQualifiedSingleColumn() {
    // Given:
    final Projection projection = Projection.of(ImmutableList.of(
        new SingleColumn(new UnqualifiedColumnReferenceExp(COL0), Optional.empty())
    ));

    // Then:
    assertThat(projection.containsExpression(new QualifiedColumnReferenceExp(A, COL0)), is(false));
  }

  @Test
  public void shouldNotMatchUnqualifiedColumnToDifferentUnqualifiedSingleColumn() {
    // Given:
    final Projection projection = Projection.of(ImmutableList.of(
        new SingleColumn(new UnqualifiedColumnReferenceExp(COL0), Optional.empty())
    ));

    // Then:
    assertThat(projection.containsExpression(new UnqualifiedColumnReferenceExp(COL1)), is(false));
  }

  @Test
  public void shouldMatchExpressionToMatchingSingleColumn() {
    // Given:
    final Projection projection = Projection.of(ImmutableList.of(
        new SingleColumn(expression, Optional.empty())
    ));

    // Then:
    assertThat(projection.containsExpression(expression), is(true));
  }

  @Test
  public void shouldNotMatchExpressionToQualifiedSingleColumn() {
    // Given:
    final Projection projection = Projection.of(ImmutableList.of(
        new AllColumns(Optional.of(A))
    ));

    // Then:
    assertThat(projection.containsExpression(expression), is(false));
  }

  @Test
  public void shouldNotMatchExpressionNonMatchingExpression() {
    // Given:
    final Projection projection = Projection.of(ImmutableList.of(
        new SingleColumn(expression2, Optional.empty())
    ));

    // Then:
    assertThat(projection.containsExpression(expression), is(false));
  }
}