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

import static io.confluent.ksql.schema.ksql.SystemColumns.ROWPARTITION_NAME;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.confluent.ksql.analyzer.Analysis.Into;
import io.confluent.ksql.execution.expression.tree.ColumnReferenceExp;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.util.ColumnExtractor;
import io.confluent.ksql.parser.tree.GroupBy;
import io.confluent.ksql.parser.tree.PartitionBy;
import io.confluent.ksql.parser.tree.SingleColumn;
import io.confluent.ksql.parser.tree.WindowExpression;
import io.confluent.ksql.serde.RefinementInfo;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import java.util.Optional;
import java.util.OptionalInt;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
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
  @Mock
  private KsqlConfig ksqlConfig;
  @Mock
  private SingleColumn singleColumn;
  @Mock
  private ColumnReferenceExp columnReferenceExp;

  private QueryValidator validator;

  @Before
  public void setUp() {
    validator = new PullQueryValidator();
    when(analysis.getRefinementInfo()).thenReturn(Optional.empty());
    when(analysis.getKsqlConfig()).thenReturn(ksqlConfig);
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


  @Test
  public void shouldThrowWhenSelectClauseContainsDisallowedColumns() {
    try(MockedStatic<ColumnExtractor> columnExtractor = mockStatic(ColumnExtractor.class)) {
      //Given:
      givenSelectClauseWithDisallowedColumnNames(columnExtractor);

      // When:
      final Exception e = assertThrows(
          KsqlException.class,
          () -> validator.validate(analysis)
      );

      // Then:
      assertThat(e.getMessage(), containsString("Pull queries don't support `ROWPARTITION` in SELECT clauses."));
    }
  }

  @Test
  public void shouldThrowWhenWhereClauseContainsDisallowedColumns() {
    try(MockedStatic<ColumnExtractor> columnExtractor = mockStatic(ColumnExtractor.class)) {
      //Given:
      givenWhereClauseWithDisallowedColumnNames(columnExtractor);

      // When:
      final Exception e = assertThrows(
          KsqlException.class,
          () -> validator.validate(analysis)
      );

      // Then:
      assertThat(e.getMessage(), containsString("Pull queries don't support `ROWPARTITION` in WHERE clauses."));
    }
  }

  private void givenSelectClauseWithDisallowedColumnNames(
      MockedStatic<ColumnExtractor> columnExtractor
  ) {
    givenColumnExtractionOfDisallowedColumns(columnExtractor);
    when(analysis.getSelectItems()).thenReturn(ImmutableList.of(singleColumn));
    when(singleColumn.getExpression()).thenReturn(AN_EXPRESSION);
  }

  private void givenWhereClauseWithDisallowedColumnNames(
      MockedStatic<ColumnExtractor> columnExtractor
  ) {
    givenColumnExtractionOfDisallowedColumns(columnExtractor);
    when(analysis.getWhereExpression()).thenReturn(Optional.of(AN_EXPRESSION));
  }

  private void givenColumnExtractionOfDisallowedColumns(
      MockedStatic<ColumnExtractor> columnExtractor) {
    columnExtractor.when(() -> ColumnExtractor.extractColumns(AN_EXPRESSION))
        .thenReturn(ImmutableSet.of(columnReferenceExp));
    when(columnReferenceExp.getColumnName()).thenReturn(ROWPARTITION_NAME);
    when(ksqlConfig.getBoolean(KsqlConfig.KSQL_ROWPARTITION_ROWOFFSET_ENABLED)).thenReturn(true);
  }

}