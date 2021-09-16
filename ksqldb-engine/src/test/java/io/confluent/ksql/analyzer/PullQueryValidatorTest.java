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
import com.google.common.collect.ImmutableSet;
import io.confluent.ksql.analyzer.Analysis.Into;
import io.confluent.ksql.execution.expression.tree.ColumnReferenceExp;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.expression.tree.QualifiedColumnReferenceExp;
import io.confluent.ksql.execution.expression.tree.UnqualifiedColumnReferenceExp;
import io.confluent.ksql.execution.util.ColumnExtractor;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.parser.OutputRefinement;
import io.confluent.ksql.parser.tree.GroupBy;
import io.confluent.ksql.parser.tree.PartitionBy;
import io.confluent.ksql.parser.tree.SingleColumn;
import io.confluent.ksql.parser.tree.WindowExpression;
import io.confluent.ksql.schema.ksql.SystemColumns;
import io.confluent.ksql.serde.RefinementInfo;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import java.util.HashSet;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import static io.confluent.ksql.schema.ksql.SystemColumns.ROWOFFSET_NAME;
import static io.confluent.ksql.schema.ksql.SystemColumns.ROWPARTITION_NAME;

@RunWith(MockitoJUnitRunner.class)
public class PullQueryValidatorTest {

  private static final Expression AN_EXPRESSION = mock(Expression.class);
  private static final Expression A_BAD_EXPRESSION = mock(Expression.class);
  private static final ColumnName A_COLUMN_NAME = ColumnName.of("asdf");
  private static final ColumnName A_BAD_COLUMN_NAME = ROWPARTITION_NAME;



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
  private SingleColumn singleColumn1;
  @Mock
  private SingleColumn singleColumn2;
  @Mock
  private ColumnReferenceExp columnReferenceExp;
  @Mock
  private UnqualifiedColumnReferenceExp unqualifiedColumnReferenceExp;
  @Mock
  private Expression expression1;
  @Mock
  private Expression expression2;

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
    // Given:
    givenSelectClauseWithDisallowedColumnNames();
    when(ksqlConfig.getBoolean(KsqlConfig.KSQL_ROWPARTITION_ROWOFFSET_ENABLED)).thenReturn(true);

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> validator.validate(analysis)
    );

    // Then:
    assertThat(e.getMessage(), containsString("Pull queries don't support ROWPARTITION or ROWOFFSET in SELECT clauses."));
  }

  @Test
  public void shouldThrowWhenWhereClauseContainsDisallowedColumns() {
    // Given:
    givenColumnExtractorAndFaultyInput();
    when(ksqlConfig.getBoolean(KsqlConfig.KSQL_ROWPARTITION_ROWOFFSET_ENABLED)).thenReturn(true);

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> validator.validate(analysis)
    );

    // Then:
    assertThat(e.getMessage(), containsString("Pull queries don't support ROWPARTITION or ROWOFFSET in WHERE clauses."));
  }

  //todo: have .getSelectItems() return a set of mocked SingleColumns
  //SingleColumn's getExpression() call should return null
  //mock ColumnExtractor.extractColumns(badExpression) to return a set of column names of my choice
  private void givenColumnExtractorAndFaultyInput() {


  }

  private void givenSelectClauseWithDisallowedColumnNames() {
    when(analysis.getSelectItems()).thenReturn(ImmutableList.of(singleColumn1, singleColumn2));
    when(singleColumn1.getExpression()).thenReturn(expression1);
    when(singleColumn2.getExpression()).thenReturn(expression2);

    Set<ColumnReferenceExp> cols = ImmutableSet.of(unqualifiedColumnReferenceExp);
    Mockito.<Set<? extends ColumnReferenceExp>>when(ColumnExtractor.extractColumns(expression1)).thenReturn(ImmutableSet.of(unqualifiedColumnReferenceExp));
    when(columnReferenceExp.getColumnName()).thenReturn(A_BAD_COLUMN_NAME);


  }

  private void givenWhereClauseWithDisallowedColumnNames() {
    when(analysis.getWhereExpression()).thenReturn(Optional.of(A_BAD_EXPRESSION));
  }

}