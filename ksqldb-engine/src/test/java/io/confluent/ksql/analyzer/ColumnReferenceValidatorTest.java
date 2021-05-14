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

package io.confluent.ksql.analyzer;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.expression.tree.QualifiedColumnReferenceExp;
import io.confluent.ksql.execution.expression.tree.UnqualifiedColumnReferenceExp;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.parser.NodeLocation;
import io.confluent.ksql.util.UnknownColumnException;
import java.util.Arrays;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ColumnReferenceValidatorTest {

  private static final String CLAUSE_TYPE = "PARTITION BY";
  private static final Expression POSSIBLE_SYNTHETIC_KEY = new UnqualifiedColumnReferenceExp(
      ColumnName.of("ROWKEY")
  );

  @Mock
  private SourceSchemas sourceSchemas;
  private ColumnReferenceValidator analyzer;

  @Before
  public void setUp() {
    analyzer = new ColumnReferenceValidator(sourceSchemas, true);
  }

  @Test
  public void shouldGetSourceForUnqualifiedColumnRef() {
    // Given:
    final ColumnName column = ColumnName.of("qualified");
    final Expression expression = new QualifiedColumnReferenceExp(
        SourceName.of("fully"),
        column
    );

    when(sourceSchemas.sourcesWithField(any(), any())).thenReturn(sourceNames("something"));

    // When:
    analyzer.analyzeExpression(expression, CLAUSE_TYPE);

    // Then:
    verify(sourceSchemas).sourcesWithField(Optional.of(SourceName.of("fully")), column);
  }

  @Test
  public void shouldGetSourceForQualifiedColumnRef() {
    // Given:
    final QualifiedColumnReferenceExp expression = new QualifiedColumnReferenceExp(
        SourceName.of("something"),
        ColumnName.of("else")
    );

    when(sourceSchemas.sourcesWithField(any(), any()))
        .thenReturn(ImmutableSet.of(SourceName.of("something")));

    // When:
    final Set<SourceName> columnRefs = analyzer.analyzeExpression(expression, CLAUSE_TYPE);

    // Then:
    verify(sourceSchemas).sourcesWithField(
        Optional.of(expression.getQualifier()),
        expression.getColumnName()
    );
    assertThat(
        Iterables.getOnlyElement(columnRefs),
        is(SourceName.of("something")));
  }

  @Test
  public void shouldThrowOnNoSources() {
    // Given:
    final Expression expression = new UnqualifiedColumnReferenceExp(
        ColumnName.of("just-name")
    );

    when(sourceSchemas.sourcesWithField(any(), any()))
        .thenReturn(ImmutableSet.of());

    // When:
    final Exception e = assertThrows(
        UnknownColumnException.class,
        () -> analyzer.analyzeExpression(expression, CLAUSE_TYPE)
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        CLAUSE_TYPE + " column 'just-name' cannot be resolved."));
  }

  @Test
  public void shouldIncludeLocationInErrorIfKnown() {
    // Given:
    final Expression expression = new UnqualifiedColumnReferenceExp(
        Optional.of(new NodeLocation(10, 23)),
        ColumnName.of("just-name")
    );

    when(sourceSchemas.sourcesWithField(any(), any()))
        .thenReturn(ImmutableSet.of());

    // When:
    final Exception e = assertThrows(
        UnknownColumnException.class,
        () -> analyzer.analyzeExpression(expression, CLAUSE_TYPE)
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "Line: 10, Col: 24: " + CLAUSE_TYPE));
  }

  @Test
  public void shouldNotThrowOnPossibleSyntheticKeyColumn() {
    // Given:
    when(sourceSchemas.isJoin()).thenReturn(true);

    // When:
    analyzer.analyzeExpression(POSSIBLE_SYNTHETIC_KEY, "SELECT");

    // Then: did not throw.
  }

  @Test
  public void shouldThrowOnPossibleSyntheticKeyColumnIfNotPossible() {
    // Given:
    analyzer = new ColumnReferenceValidator(sourceSchemas, false);

    // When:
    final Exception e = assertThrows(
        UnknownColumnException.class,
        () -> analyzer.analyzeExpression(POSSIBLE_SYNTHETIC_KEY, "SELECT")
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "SELECT column 'ROWKEY' cannot be resolved."));
  }

  @Test
  public void shouldThrowOnPossibleSyntheticKeyColumnIfNotJoin() {
    // Given:
    when(sourceSchemas.isJoin()).thenReturn(false);

    // When:
    final Exception e = assertThrows(
        UnknownColumnException.class,
        () -> analyzer.analyzeExpression(POSSIBLE_SYNTHETIC_KEY, "SELECT")
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "SELECT column 'ROWKEY' cannot be resolved."));
  }

  @Test
  public void shouldThrowOnPossibleSyntheticKeyColumnIfQualifiedColumnReference() {
    // Given:
    when(sourceSchemas.isJoin()).thenReturn(true);

    final Expression notSyntheticKey = new QualifiedColumnReferenceExp(
        SourceName.of("Bob"), ColumnName.of("ROWKEY")
    );

    // When:
    final Exception e = assertThrows(
        UnknownColumnException.class,
        () -> analyzer.analyzeExpression(notSyntheticKey, "SELECT")
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "SELECT column 'Bob.ROWKEY' cannot be resolved."));
  }

  @Test
  public void shouldThrowOnPossibleSyntheticKeyColumnIfNotSelect() {
    // Given:
    when(sourceSchemas.isJoin()).thenReturn(true);

    // When:
    final Exception e = assertThrows(
        UnknownColumnException.class,
        () -> analyzer.analyzeExpression(POSSIBLE_SYNTHETIC_KEY, "NOT SELECT")
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "NOT SELECT column 'ROWKEY' cannot be resolved."));
  }

  @Test
  public void shouldThrowOnPossibleSyntheticKeyColumnIfNotValidSyntheticKeyColumnName() {
    // Given:
    when(sourceSchemas.isJoin()).thenReturn(true);

    final Expression notSyntheticKey = new UnqualifiedColumnReferenceExp(
        ColumnName.of("NOT_ROWKEY")
    );

    // When:
    final Exception e = assertThrows(
        UnknownColumnException.class,
        () -> analyzer.analyzeExpression(notSyntheticKey, "SELECT")
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "SELECT column 'NOT_ROWKEY' cannot be resolved."));
  }

  private static Set<SourceName> sourceNames(final String... names) {
    return Arrays.stream(names)
        .map(SourceName::of)
        .collect(Collectors.toSet());
  }
}