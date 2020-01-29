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

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.expression.tree.QualifiedColumnReferenceExp;
import io.confluent.ksql.execution.expression.tree.StringLiteral;
import io.confluent.ksql.execution.expression.tree.UnqualifiedColumnReferenceExp;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.schema.ksql.ColumnRef;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.SchemaUtil;
import java.util.Arrays;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ExpressionAnalyzerTest {

  private static final Expression WINDOW_START_EXP = new UnqualifiedColumnReferenceExp(
      ColumnRef.of(SchemaUtil.WINDOWSTART_NAME)
  );

  private static final Expression OTHER_EXP = new StringLiteral("foo");

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Mock
  private SourceSchemas sourceSchemas;
  private ExpressionAnalyzer analyzer;

  @Before
  public void setUp() {
    analyzer = new ExpressionAnalyzer(sourceSchemas);
  }

  @Test
  public void shouldGetSourceForUnqualifiedColumnRef() {
    // Given:
    final ColumnRef column = ColumnRef.of(ColumnName.of("qualified"));
    final Expression expression = new QualifiedColumnReferenceExp(
        SourceName.of("fully"),
        column
    );

    when(sourceSchemas.sourcesWithField(any(), any())).thenReturn(sourceNames("something"));

    // When:
    analyzer.analyzeExpression(expression);

    // Then:
    verify(sourceSchemas).sourcesWithField(Optional.of(SourceName.of("fully")), column);
  }

  @Test
  public void shouldThrowOnMultipleSources() {
    // Given:
    final Expression expression = new UnqualifiedColumnReferenceExp(
        ColumnRef.of(ColumnName.of("just-name"))
    );

    when(sourceSchemas.sourcesWithField(any(), any()))
        .thenReturn(sourceNames("multiple", "sources"));

    // Then:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage(
        "Column 'just-name' is ambiguous. Could be any of: multiple.just-name, sources.just-name");

    // When:
    analyzer.analyzeExpression(expression);
  }

  @Test
  public void shouldGetSourceForQualifiedColumnRef() {
    // Given:
    final QualifiedColumnReferenceExp expression = new QualifiedColumnReferenceExp(
        SourceName.of("something"),
        ColumnRef.of(ColumnName.of("else"))
    );

    when(sourceSchemas.sourcesWithField(any(), any()))
        .thenReturn(ImmutableSet.of(SourceName.of("something")));

    // When:
    final Set<SourceName> columnRefs = analyzer.analyzeExpression(expression);

    // Then:
    verify(sourceSchemas).sourcesWithField(
        Optional.of(expression.getQualifier()),
        expression.getReference()
    );
    assertThat(
        Iterables.getOnlyElement(columnRefs),
        is(SourceName.of("something")));
  }

  @Test
  public void shouldThrowOnNoSources() {
    // Given:
    final Expression expression = new UnqualifiedColumnReferenceExp(
        ColumnRef.of(ColumnName.of("just-name"))
    );

    when(sourceSchemas.sourcesWithField(any(), any()))
        .thenReturn(ImmutableSet.of());

    // Then:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage(
        "Column 'just-name' cannot be resolved.");

    // When:
    analyzer.analyzeExpression(expression);
  }

  private static Set<SourceName> sourceNames(final String... names) {
    return Arrays.stream(names)
        .map(SourceName::of)
        .collect(Collectors.toSet());
  }
}