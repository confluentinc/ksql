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

import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableSet;
import io.confluent.ksql.execution.expression.tree.ComparisonExpression;
import io.confluent.ksql.execution.expression.tree.ComparisonExpression.Type;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.schema.ksql.ColumnRef;
import io.confluent.ksql.execution.expression.tree.ColumnReferenceExp;
import io.confluent.ksql.execution.expression.tree.StringLiteral;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.SchemaUtil;
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

  private static final Expression WINDOW_START_EXP = new ColumnReferenceExp(
      ColumnRef.of(SourceName.of("something"), SchemaUtil.WINDOWSTART_NAME)
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
  public void shouldNotThrowOnWindowStartIfAllowed() {
    // Given:
    final Expression expression = new ComparisonExpression(
        Type.EQUAL,
        WINDOW_START_EXP,
        OTHER_EXP
    );

    // When:
    analyzer.analyzeExpression(expression, true);

    // Then: did not throw
  }

  @Test
  public void shouldThrowOnWindowStartIfNotAllowed() {
    // Given:
    final Expression expression = new ComparisonExpression(
        Type.EQUAL,
        WINDOW_START_EXP,
        OTHER_EXP
    );

    // Then:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage(
        "Field 'something.WINDOWSTART' cannot be resolved.");

    // When:
    analyzer.analyzeExpression(expression, false);
  }

  @Test
  public void shouldNotThrowOnMultipleSourcesIfFullyQualified() {
    // Given:
    final Expression expression = new ColumnReferenceExp(
        ColumnRef.of(SourceName.of("fully"), ColumnName.of("qualified"))
    );

    when(sourceSchemas.sourcesWithField(ColumnName.of("qualified")))
        .thenReturn(ImmutableSet.of("multiple", "sources", "fully").stream().map(SourceName::of).collect(Collectors.toSet()));

    // When:
    analyzer.analyzeExpression(expression, true);

    // Then: did not throw
  }

  @Test
  public void shouldThrowOnMultipleSourcesIfFullyQualifiedButNoMatch() {
    // Given:
    final Expression expression = new ColumnReferenceExp(
        ColumnRef.of(SourceName.of("fully"), ColumnName.of("qualified"))
    );

    when(sourceSchemas.sourcesWithField(ColumnName.of("qualified")))
        .thenReturn(ImmutableSet.of("not-fully", "also-not-fully").stream().map(SourceName::of).collect(Collectors.toSet()));

    // Then:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage(
        "Source 'fully', used in 'fully.qualified' cannot be resolved.");

    // When:
    analyzer.analyzeExpression(expression, true);
  }

  @Test
  public void shouldThrowOnMultipleSourcesIfNotFullyQualified() {
    // Given:
    final Expression expression = new ColumnReferenceExp(
        ColumnRef.of("just-name")
    );

    when(sourceSchemas.sourcesWithField(ColumnName.of("just-name")))
        .thenReturn(ImmutableSet.of("multiple", "sources").stream().map(SourceName::of).collect(Collectors.toSet()));

    // Then:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage(
        "Field 'just-name' is ambiguous. Could be any of: multiple.just-name, sources.just-name");

    // When:
    analyzer.analyzeExpression(expression, true);
  }

  @Test
  public void shouldThrowOnNoSources() {
    // Given:
    final Expression expression = new ColumnReferenceExp(
        ColumnRef.of("just-name")
    );

    when(sourceSchemas.sourcesWithField(ColumnName.of("just-name")))
        .thenReturn(ImmutableSet.of());

    // Then:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage(
        "Field 'just-name' cannot be resolved.");

    // When:
    analyzer.analyzeExpression(expression, true);
  }
}