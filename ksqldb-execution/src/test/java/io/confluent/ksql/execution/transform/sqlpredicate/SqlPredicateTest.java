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

package io.confluent.ksql.execution.transform.sqlpredicate;

import static io.confluent.ksql.GenericRow.genericRow;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.codegen.CompiledExpression;
import io.confluent.ksql.execution.expression.tree.ComparisonExpression;
import io.confluent.ksql.execution.expression.tree.ComparisonExpression.Type;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.expression.tree.LongLiteral;
import io.confluent.ksql.execution.expression.tree.UnqualifiedColumnReferenceExp;
import io.confluent.ksql.execution.transform.KsqlProcessingContext;
import io.confluent.ksql.execution.transform.KsqlTransformer;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.logging.processing.ProcessingLogger;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.util.KsqlConfig;
import java.util.Collections;
import java.util.Optional;
import java.util.function.Supplier;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

public class SqlPredicateTest {

  private static final KsqlConfig KSQL_CONFIG = new KsqlConfig(Collections.emptyMap());

  private static final LogicalSchema SCHEMA = LogicalSchema.builder()
      .keyColumn(ColumnName.of("ID"), SqlTypes.INTEGER)
      .valueColumn(ColumnName.of("COL0"), SqlTypes.BIGINT)
      .valueColumn(ColumnName.of("COL1"), SqlTypes.DOUBLE)
      .valueColumn(ColumnName.of("COL2"), SqlTypes.STRING)
      .build();

  private static final UnqualifiedColumnReferenceExp COL0 =
      new UnqualifiedColumnReferenceExp(ColumnName.of("COL0"));

  private static final GenericRow VALUE = genericRow(22L, 33.3, "a string");

  @Mock
  private ProcessingLogger processingLogger;
  @Mock
  private FunctionRegistry functionRegistry;
  @Mock
  private KsqlProcessingContext ctx;
  @Mock
  private Expression filterExpression;
  @Mock
  private CompiledExpression evaluator;

  private SqlPredicate predicate;

  @Rule
  public final MockitoRule mockitoRule = MockitoJUnit.rule();
  private KsqlTransformer<Object, Optional<GenericRow>> transformer;

  @Before
  public void init() {
    when(evaluator.getExpressionType()).thenReturn(SqlTypes.BOOLEAN);

    predicate = new SqlPredicate(filterExpression, evaluator);
    transformer = predicate.getTransformer(processingLogger);
  }

  @Test
  public void shouldCompileEvaluator() {
    // Given:
    predicate = new SqlPredicate(
        new ComparisonExpression(Type.LESS_THAN, COL0, new LongLiteral(100)),
        SCHEMA,
        KSQL_CONFIG,
        functionRegistry
    );

    transformer = predicate.getTransformer(processingLogger);

    // When:
    final Optional<GenericRow> result1 = transformer.transform("key", genericRow(99L));
    final Optional<GenericRow> result2 = transformer.transform("key", genericRow(100L));

    // Then:
    assertThat(result1, is(not(Optional.empty())));
    assertThat(result2, is(Optional.empty()));
  }

  @Test
  public void shouldPassFilter() {
    // Given:
    when(evaluator.evaluate(any(), any(), any(), any())).thenReturn(true);

    // When:
    final Optional<GenericRow> result = transformer.transform("key", VALUE);

    // Then:
    assertThat(result, is(Optional.of(VALUE)));
  }

  @Test
  public void shouldNotPassFilter() {
    // Given:
    when(evaluator.evaluate(any(), any(), any(), any())).thenReturn(false);

    // When:
    final Optional<GenericRow> result = transformer.transform("key", VALUE);

    // Then:
    assertThat(result, is(Optional.empty()));
  }

  @Test
  public void shouldIgnoreNullRows() {
    // When:
    final Optional<GenericRow> result = transformer.transform("key", null);

    // Then:
    assertThat(result, is(Optional.empty()));

    verify(evaluator, never()).evaluate(any(), any(), any(), any());
  }

  @SuppressWarnings("unchecked")
  @Test
  public void shouldInvokeEvaluatorWithCorrectParams() {
    // Given:
    when(evaluator.evaluate(any(), any(), any(), any())).thenReturn(true);

    // When:
    transformer.transform("key", VALUE);

    // Then:
    final ArgumentCaptor<Supplier<String>> errorMsgCaptor = ArgumentCaptor.forClass(Supplier.class);
    verify(evaluator)
        .evaluate(eq(VALUE), eq(false), eq(processingLogger), errorMsgCaptor.capture());

    assertThat(errorMsgCaptor.getValue().get(), is("Error evaluating predicate filterExpression"));
  }
}
