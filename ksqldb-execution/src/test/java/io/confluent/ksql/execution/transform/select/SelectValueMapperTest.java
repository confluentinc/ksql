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

package io.confluent.ksql.execution.transform.select;

import static io.confluent.ksql.GenericRow.genericRow;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.codegen.ExpressionMetadata;
import io.confluent.ksql.execution.expression.tree.FunctionCall;
import io.confluent.ksql.execution.transform.KsqlProcessingContext;
import io.confluent.ksql.execution.transform.KsqlTransformer;
import io.confluent.ksql.logging.processing.ProcessingLogger;
import io.confluent.ksql.logging.processing.RecordProcessingError;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.FunctionName;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class SelectValueMapperTest {

  private static final ColumnName NAME0 = ColumnName.of("apple");
  private static final ColumnName NAME1 = ColumnName.of("cherry");
  private static final ColumnName NAME2 = ColumnName.of("banana");
  private static final Object KEY = null; // Not used yet.
  private static final GenericRow VALUE = genericRow(1234, 0, "hotdog");

  @Mock
  private ExpressionMetadata col0;
  @Mock
  private ExpressionMetadata col1;
  @Mock
  private ExpressionMetadata col2;
  @Mock
  private ProcessingLogger processingLogger;
  @Mock
  private KsqlProcessingContext ctx;

  private KsqlTransformer<Object, GenericRow> transformer;

  @Before
  public void setup() {
    final SelectValueMapper<Object> selectValueMapper = new SelectValueMapper<>(
        ImmutableList.of(
            SelectValueMapper.SelectInfo.of(NAME0, col0),
            SelectValueMapper.SelectInfo.of(NAME1, col1),
            SelectValueMapper.SelectInfo.of(NAME2, col2)
        )
    );

    transformer = selectValueMapper.getTransformer(processingLogger);
  }

  @Test
  public void shouldInvokeEvaluatorsWithCorrectParams() {
    // When:
    transformer.transform(KEY, VALUE, ctx);

    // Then:
    verify(col0).evaluate(VALUE);
    verify(col1).evaluate(VALUE);
    verify(col2).evaluate(VALUE);
  }

  @Test
  public void shouldEvaluateExpressions() {
    // Given:
    givenEvaluations(100, 200, 300);

    // When:
    final GenericRow result = transformer.transform(KEY, VALUE, ctx);

    // Then:
    assertThat(result, equalTo(genericRow(100, 200, 300)));
  }

  @Test
  public void shouldHandleNullRows() {
    // When:
    final GenericRow result = transformer.transform(KEY, null, ctx);

    // Then:
    assertThat(result, is(nullValue()));
  }

  @Test
  public void shouldWriteProcessingLogOnError() {
    // Given:
    when(col0.getExpression()).thenReturn(
        new FunctionCall(FunctionName.of("kumquat"), ImmutableList.of())
    );
    final RuntimeException e = new RuntimeException("oops");
    when(col0.evaluate(any())).thenThrow(e);

    final GenericRow row = genericRow(0L, "key", 2L, "foo", "whatever", null, "boo", "hoo");

    // When:
    transformer.transform(KEY, row, ctx);

    // Then:
    verify(processingLogger).error(RecordProcessingError.recordProcessingError(
        "Error computing expression kumquat() "
            + "for column apple with index 0: oops",
        e,
        row
    ));
  }

  private void givenEvaluations(final Object result0, final Object result1, final Object result2) {
    when(col0.evaluate(any())).thenReturn(result0);
    when(col1.evaluate(any())).thenReturn(result1);
    when(col2.evaluate(any())).thenReturn(result2);
  }
}
