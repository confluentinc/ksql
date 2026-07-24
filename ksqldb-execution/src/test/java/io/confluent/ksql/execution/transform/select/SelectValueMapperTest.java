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
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.codegen.CompiledExpression;
import io.confluent.ksql.execution.expression.tree.UnqualifiedColumnReferenceExp;
import io.confluent.ksql.execution.transform.KsqlProcessingContext;
import io.confluent.ksql.execution.transform.KsqlTransformer;
import io.confluent.ksql.logging.processing.ProcessingLogger;
import io.confluent.ksql.name.ColumnName;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
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
  private CompiledExpression col0;
  @Mock
  private CompiledExpression col1;
  @Mock
  private CompiledExpression col2;
  @Mock
  private ProcessingLogger processingLogger;
  @Mock
  private KsqlProcessingContext ctx;

  private KsqlTransformer<Object, GenericRow> transformer;

  @Before
  public void setup() {
    when(col0.getExpression()).thenReturn(new UnqualifiedColumnReferenceExp(ColumnName.of("a")));
    when(col1.getExpression()).thenReturn(new UnqualifiedColumnReferenceExp(ColumnName.of("c")));
    when(col2.getExpression()).thenReturn(new UnqualifiedColumnReferenceExp(ColumnName.of("b")));

    final SelectValueMapper<Object> selectValueMapper = new SelectValueMapper<>(
        ImmutableList.of(
            SelectValueMapper.SelectInfo.of(NAME0, col0),
            SelectValueMapper.SelectInfo.of(NAME1, col1),
            SelectValueMapper.SelectInfo.of(NAME2, col2)
        )
    );

    transformer = selectValueMapper.getTransformer(processingLogger);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void shouldInvokeEvaluatorsWithCorrectParams() {
    // When:
    transformer.transform(KEY, VALUE);

    // Then:
    final ArgumentCaptor<Supplier<String>> errorMsgCaptor = ArgumentCaptor.forClass(Supplier.class);
    verify(col0).evaluate(eq(VALUE), isNull(), eq(processingLogger), errorMsgCaptor.capture());
    verify(col1).evaluate(eq(VALUE), isNull(), eq(processingLogger), errorMsgCaptor.capture());
    verify(col2).evaluate(eq(VALUE), isNull(), eq(processingLogger), errorMsgCaptor.capture());

    final List<String> errorMsgs = errorMsgCaptor.getAllValues().stream()
        .map(Supplier::get)
        .collect(Collectors.toList());

    assertThat(errorMsgs, contains(
        "Error computing expression a for column apple with index 0",
        "Error computing expression c for column cherry with index 1",
        "Error computing expression b for column banana with index 2"
    ));
  }

  @Test
  public void shouldEvaluateExpressions() {
    // Given:
    when(col0.evaluate(any(), any(), any(), any())).thenReturn(100);
    when(col1.evaluate(any(), any(), any(), any())).thenReturn(200);
    when(col2.evaluate(any(), any(), any(), any())).thenReturn(300);

    // When:
    final GenericRow result = transformer.transform(KEY, VALUE);

    // Then:
    assertThat(result, equalTo(genericRow(100, 200, 300)));
  }

  @Test
  public void shouldHandleNullRows() {
    // When:
    final GenericRow result = transformer.transform(KEY, null);

    // Then:
    assertThat(result, is(nullValue()));
  }
}
