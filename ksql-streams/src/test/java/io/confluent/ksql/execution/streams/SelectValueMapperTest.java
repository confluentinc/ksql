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

package io.confluent.ksql.execution.streams;

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
import io.confluent.ksql.logging.processing.ProcessingLogConfig;
import io.confluent.ksql.logging.processing.ProcessingLogMessageSchema;
import io.confluent.ksql.logging.processing.ProcessingLogMessageSchema.MessageType;
import io.confluent.ksql.logging.processing.ProcessingLogger;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.FunctionName;
import java.util.Collections;
import java.util.function.Function;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

public class SelectValueMapperTest {
  private static final ColumnName NAME0 = ColumnName.of("apple");
  private static final ColumnName NAME1 = ColumnName.of("cherry");
  private static final ColumnName NAME2 = ColumnName.of("banana");
  private static final GenericRow ROW = new GenericRow(ImmutableList.of(1234, 0, "hotdog"));

  @Mock
  private ExpressionMetadata col0;
  @Mock
  private ExpressionMetadata col1;
  @Mock
  private ExpressionMetadata col2;
  @Mock
  private ProcessingLogger processingLogger;

  private SelectValueMapper selectValueMapper;

  @Rule
  public final MockitoRule mockitoRule = MockitoJUnit.rule();

  @Before
  public void setup() {
    selectValueMapper = new SelectValueMapper(
        ImmutableList.of(
            SelectValueMapper.SelectInfo.of(NAME0, col0),
            SelectValueMapper.SelectInfo.of(NAME1, col1),
            SelectValueMapper.SelectInfo.of(NAME2, col2)
        ),
        processingLogger
    );
  }

  private void givenEvaluations(final Object result0, final Object result1, final Object result2) {
    when(col0.evaluate(any())).thenReturn(result0);
    when(col1.evaluate(any())).thenReturn(result1);
    when(col2.evaluate(any())).thenReturn(result2);
  }

  @Test
  public void shouldEvaluateExpressions() {
    // Given:
    givenEvaluations(100, 200, 300);

    // When:
    final GenericRow result = selectValueMapper.apply(ROW);

    // Then:
    assertThat(result, equalTo(new GenericRow(ImmutableList.of(100, 200, 300))));
  }

  @Test
  public void shouldHandleNullRows() {
    // When:
    final GenericRow result = selectValueMapper.apply(null);

    // Then:
    assertThat(result, is(nullValue()));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void shouldWriteProcessingLogOnError() {
    // Given:
    when(col0.getExpression()).thenReturn(
        new FunctionCall(FunctionName.of("kumquat"), ImmutableList.of())
    );
    when(col0.evaluate(any())).thenThrow(new RuntimeException("oops"));

    // When:
    selectValueMapper.apply(
        new GenericRow(0L, "key", 2L, "foo", "whatever", null, "boo", "hoo"));

    // Then:
    final ArgumentCaptor<Function<ProcessingLogConfig, SchemaAndValue>> captor
        = ArgumentCaptor.forClass(Function.class);
    verify(processingLogger).error(captor.capture());
    final SchemaAndValue schemaAndValue = captor.getValue().apply(
        new ProcessingLogConfig(Collections.emptyMap()));
    assertThat(schemaAndValue.schema(), equalTo(ProcessingLogMessageSchema.PROCESSING_LOG_SCHEMA));
    final Struct struct = (Struct) schemaAndValue.value();
    assertThat(
        struct.get(ProcessingLogMessageSchema.TYPE),
        equalTo(MessageType.RECORD_PROCESSING_ERROR.ordinal()));
    final Struct errorStruct
        = struct.getStruct(ProcessingLogMessageSchema.RECORD_PROCESSING_ERROR);
    assertThat(
        errorStruct.get(ProcessingLogMessageSchema.RECORD_PROCESSING_ERROR_FIELD_MESSAGE),
        equalTo(
            "Error computing expression kumquat() "
                + "for column apple with index 0: oops")
    );
  }
}
