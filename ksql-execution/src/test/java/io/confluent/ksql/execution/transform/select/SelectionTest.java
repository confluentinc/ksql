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

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.execution.expression.tree.ArithmeticBinaryExpression;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.expression.tree.UnqualifiedColumnReferenceExp;
import io.confluent.ksql.execution.plan.SelectExpression;
import io.confluent.ksql.execution.transform.select.SelectValueMapper.SelectInfo;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.logging.processing.ProcessingLogContext;
import io.confluent.ksql.logging.processing.ProcessingLogger;
import io.confluent.ksql.logging.processing.ProcessingLoggerFactory;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.schema.Operator;
import io.confluent.ksql.schema.ksql.ColumnRef;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.util.KsqlConfig;
import java.util.List;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

public class SelectionTest {
  private static final LogicalSchema SCHEMA = LogicalSchema.builder()
      .valueColumn(ColumnName.of("GIRAFFE"), SqlTypes.STRING)
      .valueColumn(ColumnName.of("MANATEE"), SqlTypes.INTEGER)
      .valueColumn(ColumnName.of("RACCOON"), SqlTypes.BIGINT)
      .build().withMetaAndKeyColsInValue(false);

  private static final Expression EXPRESSION1 =
      new UnqualifiedColumnReferenceExp(ColumnRef.of(ColumnName.of("GIRAFFE")));

  private static final Expression EXPRESSION2 = new ArithmeticBinaryExpression(
      Operator.ADD,
      new UnqualifiedColumnReferenceExp(ColumnRef.of(ColumnName.of("MANATEE"))),
      new UnqualifiedColumnReferenceExp(ColumnRef.of(ColumnName.of("RACCOON")))
  );
  private static final List<SelectExpression> SELECT_EXPRESSIONS = ImmutableList.of(
      SelectExpression.of(ColumnName.of("FOO"), EXPRESSION1),
      SelectExpression.of(ColumnName.of("BAR"), EXPRESSION2)
  );

  @Mock
  private KsqlConfig ksqlConfig;
  @Mock
  private FunctionRegistry functionRegistry;
  @Mock
  private ProcessingLogContext processingLogContext;
  @Mock
  private ProcessingLoggerFactory processingLoggerFactory;
  @Mock
  private ProcessingLogger processingLogger;

  private Selection<String> selection;

  @Rule
  public final MockitoRule mockitoRule = MockitoJUnit.rule();

  @Before
  public void setup() {
    when(processingLogContext.getLoggerFactory()).thenReturn(processingLoggerFactory);
    when(processingLoggerFactory.getLogger(anyString())).thenReturn(processingLogger);
    selection = Selection.of(
        SCHEMA,
        SELECT_EXPRESSIONS,
        ksqlConfig,
        functionRegistry
    );
  }

  @Test
  public void shouldBuildMapperWithCorrectExpressions() {
    // When:
    final SelectValueMapper<String> mapper = selection.getMapper();

    // Then:
    final List<SelectInfo> selectInfos = mapper.getSelects();
    assertThat(
        selectInfos.get(0).getEvaluator().getExpression(),
        equalTo(EXPRESSION1));
    assertThat(
        selectInfos.get(1).getEvaluator().getExpression(),
        equalTo(EXPRESSION2));
  }

  @Test
  public void shouldBuildCorrectResultSchema() {
    // When:
    final LogicalSchema resultSchema = selection.getSchema();

    // Then:
    final LogicalSchema expected = new LogicalSchema.Builder()
        .keyColumn(ColumnName.of("ROWKEY"), SqlTypes.STRING)
        .valueColumn(ColumnName.of("FOO"), SqlTypes.STRING)
        .valueColumn(ColumnName.of("BAR"), SqlTypes.BIGINT)
        .build();
    assertThat(resultSchema, equalTo(expected));
  }
}