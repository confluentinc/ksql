/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.ksql.execution.sqlpredicate;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.expression.tree.ComparisonExpression;
import io.confluent.ksql.execution.expression.tree.ComparisonExpression.Type;
import io.confluent.ksql.execution.expression.tree.DereferenceExpression;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.expression.tree.FunctionCall;
import io.confluent.ksql.execution.expression.tree.IntegerLiteral;
import io.confluent.ksql.execution.expression.tree.LogicalBinaryExpression;
import io.confluent.ksql.execution.expression.tree.QualifiedName;
import io.confluent.ksql.execution.expression.tree.QualifiedNameReference;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.function.KsqlFunction;
import io.confluent.ksql.function.UdfFactory;
import io.confluent.ksql.function.udf.Kudf;
import io.confluent.ksql.logging.processing.ProcessingLogConfig;
import io.confluent.ksql.logging.processing.ProcessingLogMessageSchema;
import io.confluent.ksql.logging.processing.ProcessingLogMessageSchema.MessageType;
import io.confluent.ksql.logging.processing.ProcessingLogger;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.util.KsqlConfig;
import java.util.Collections;
import java.util.function.Function;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

@SuppressWarnings("unchecked")
public class SqlPredicateTest {
  private final KsqlConfig ksqlConfig = new KsqlConfig(Collections.emptyMap());
  private final ProcessingLogConfig processingLogConfig
      = new ProcessingLogConfig(Collections.emptyMap());
  private final LogicalSchema schema = LogicalSchema.builder()
      .valueColumn("COL0", SqlTypes.BIGINT)
      .valueColumn("COL1", SqlTypes.DOUBLE)
      .valueColumn("COL2", SqlTypes.STRING)
      .build()
      .withAlias("TEST1")
      .withMetaAndKeyColsInValue();
  private final QualifiedNameReference test1 = new QualifiedNameReference(
      QualifiedName.of("TEST1")
  );
  private final DereferenceExpression col0 = new DereferenceExpression(test1, "COL0");
  private final DereferenceExpression col2 = new DereferenceExpression(test1, "COL2");

  @Mock
  private ProcessingLogger processingLogger;
  @Mock
  private FunctionRegistry functionRegistry;
  @Mock
  private UdfFactory lenFactory;

  private KsqlFunction lenFunction = KsqlFunction.createLegacyBuiltIn(
      Schema.OPTIONAL_INT32_SCHEMA,
      ImmutableList.of(Schema.OPTIONAL_STRING_SCHEMA),
      "LEN",
      LenDummy.class
  );

  private static class LenDummy implements Kudf {
    @Override
    public Object evaluate(Object... args) {
      throw new IllegalStateException();
    }
  }

  @Rule
  public final MockitoRule mockitoRule = MockitoJUnit.rule();

  @Before
  public void init() {
    when(functionRegistry.getUdfFactory("LEN")).thenReturn(lenFactory);
    when(lenFactory.getFunction(any())).thenReturn(lenFunction);
  }

  @Test
  public void testFilter() {
    // Given:
    final SqlPredicate predicate = givenSqlPredicateFor(
        new ComparisonExpression(Type.GREATER_THAN, col0, new IntegerLiteral(100)));

    // When/Then:
    assertThat(
        predicate.getFilterExpression().toString().toUpperCase(),
        equalTo("(TEST1.COL0 > 100)"));
    assertThat(predicate.getColumnIndexes().length, equalTo(1));

  }

  @Test
  public void testFilterBiggerExpression() {
    // Given:
    final SqlPredicate predicate = givenSqlPredicateFor(
        new LogicalBinaryExpression(
            LogicalBinaryExpression.Type.AND,
            new ComparisonExpression(Type.GREATER_THAN, col0, new IntegerLiteral(100)),
            new ComparisonExpression(
                Type.EQUAL,
                new FunctionCall(QualifiedName.of("LEN"), ImmutableList.of(col2)),
                new IntegerLiteral(5)
            )
        )
    );

    // When/Then:
    assertThat(
        predicate.getFilterExpression().toString().toUpperCase(),
        equalTo("((TEST1.COL0 > 100) AND (LEN(TEST1.COL2) = 5))"));
    assertThat(predicate.getColumnIndexes().length, equalTo(3));
  }

  @Test
  public void shouldIgnoreNullRows() {
    // Given:
    final SqlPredicate sqlPredicate = givenSqlPredicateFor(
        new ComparisonExpression(Type.GREATER_THAN, col0, new IntegerLiteral(100)));

    // When/Then:
    assertThat(sqlPredicate.getPredicate().test("key", null), is(false));
  }

  @Test
  public void shouldWriteProcessingLogOnError() {
    // Given:
    final SqlPredicate sqlPredicate = givenSqlPredicateFor(
        new ComparisonExpression(Type.GREATER_THAN, col0, new IntegerLiteral(100)));

    // When:
    sqlPredicate.getPredicate().test(
        "key",
        new GenericRow(0L, "key", Collections.emptyList()));

    // Then:
    final ArgumentCaptor<Function<ProcessingLogConfig, SchemaAndValue>> captor
        = ArgumentCaptor.forClass(Function.class);
    verify(processingLogger).error(captor.capture());
    final SchemaAndValue schemaAndValue = captor.getValue().apply(processingLogConfig);
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
            "Error evaluating predicate (TEST1.COL0 > 100): "
                + "Invalid field type. Value must be Long.")
    );
  }

  private SqlPredicate givenSqlPredicateFor(final Expression sqlPredicate) {
    return new SqlPredicate(
        sqlPredicate,
        schema,
        ksqlConfig,
        functionRegistry,
        processingLogger
    );
  }
}
