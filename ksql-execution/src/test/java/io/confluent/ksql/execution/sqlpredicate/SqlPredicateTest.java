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
import io.confluent.ksql.execution.expression.tree.ColumnReferenceExp;
import io.confluent.ksql.execution.expression.tree.ComparisonExpression;
import io.confluent.ksql.execution.expression.tree.ComparisonExpression.Type;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.expression.tree.FunctionCall;
import io.confluent.ksql.execution.expression.tree.IntegerLiteral;
import io.confluent.ksql.execution.expression.tree.LogicalBinaryExpression;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.function.KsqlScalarFunction;
import io.confluent.ksql.function.UdfFactory;
import io.confluent.ksql.function.types.ParamTypes;
import io.confluent.ksql.function.udf.Kudf;
import io.confluent.ksql.logging.processing.ProcessingLogConfig;
import io.confluent.ksql.logging.processing.ProcessingLogMessageSchema;
import io.confluent.ksql.logging.processing.ProcessingLogMessageSchema.MessageType;
import io.confluent.ksql.logging.processing.ProcessingLogger;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.FunctionName;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.schema.ksql.ColumnRef;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.util.KsqlConfig;
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

@SuppressWarnings("unchecked")
public class SqlPredicateTest {

  private static final KsqlConfig KSQL_CONFIG = new KsqlConfig(Collections.emptyMap());

  private static final LogicalSchema SCHEMA = LogicalSchema.builder()
      .valueColumn(ColumnName.of("COL0"), SqlTypes.BIGINT)
      .valueColumn(ColumnName.of("COL1"), SqlTypes.DOUBLE)
      .valueColumn(ColumnName.of("COL2"), SqlTypes.STRING)
      .build()
      .withAlias(SourceName.of("TEST1"))
      .withMetaAndKeyColsInValue();

  private static final SourceName TEST1 = SourceName.of("TEST1");

  private static final ColumnReferenceExp COL0 =
      new ColumnReferenceExp(ColumnRef.of(TEST1, ColumnName.of("COL0")));

  private static final ColumnReferenceExp COL2 =
      new ColumnReferenceExp(ColumnRef.of(TEST1, ColumnName.of("COL2")));

  private static final KsqlScalarFunction LEN_FUNCTION = KsqlScalarFunction.createLegacyBuiltIn(
      SqlTypes.INTEGER,
      ImmutableList.of(ParamTypes.STRING),
      FunctionName.of("LEN"),
      LenDummy.class
  );

  @Mock
  private ProcessingLogger processingLogger;
  @Mock
  private ProcessingLogConfig processingLogConfig;
  @Mock
  private FunctionRegistry functionRegistry;
  @Mock
  private UdfFactory lenFactory;

  @Rule
  public final MockitoRule mockitoRule = MockitoJUnit.rule();

  @Before
  public void init() {
    when(functionRegistry.getUdfFactory("LEN")).thenReturn(lenFactory);
    when(lenFactory.getFunction(any())).thenReturn(LEN_FUNCTION);
  }

  @Test
  public void testFilter() {
    // Given:
    SqlPredicate predicate = givenSqlPredicateFor(
        new ComparisonExpression(Type.GREATER_THAN, COL0, new IntegerLiteral(100)));

    // When/Then:
    assertThat(
        predicate.getFilterExpression().toString().toUpperCase(),
        equalTo("(TEST1.COL0 > 100)")
    );
    assertThat(predicate.getColumnIndexes().length, equalTo(1));

  }

  @Test
  public void testFilterBiggerExpression() {
    // Given:
    SqlPredicate predicate = givenSqlPredicateFor(
        new LogicalBinaryExpression(
            LogicalBinaryExpression.Type.AND,
            new ComparisonExpression(Type.GREATER_THAN, COL0, new IntegerLiteral(100)),
            new ComparisonExpression(
                Type.EQUAL,
                new FunctionCall(FunctionName.of("LEN"), ImmutableList.of(COL2)),
                new IntegerLiteral(5)
            )
        )
    );

    // When/Then:
    assertThat(
        predicate.getFilterExpression().toString().toUpperCase(),
        equalTo("((TEST1.COL0 > 100) AND (LEN(TEST1.COL2) = 5))")
    );
    assertThat(predicate.getColumnIndexes().length, equalTo(3));
  }

  @Test
  public void shouldIgnoreNullRows() {
    // Given:
    SqlPredicate sqlPredicate = givenSqlPredicateFor(
        new ComparisonExpression(Type.GREATER_THAN, COL0, new IntegerLiteral(100)));

    // When/Then:
    assertThat(sqlPredicate.getPredicate().test("key", null), is(false));
  }

  @Test
  public void shouldWriteProcessingLogOnError() {
    // Given:
    SqlPredicate sqlPredicate = givenSqlPredicateFor(
        new ComparisonExpression(Type.GREATER_THAN, COL0, new IntegerLiteral(100)));

    // When:
    sqlPredicate.getPredicate().test(
        "key",
        new GenericRow(0L, "key", Collections.emptyList())
    );

    // Then:
    ArgumentCaptor<Function<ProcessingLogConfig, SchemaAndValue>> captor
        = ArgumentCaptor.forClass(Function.class);
    verify(processingLogger).error(captor.capture());
    SchemaAndValue schemaAndValue = captor.getValue().apply(processingLogConfig);
    assertThat(schemaAndValue.schema(), equalTo(ProcessingLogMessageSchema.PROCESSING_LOG_SCHEMA));
    Struct struct = (Struct) schemaAndValue.value();
    assertThat(
        struct.get(ProcessingLogMessageSchema.TYPE),
        equalTo(MessageType.RECORD_PROCESSING_ERROR.ordinal())
    );
    Struct errorStruct
        = struct.getStruct(ProcessingLogMessageSchema.RECORD_PROCESSING_ERROR);
    assertThat(
        errorStruct.get(ProcessingLogMessageSchema.RECORD_PROCESSING_ERROR_FIELD_MESSAGE),
        equalTo(
            "Error evaluating predicate (TEST1.COL0 > 100): "
                + "argument type mismatch")
    );
  }

  private SqlPredicate givenSqlPredicateFor(Expression sqlPredicate) {
    return new SqlPredicate(
        sqlPredicate,
        SCHEMA,
        KSQL_CONFIG,
        functionRegistry,
        processingLogger
    );
  }

  public static class LenDummy implements Kudf {

    @Override
    public Object evaluate(Object... args) {
      throw new IllegalStateException();
    }
  }
}
