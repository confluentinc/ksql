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
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.expression.tree.IntegerLiteral;
import io.confluent.ksql.execution.expression.tree.LongLiteral;
import io.confluent.ksql.execution.expression.tree.UnqualifiedColumnReferenceExp;
import io.confluent.ksql.execution.transform.KsqlProcessingContext;
import io.confluent.ksql.execution.transform.KsqlTransformer;
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
import io.confluent.ksql.schema.ksql.ColumnRef;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.util.KsqlConfig;
import java.util.Collections;
import java.util.Optional;
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
      .noImplicitColumns()
      .keyColumn(ColumnName.of("ID"), SqlTypes.INTEGER)
      .valueColumn(ColumnName.of("COL0"), SqlTypes.BIGINT)
      .valueColumn(ColumnName.of("COL1"), SqlTypes.DOUBLE)
      .valueColumn(ColumnName.of("COL2"), SqlTypes.STRING)
      .build();

  private static final UnqualifiedColumnReferenceExp COL0 =
      new UnqualifiedColumnReferenceExp(ColumnRef.of(ColumnName.of("COL0")));

  private static final UnqualifiedColumnReferenceExp COL2 =
      new UnqualifiedColumnReferenceExp(ColumnRef.of(ColumnName.of("COL2")));

  private static final KsqlScalarFunction LEN_FUNCTION = KsqlScalarFunction.createLegacyBuiltIn(
      SqlTypes.INTEGER,
      ImmutableList.of(ParamTypes.STRING),
      FunctionName.of("LEN"),
      LenDummy.class
  );

  private static final GenericRow VALUE = new GenericRow(22L, 33.3, "a string");

  @Mock
  private ProcessingLogger processingLogger;
  @Mock
  private ProcessingLogConfig processingLogConfig;
  @Mock
  private FunctionRegistry functionRegistry;
  @Mock
  private UdfFactory lenFactory;
  @Mock
  private KsqlProcessingContext ctx;

  @Rule
  public final MockitoRule mockitoRule = MockitoJUnit.rule();

  @Before
  public void init() {
    when(functionRegistry.getUdfFactory(FunctionName.of("LEN"))).thenReturn(lenFactory);
    when(lenFactory.getFunction(any())).thenReturn(LEN_FUNCTION);
  }

  @Test
  public void shouldPassFilter() {
    // Given:
    final KsqlTransformer<Object, Optional<GenericRow>> predicate = givenSqlPredicateFor(
        new ComparisonExpression(Type.LESS_THAN, COL0, new LongLiteral(100)));

    // When/Then:
    assertThat(predicate.transform("key", VALUE, ctx), is(Optional.of(VALUE)));
  }

  @Test
  public void shouldNotPassFilter() {
    // Given:
    final KsqlTransformer<Object, Optional<GenericRow>> predicate = givenSqlPredicateFor(
        new ComparisonExpression(Type.GREATER_THAN, COL0, new LongLiteral(100)));

    // When/Then:
    assertThat(predicate.transform("key", VALUE, ctx), is(Optional.empty()));
  }

  @Test
  public void shouldIgnoreNullRows() {
    // Given:
    final KsqlTransformer<Object, Optional<GenericRow>> predicate = givenSqlPredicateFor(
        new ComparisonExpression(Type.GREATER_THAN, COL0, new IntegerLiteral(100)));

    // When/Then:
    assertThat(predicate.transform("key", null, ctx), is(Optional.empty()));
  }

  @Test
  public void shouldWriteProcessingLogOnError() {
    // Given:
    final KsqlTransformer<Object, Optional<GenericRow>> predicate = givenSqlPredicateFor(
        new ComparisonExpression(Type.GREATER_THAN, COL0, new IntegerLiteral(100)));

    // When:
    predicate.transform(
        "key",
        new GenericRow("wrong", "types", "in", "here", "to", "force", "error"),
        ctx
    );

    // Then:
    final ArgumentCaptor<Function<ProcessingLogConfig, SchemaAndValue>> captor
        = ArgumentCaptor.forClass(Function.class);
    verify(processingLogger).error(captor.capture());
    final SchemaAndValue schemaAndValue = captor.getValue().apply(processingLogConfig);
    assertThat(schemaAndValue.schema(), equalTo(ProcessingLogMessageSchema.PROCESSING_LOG_SCHEMA));
    final Struct struct = (Struct) schemaAndValue.value();
    assertThat(
        struct.get(ProcessingLogMessageSchema.TYPE),
        equalTo(MessageType.RECORD_PROCESSING_ERROR.ordinal())
    );
    final Struct errorStruct
        = struct.getStruct(ProcessingLogMessageSchema.RECORD_PROCESSING_ERROR);
    assertThat(
        errorStruct.get(ProcessingLogMessageSchema.RECORD_PROCESSING_ERROR_FIELD_MESSAGE),
        equalTo(
            "Error evaluating predicate (COL0 > 100): "
                + "argument type mismatch")
    );
  }

  private KsqlTransformer<Object, Optional<GenericRow>> givenSqlPredicateFor(
      final Expression filterExpression
  ) {
    return new SqlPredicate(
        filterExpression,
        SCHEMA,
        KSQL_CONFIG,
        functionRegistry
    ).getTransformer(processingLogger);
  }

  public static class LenDummy implements Kudf {

    @Override
    public Object evaluate(final Object... args) {
      throw new IllegalStateException();
    }
  }
}
