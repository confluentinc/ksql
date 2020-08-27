/*
 * Copyright 2020 Confluent Inc.
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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.expression.tree.ArithmeticUnaryExpression;
import io.confluent.ksql.execution.expression.tree.ArithmeticUnaryExpression.Sign;
import io.confluent.ksql.execution.expression.tree.DereferenceExpression;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.expression.tree.FunctionCall;
import io.confluent.ksql.execution.expression.tree.NullLiteral;
import io.confluent.ksql.execution.expression.tree.UnqualifiedColumnReferenceExp;
import io.confluent.ksql.execution.util.StructKeyUtil;
import io.confluent.ksql.execution.util.StructKeyUtil.KeyBuilder;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.function.KsqlScalarFunction;
import io.confluent.ksql.function.UdfFactory;
import io.confluent.ksql.function.types.ParamTypes;
import io.confluent.ksql.function.udf.Kudf;
import io.confluent.ksql.logging.processing.ProcessingLogger;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.FunctionName;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.SystemColumns;
import io.confluent.ksql.schema.ksql.types.SqlStruct;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.util.KsqlConfig;
import java.util.Optional;
import java.util.function.BiFunction;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.streams.KeyValue;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class PartitionByParamsFactoryTest {

  private static final KsqlConfig KSQL_CONFIG = new KsqlConfig(ImmutableMap.of());

  private static final ColumnName COL0 = ColumnName.of("COL0");
  private static final ColumnName COL1 = ColumnName.of("COL1");
  private static final ColumnName COL2 = ColumnName.of("KSQL_COL_3");
  private static final ColumnName COL3 = ColumnName.of("COL3");

  private static final SqlStruct COL3_TYPE = SqlTypes.struct()
      .field("someField", SqlTypes.BIGINT)
      .build();

  private static final LogicalSchema SCHEMA = LogicalSchema.builder()
      .keyColumn(COL0, SqlTypes.STRING)
      .valueColumn(COL1, SqlTypes.INTEGER)
      .valueColumn(COL2, SqlTypes.INTEGER)
      .valueColumn(COL3, COL3_TYPE)
      .valueColumn(SystemColumns.ROWTIME_NAME, SqlTypes.BIGINT)
      .valueColumn(COL0, SqlTypes.STRING)
      .build();

  private static final FunctionName FAILING_UDF_NAME = FunctionName.of("I_THROW");
  private static final KsqlScalarFunction FAILING_UDF_FUNC = KsqlScalarFunction.createLegacyBuiltIn(
      SqlTypes.INTEGER,
      ImmutableList.of(ParamTypes.STRING),
      FAILING_UDF_NAME,
      FailingUdf.class
  );

  private static final FunctionName CONSTANT_UDF_NAME = FunctionName.of("I_RETURN_42");
  private static final KsqlScalarFunction CONSTANT_UDF_FUNC = KsqlScalarFunction
      .createLegacyBuiltIn(
          SqlTypes.BIGINT,
          ImmutableList.of(ParamTypes.INTEGER),
          CONSTANT_UDF_NAME,
          ConstantUdf.class
      );

  private static final FunctionCall FAILING_UDF =
      new FunctionCall(FAILING_UDF_NAME, ImmutableList.of());

  private static final String OLD_KEY = "oldKey";
  private static final int COL1_VALUE = 123;

  @Mock
  private ProcessingLogger logger;
  @Mock
  private FunctionRegistry functionRegistry;
  @Mock
  private UdfFactory failingUdfFactory;
  @Mock
  private UdfFactory constantUdfFactory;

  private final Struct key = new Struct(SCHEMA.keyConnectSchema());
  private final GenericRow value = new GenericRow();

  @Before
  public void setUp() {
    when(functionRegistry.getUdfFactory(FAILING_UDF_NAME)).thenReturn(failingUdfFactory);
    when(failingUdfFactory.getFunction(any())).thenReturn(FAILING_UDF_FUNC);

    when(functionRegistry.getUdfFactory(CONSTANT_UDF_NAME)).thenReturn(constantUdfFactory);
    when(constantUdfFactory.getFunction(any())).thenReturn(CONSTANT_UDF_FUNC);

    key.put(COL0.text(), OLD_KEY);
    value
        .append(COL1_VALUE) // COL1
        .append(10L)        // COL2
        .append(1000L)      // Copy of ROWTIME in value
        .append(OLD_KEY);   // Copy of key in value
  }

  @Test
  public void shouldBuildResultSchemaWhenPartitioningByColumnRef() {
    // Given:
    final Expression partitionBy = new UnqualifiedColumnReferenceExp(COL1);

    // When:
    final LogicalSchema resultSchema = PartitionByParamsFactory.buildSchema(
        SCHEMA,
        partitionBy,
        functionRegistry
    );

    // Then:
    assertThat(resultSchema, is(LogicalSchema.builder()
        .keyColumn(COL1, SqlTypes.INTEGER)
        .valueColumn(COL1, SqlTypes.INTEGER)
        .valueColumn(COL2, SqlTypes.INTEGER)
        .valueColumn(COL3, COL3_TYPE)
        .valueColumn(SystemColumns.ROWTIME_NAME, SqlTypes.BIGINT)
        .valueColumn(COL0, SqlTypes.STRING)
        .build()));
  }

  @Test
  public void shouldBuildResultSchemaWhenPartitioningByStructField() {
    // Given:
    final Expression partitionBy = new DereferenceExpression(
        Optional.empty(),
        new UnqualifiedColumnReferenceExp(COL3),
        "someField"
    );

    // When:
    final LogicalSchema resultSchema = PartitionByParamsFactory.buildSchema(
        SCHEMA,
        partitionBy,
        functionRegistry
    );

    // Then:
    assertThat(resultSchema, is(LogicalSchema.builder()
        .keyColumn(ColumnName.of("someField"), SqlTypes.BIGINT)
        .valueColumn(COL1, SqlTypes.INTEGER)
        .valueColumn(COL2, SqlTypes.INTEGER)
        .valueColumn(COL3, COL3_TYPE)
        .valueColumn(SystemColumns.ROWTIME_NAME, SqlTypes.BIGINT)
        .valueColumn(COL0, SqlTypes.STRING)
        .valueColumn(ColumnName.of("someField"), SqlTypes.BIGINT)
        .build()));
  }

  @Test
  public void shouldBuildResultSchemaWhenPartitioningByOtherExpressionType() {
    // Given:
    final Expression partitionBy = new ArithmeticUnaryExpression(
        Optional.empty(),
        Sign.MINUS,
        new UnqualifiedColumnReferenceExp(COL1)
    );

    // When:
    final LogicalSchema resultSchema = PartitionByParamsFactory.buildSchema(
        SCHEMA,
        partitionBy,
        functionRegistry
    );

    // Then:
    assertThat(resultSchema, is(LogicalSchema.builder()
        .keyColumn(ColumnName.of("KSQL_COL_0"), SqlTypes.INTEGER)
        .valueColumn(COL1, SqlTypes.INTEGER)
        .valueColumn(COL2, SqlTypes.INTEGER)
        .valueColumn(COL3, COL3_TYPE)
        .valueColumn(SystemColumns.ROWTIME_NAME, SqlTypes.BIGINT)
        .valueColumn(COL0, SqlTypes.STRING)
        .valueColumn(ColumnName.of("KSQL_COL_0"), SqlTypes.INTEGER)
        .build()));
  }

  @Test
  public void shouldBuildResultSchemaWhenPartitioningByNull() {
    // Given:
    final Expression partitionBy = new NullLiteral();

    // When:
    final LogicalSchema resultSchema = PartitionByParamsFactory.buildSchema(
        SCHEMA,
        partitionBy,
        functionRegistry
    );

    // Then:
    assertThat(resultSchema, is(LogicalSchema.builder()
        .valueColumn(COL1, SqlTypes.INTEGER)
        .valueColumn(COL2, SqlTypes.INTEGER)
        .valueColumn(COL3, COL3_TYPE)
        .valueColumn(SystemColumns.ROWTIME_NAME, SqlTypes.BIGINT)
        .valueColumn(COL0, SqlTypes.STRING)
        .build()));
  }

  @Test
  public void shouldLogOnErrorExtractingNewKey() {
    // Given:
    final BiFunction<Object, GenericRow, KeyValue<Struct, GenericRow>> mapper =
        partitionBy(FAILING_UDF).getMapper();

    // When:
    mapper.apply(key, value);

    // Then:
    verify(logger).error(any());
  }

  @Test
  public void shouldPartitionByNullAnyRowsWhereFailedToExtractKey() {
    // Given:
    final BiFunction<Object, GenericRow, KeyValue<Struct, GenericRow>> mapper =
        partitionBy(FAILING_UDF).getMapper();

    // When:
    final KeyValue<Struct, GenericRow> result = mapper.apply(key, value);

    // Then:
    final KeyBuilder keyBuilder = StructKeyUtil
        .keyBuilder(ColumnName.of("KSQL_COL_0"), SqlTypes.INTEGER);

    assertThat(result.key, is(keyBuilder.build(null)));
  }

  @Test
  public void shouldSetNewKey() {
    // Given:
    final BiFunction<Object, GenericRow, KeyValue<Struct, GenericRow>> mapper =
        partitionBy(new UnqualifiedColumnReferenceExp(COL1)).getMapper();

    // When:
    final KeyValue<Struct, GenericRow> result = mapper.apply(key, value);

    // Then:
    final KeyBuilder keyBuilder = StructKeyUtil.keyBuilder(COL1, SqlTypes.INTEGER);
    assertThat(result.key, is(keyBuilder.build(COL1_VALUE)));
  }

  @Test
  public void shouldNotChangeValueIfPartitioningByColumnReference() {
    // Given:
    final BiFunction<Object, GenericRow, KeyValue<Struct, GenericRow>> mapper =
        partitionBy(new UnqualifiedColumnReferenceExp(COL1)).getMapper();

    final ImmutableList<Object> originals = ImmutableList.copyOf(value.values());

    // When:
    final KeyValue<Struct, GenericRow> result = mapper.apply(key, value);

    // Then:
    assertThat(result.value, is(GenericRow.fromList(originals)));
  }

  @Test
  public void shouldAppendNewKeyColumnToValueIfNotPartitioningByColumnReference() {
    // Given:
    final BiFunction<Object, GenericRow, KeyValue<Struct, GenericRow>> mapper =
        partitionBy(new FunctionCall(
            CONSTANT_UDF_NAME,
            ImmutableList.of(new UnqualifiedColumnReferenceExp(COL1)))
        ).getMapper();

    final ImmutableList<Object> originals = ImmutableList.copyOf(value.values());

    // When:
    final KeyValue<Struct, GenericRow> result = mapper.apply(key, value);

    // Then:
    assertThat(result.value, is(GenericRow.fromList(originals).append(ConstantUdf.VALUE)));
  }

  @Test
  public void shouldNotChangeValueIfPartitioningByNull() {
    // Given:
    final BiFunction<Object, GenericRow, KeyValue<Struct, GenericRow>> mapper =
        partitionBy(new NullLiteral()).getMapper();

    final ImmutableList<Object> originals = ImmutableList.copyOf(value.values());

    // When:
    final KeyValue<Struct, GenericRow> result = mapper.apply(key, value);

    // Then:
    assertThat(result.value, is(GenericRow.fromList(originals)));
  }

  private PartitionByParams partitionBy(final Expression expression) {
    return PartitionByParamsFactory
        .build(SCHEMA, expression,  KSQL_CONFIG, functionRegistry, logger);
  }

  public static class FailingUdf implements Kudf {

    @Override
    public Object evaluate(final Object... args) {
      throw new IllegalStateException();
    }
  }

  public static class ConstantUdf implements Kudf {

    private static final long VALUE = 42L;

    @Override
    public Object evaluate(final Object... args) {
      return VALUE;
    }
  }
}