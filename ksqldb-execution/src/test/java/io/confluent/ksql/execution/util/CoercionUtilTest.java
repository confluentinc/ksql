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

package io.confluent.ksql.execution.util;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertThrows;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.execution.expression.tree.BooleanLiteral;
import io.confluent.ksql.execution.expression.tree.Cast;
import io.confluent.ksql.execution.expression.tree.CreateArrayExpression;
import io.confluent.ksql.execution.expression.tree.CreateMapExpression;
import io.confluent.ksql.execution.expression.tree.CreateStructExpression;
import io.confluent.ksql.execution.expression.tree.CreateStructExpression.Field;
import io.confluent.ksql.execution.expression.tree.DecimalLiteral;
import io.confluent.ksql.execution.expression.tree.DoubleLiteral;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.expression.tree.IntegerLiteral;
import io.confluent.ksql.execution.expression.tree.LambdaVariable;
import io.confluent.ksql.execution.expression.tree.LongLiteral;
import io.confluent.ksql.execution.expression.tree.NullLiteral;
import io.confluent.ksql.execution.expression.tree.StringLiteral;
import io.confluent.ksql.execution.expression.tree.Type;
import io.confluent.ksql.execution.expression.tree.UnqualifiedColumnReferenceExp;
import io.confluent.ksql.execution.util.CoercionUtil.Result;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlDecimal;
import io.confluent.ksql.schema.ksql.types.SqlStruct;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.util.KsqlException;
import java.math.BigDecimal;
import java.util.Collections;
import java.util.Optional;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class CoercionUtilTest {

  private static final LogicalSchema SCHEMA = LogicalSchema.builder()
      .valueColumn(ColumnName.of("BOOL"), SqlTypes.BOOLEAN)
      .valueColumn(ColumnName.of("INT"), SqlTypes.INTEGER)
      .valueColumn(ColumnName.of("BIGINT"), SqlTypes.BIGINT)
      .valueColumn(ColumnName.of("DECIMAL"), SqlTypes.decimal(4, 2))
      .valueColumn(ColumnName.of("DOUBLE"), SqlTypes.DOUBLE)
      .valueColumn(ColumnName.of("STR"), SqlTypes.STRING)
      .valueColumn(ColumnName.of("ARRAY_INT"), SqlTypes.array(SqlTypes.INTEGER))
      .valueColumn(ColumnName.of("ARRAY_BIGINT"), SqlTypes.array(SqlTypes.BIGINT))
      .valueColumn(ColumnName.of("ARRAY_STR"), SqlTypes.array(SqlTypes.STRING))
      .build();

  private static final Expression BOOL_EXPRESSION =
      new UnqualifiedColumnReferenceExp(ColumnName.of("BOOL"));
  private static final Expression INT_EXPRESSION =
      new UnqualifiedColumnReferenceExp(ColumnName.of("INT"));
  private static final Expression BIGINT_EXPRESSION =
      new UnqualifiedColumnReferenceExp(ColumnName.of("BIGINT"));
  private static final Expression DECIMAL_EXPRESSION =
      new UnqualifiedColumnReferenceExp(ColumnName.of("DECIMAL"));
  private static final Expression DOUBLE_EXPRESSION =
      new UnqualifiedColumnReferenceExp(ColumnName.of("DOUBLE"));
  private static final Expression STRING_EXPRESSION =
      new UnqualifiedColumnReferenceExp(ColumnName.of("STR"));

  @Mock
  private FunctionRegistry functionRegistry;
  private ExpressionTypeManager typeManager;

  @Before
  public void setUp() {
    typeManager = new ExpressionTypeManager(SCHEMA, functionRegistry);
  }

  @Test
  public void shouldHandleEmpty() {
    // Given:
    final ImmutableList<Expression> expressions = ImmutableList.of();

    // When:
    final Result result = CoercionUtil.coerceUserList(expressions, typeManager);

    // Then:
    assertThat(result.commonType(), is(Optional.empty()));
    assertThat(result.expressions(), is(ImmutableList.of()));
  }

  @Test
  public void shouldHandleOnlyNulls() {
    // Given:
    final ImmutableList<Expression> expressions = ImmutableList.of(
        new NullLiteral(),
        new NullLiteral()
    );

    // When:
    final Result result = CoercionUtil.coerceUserList(expressions, typeManager);

    // Then:
    assertThat(result.commonType(), is(Optional.empty()));
    assertThat(result.expressions(), is(ImmutableList.of(
        new NullLiteral(),
        new NullLiteral()
    )));
  }

  @Test
  public void shouldHandleSomeNulls() {
    // Given:
    final ImmutableList<Expression> expressions = ImmutableList.of(
        new NullLiteral(),
        new IntegerLiteral(10),
        new NullLiteral(),
        new LongLiteral(20L),
        new NullLiteral()
    );

    // When:
    final Result result = CoercionUtil.coerceUserList(expressions, typeManager);

    // Then:
    assertThat(result.commonType(), is(Optional.of(SqlTypes.BIGINT)));
    assertThat(result.expressions(), is(ImmutableList.of(
        new NullLiteral(),
        new LongLiteral(10),
        new NullLiteral(),
        new LongLiteral(20L),
        new NullLiteral()
    )));
  }

  @Test
  public void shouldDriveCoercionFromFirstNonNullExpression() {
    // Given:
    final ImmutableList<Expression> stringFirst = ImmutableList.of(
        new NullLiteral(),
        new StringLiteral("false"),
        new BooleanLiteral(true)
    );

    final ImmutableList<Expression> boolFirst = ImmutableList.of(
        new NullLiteral(),
        new BooleanLiteral(true),
        new StringLiteral("false")
    );

    // When:
    final Result stringResult = CoercionUtil.coerceUserList(stringFirst, typeManager);
    final Result boolResult = CoercionUtil.coerceUserList(boolFirst, typeManager);

    // Then:
    assertThat(stringResult.commonType(), is(Optional.of(SqlTypes.STRING)));
    assertThat(stringResult.expressions(), is(ImmutableList.of(
        new NullLiteral(),
        new StringLiteral("false"),
        new StringLiteral("true")
    )));

    assertThat(boolResult.commonType(), is(Optional.of(SqlTypes.BOOLEAN)));
    assertThat(boolResult.expressions(), is(ImmutableList.of(
        new NullLiteral(),
        new BooleanLiteral(true),
        new BooleanLiteral(false)
    )));
  }

  @Test
  public void shouldThrowOnIncompatible() {
    // Given:
    final ImmutableList<Expression> expressions = ImmutableList.of(
        new BooleanLiteral(true),
        new IntegerLiteral(10) // <-- can not be coerced to boolean.
    );

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> CoercionUtil.coerceUserList(expressions, typeManager)
    );

    // Then:
    assertThat(e.getMessage(), is("operator does not exist: BOOLEAN = INTEGER (10)"
        + System.lineSeparator()
        + "Hint: You might need to add explicit type casts.")
    );
  }

  @Test
  public void shouldCoerceToBooleans() {
    // Given:
    final ImmutableList<Expression> expressions = ImmutableList.of(
        new BooleanLiteral(true),
        new StringLiteral("FaLsE"),
        BOOL_EXPRESSION
    );

    // When:
    final Result result = CoercionUtil.coerceUserList(expressions, typeManager);

    // Then:
    assertThat(result.commonType(), is(Optional.of(SqlTypes.BOOLEAN)));
    assertThat(result.expressions(), is(ImmutableList.of(
        new BooleanLiteral(true),
        new BooleanLiteral(false),
        BOOL_EXPRESSION
    )));
  }

  @Test
  public void shouldNotCoerceStringExpressionToBoolean() {
    // Given:
    final ImmutableList<Expression> expressions = ImmutableList.of(
        new BooleanLiteral(true),
        STRING_EXPRESSION
    );

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> CoercionUtil.coerceUserList(expressions, typeManager)
    );

    // Then:
    assertThat(e.getMessage(), startsWith("operator does not exist: BOOLEAN = STRING (STR)"));
  }

  @Test
  public void shouldCoerceToInts() {
    // Given:
    final ImmutableList<Expression> expressions = ImmutableList.of(
        new IntegerLiteral(10),
        new StringLiteral("\t -100 \t"),
        INT_EXPRESSION
    );

    // When:
    final Result result = CoercionUtil.coerceUserList(expressions, typeManager);

    // Then:
    assertThat(result.commonType(), is(Optional.of(SqlTypes.INTEGER)));
    assertThat(result.expressions(), is(ImmutableList.of(
        new IntegerLiteral(10),
        new IntegerLiteral(-100),
        INT_EXPRESSION
    )));
  }

  @Test
  public void shouldCoerceToBigInts() {
    // Given:
    final ImmutableList<Expression> expressions = ImmutableList.of(
        new IntegerLiteral(10),
        new LongLiteral(1234567890),
        new StringLiteral("\t -100 \t"),
        BIGINT_EXPRESSION,
        INT_EXPRESSION
    );

    // When:
    final Result result = CoercionUtil.coerceUserList(expressions, typeManager);

    // Then:
    assertThat(result.commonType(), is(Optional.of(SqlTypes.BIGINT)));
    assertThat(result.expressions(), is(ImmutableList.of(
        new LongLiteral(10),
        new LongLiteral(1234567890),
        new LongLiteral(-100),
        BIGINT_EXPRESSION,
        cast(INT_EXPRESSION, SqlTypes.BIGINT)
    )));
  }

  @Test
  public void shouldCoerceToBigIntIfStringNumericTooWideForInt() {
    // Given:
    final ImmutableList<Expression> expressions = ImmutableList.of(
        new IntegerLiteral(10),
        new StringLiteral("1234567890000")
    );

    // When:
    final Result result = CoercionUtil.coerceUserList(expressions, typeManager);

    // Then:
    assertThat(result.commonType(), is(Optional.of(SqlTypes.BIGINT)));
    assertThat(result.expressions(), is(ImmutableList.of(
        new LongLiteral(10),
        new LongLiteral(1234567890000L)
    )));
  }

  @Test
  public void shouldCoerceToDecimals() {
    // Given:
    final ImmutableList<Expression> expressions = ImmutableList.of(
        new IntegerLiteral(10),
        new LongLiteral(1234567890),
        new StringLiteral("\t -100.010 \t"),
        BIGINT_EXPRESSION,
        DECIMAL_EXPRESSION,
        INT_EXPRESSION
    );

    // When:
    final Result result = CoercionUtil.coerceUserList(expressions, typeManager);

    // Then:
    final SqlDecimal decimalType = SqlTypes.decimal(22, 3);
    assertThat(result.commonType(), is(Optional.of(decimalType)));
    assertThat(result.expressions(), is(ImmutableList.of(
        new DecimalLiteral(new BigDecimal("10.000")),
        new DecimalLiteral(new BigDecimal("1234567890.000")),
        new DecimalLiteral(new BigDecimal("-100.010")),
        cast(BIGINT_EXPRESSION, decimalType),
        cast(DECIMAL_EXPRESSION, decimalType),
        cast(INT_EXPRESSION, decimalType)
    )));
  }

  @Test
  public void shouldCoerceStringNumericWithENotationToDecimals() {
    // Given:
    final ImmutableList<Expression> expressions = ImmutableList.of(
        new IntegerLiteral(10),
        new StringLiteral("1e3")
    );

    // When:
    final Result result = CoercionUtil.coerceUserList(expressions, typeManager);

    // Then:
    assertThat(result.commonType(), is(Optional.of(SqlTypes.decimal(10, 0))));
    assertThat(result.expressions(), is(ImmutableList.of(
        new DecimalLiteral(new BigDecimal("10")),
        new DecimalLiteral(new BigDecimal("1000"))
    )));
  }

  @Test
  public void shouldCoerceStringNumericWithDecimalPointToDecimals() {
    // Given:
    final ImmutableList<Expression> expressions = ImmutableList.of(
        new IntegerLiteral(10),
        new StringLiteral("1.0")
    );

    // When:
    final Result result = CoercionUtil.coerceUserList(expressions, typeManager);

    // Then:
    assertThat(result.commonType(), is(Optional.of(SqlTypes.decimal(11, 1))));
    assertThat(result.expressions(), is(ImmutableList.of(
        new DecimalLiteral(new BigDecimal("10.0")),
        new DecimalLiteral(new BigDecimal("1.0"))
    )));
  }

  @Test
  public void shouldCoerceToIntsToDecimals() {
    // Given:
    final ImmutableList<Expression> expressions = ImmutableList.of(
        new DecimalLiteral(new BigDecimal("1.1")),
        new IntegerLiteral(10),
        INT_EXPRESSION
    );

    // When:
    final Result result = CoercionUtil.coerceUserList(expressions, typeManager);

    // Then:
    final SqlDecimal decimalType = SqlTypes.decimal(11, 1);
    assertThat(result.commonType(), is(Optional.of(decimalType)));
    assertThat(result.expressions(), is(ImmutableList.of(
        new DecimalLiteral(new BigDecimal("1.1")),
        new DecimalLiteral(new BigDecimal("10.0")),
        cast(INT_EXPRESSION, decimalType)
    )));
  }

  @Test
  public void shouldCoerceToDoubles() {
    // Given:
    final ImmutableList<Expression> expressions = ImmutableList.of(
        new IntegerLiteral(10),
        new LongLiteral(1234567890),
        new DoubleLiteral(123.456),
        new StringLiteral("\t -100.010 \t"),
        BIGINT_EXPRESSION,
        DECIMAL_EXPRESSION,
        INT_EXPRESSION,
        DOUBLE_EXPRESSION
    );

    // When:
    final Result result = CoercionUtil.coerceUserList(expressions, typeManager);

    // Then:
    assertThat(result.commonType(), is(Optional.of(SqlTypes.DOUBLE)));
    assertThat(result.expressions(), is(ImmutableList.of(
        new DoubleLiteral(10.0),
        new DoubleLiteral(1234567890.0),
        new DoubleLiteral(123.456),
        new DoubleLiteral(-100.01),
        cast(BIGINT_EXPRESSION, SqlTypes.DOUBLE),
        cast(DECIMAL_EXPRESSION, SqlTypes.DOUBLE),
        cast(INT_EXPRESSION, SqlTypes.DOUBLE),
        DOUBLE_EXPRESSION
    )));
  }

  @Test
  public void shouldNotCoerceStringExpressionToNumber() {
    // Given:
    final ImmutableList<Expression> expressions = ImmutableList.of(
        new IntegerLiteral(10),
        STRING_EXPRESSION
    );

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> CoercionUtil.coerceUserList(expressions, typeManager)
    );

    // Then:
    assertThat(e.getMessage(), startsWith("operator does not exist: INTEGER = STRING (STR)"));
  }

  @Test
  public void shouldCoerceArrayOfCompatibleLiterals() {
    // Given:
    final ImmutableList<Expression> expressions = ImmutableList.of(
        new CreateArrayExpression(
            ImmutableList.of(
                new IntegerLiteral(10),
                new IntegerLiteral(289476)
            )
        ),
        new CreateArrayExpression(
            ImmutableList.of(
                new StringLiteral("123456789000"),
                new StringLiteral("22"),
                new StringLiteral("\t -100 \t")
            )
        )
    );

    // When:
    final Result result = CoercionUtil.coerceUserList(expressions, typeManager);

    // Then:
    assertThat(result.commonType(), is(Optional.of(SqlTypes.array(SqlTypes.BIGINT))));
    assertThat(result.expressions(), is(ImmutableList.of(
        cast(new CreateArrayExpression(
            ImmutableList.of(
                new IntegerLiteral(10),
                new IntegerLiteral(289476)
            )
        ), SqlTypes.array(SqlTypes.BIGINT)),
        cast(new CreateArrayExpression(
            ImmutableList.of(
                new StringLiteral("123456789000"),
                new StringLiteral("22"),
                new StringLiteral("\t -100 \t")
            )
        ), SqlTypes.array(SqlTypes.BIGINT))
    )));
  }

  @Test
  public void shouldNotCoerceArrayOfIncompatibleLiterals() {
    // Given:
    final ImmutableList<Expression> expressions = ImmutableList.of(
        new CreateArrayExpression(
            ImmutableList.of(
                new IntegerLiteral(10)
            )
        ),
        new CreateArrayExpression(
            ImmutableList.of(
                new BooleanLiteral(false)
            )
        )
    );

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> CoercionUtil.coerceUserList(expressions, typeManager)
    );

    // Then:
    assertThat(e.getMessage(),
        startsWith("operator does not exist: ARRAY<INTEGER> = ARRAY<BOOLEAN> (ARRAY[false])"));
  }

  @Test
  public void shouldNotCoerceArrayWithDifferentExpression() {
    // Given:
    final ImmutableList<Expression> expressions = ImmutableList.of(
        new CreateArrayExpression(
            ImmutableList.of(
                new IntegerLiteral(10)
            )
        ),
        new CreateArrayExpression(
            ImmutableList.of(
                STRING_EXPRESSION
            )
        )
    );

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> CoercionUtil.coerceUserList(expressions, typeManager)
    );

    // Then:
    assertThat(e.getMessage(),
        startsWith("operator does not exist: ARRAY<INTEGER> = ARRAY<STRING> (ARRAY[STR])"));
  }

  @Test
  public void shouldCoerceMapOfCompatibleLiterals() {
    // Given:
    final ImmutableList<Expression> expressions = ImmutableList.of(
        new CreateMapExpression(
            ImmutableMap.of(
                new IntegerLiteral(10),
                new IntegerLiteral(289476)
            )
        ),
        new CreateMapExpression(
            ImmutableMap.of(
                new StringLiteral("123456789000"),
                new StringLiteral("\t -100 \t")
            )
        )
    );

    // When:
    final Result result = CoercionUtil.coerceUserList(expressions, typeManager);

    // Then:
    assertThat(result.commonType(), is(Optional.of(SqlTypes.map(SqlTypes.BIGINT, SqlTypes.INTEGER))));
    assertThat(result.expressions(), is(ImmutableList.of(
        cast(new CreateMapExpression(
            ImmutableMap.of(
                new IntegerLiteral(10),
                new IntegerLiteral(289476)
            )
        ), SqlTypes.map(SqlTypes.BIGINT, SqlTypes.INTEGER)),
        cast(new CreateMapExpression(
            ImmutableMap.of(
                new StringLiteral("123456789000"),
                new StringLiteral("\t -100 \t")
            )
        ), SqlTypes.map(SqlTypes.BIGINT, SqlTypes.INTEGER))
    )));
  }

  @Test
  public void shouldNotCoerceMapOfIncompatibleKeyLiterals() {
    // Given:
    final ImmutableList<Expression> expressions = ImmutableList.of(
        new CreateMapExpression(
            ImmutableMap.of(
                new IntegerLiteral(10),
                new IntegerLiteral(289476)
            )
        ),
        new CreateMapExpression(
            ImmutableMap.of(
                new BooleanLiteral(false),
                new StringLiteral("123456789000")
            )
        )
    );

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> CoercionUtil.coerceUserList(expressions, typeManager)
    );

    // Then:
    assertThat(e.getMessage(),
        startsWith("operator does not exist: MAP<INTEGER, INTEGER> = MAP<BOOLEAN, STRING> (MAP(false:='123456789000'))"));
  }

  @Test
  public void shouldNotCoerceMapOfIncompatibleValueLiterals() {
    // Given:
    final ImmutableList<Expression> expressions = ImmutableList.of(
        new CreateMapExpression(
            ImmutableMap.of(
                new IntegerLiteral(10),
                new IntegerLiteral(289476)
            )
        ),
        new CreateMapExpression(
            ImmutableMap.of(
                new StringLiteral("123456789000"),
                new BooleanLiteral(false)
            )
        )
    );

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> CoercionUtil.coerceUserList(expressions, typeManager)
    );

    // Then:
    assertThat(e.getMessage(),
        startsWith("operator does not exist: MAP<INTEGER, INTEGER> = MAP<STRING, BOOLEAN> (MAP('123456789000':=false))"));
  }

  @Test
  public void shouldNotCoerceMapWithDifferentKeyExpression() {
    // Given:
    final ImmutableList<Expression> expressions = ImmutableList.of(
        new CreateMapExpression(
            ImmutableMap.of(
                new IntegerLiteral(10),
                new IntegerLiteral(10)
            )
        ),
        new CreateMapExpression(
            ImmutableMap.of(
                STRING_EXPRESSION,
                new IntegerLiteral(10)
            )
        )
    );

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> CoercionUtil.coerceUserList(expressions, typeManager)
    );

    // Then:
    assertThat(e.getMessage(),
        startsWith("operator does not exist: MAP<INTEGER, INTEGER> = MAP<STRING, INTEGER> (MAP(STR:=10))"));
  }

  @Test
  public void shouldNotCoerceMapWithDifferentValueExpression() {
    // Given:
    final ImmutableList<Expression> expressions = ImmutableList.of(
        new CreateMapExpression(
            ImmutableMap.of(
                new IntegerLiteral(10),
                new IntegerLiteral(10)
            )
        ),
        new CreateMapExpression(
            ImmutableMap.of(
                new IntegerLiteral(10),
                STRING_EXPRESSION
            )
        )
    );

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> CoercionUtil.coerceUserList(expressions, typeManager)
    );

    // Then:
    assertThat(e.getMessage(),
        startsWith("operator does not exist: MAP<INTEGER, INTEGER> = MAP<INTEGER, STRING> (MAP(10:=STR))"));
  }

  @Test
  public void shouldCoerceStructOfCompatibleLiterals() {
    // Given:
    final ImmutableList<Expression> expressions = ImmutableList.of(
        new CreateStructExpression(
            ImmutableList.of(
                new Field("a", new IntegerLiteral(10))
            )
        ),
        new CreateStructExpression(
            ImmutableList.of(
                new Field("a", new StringLiteral("123456789000"))
            )
        )
    );

    // When:
    final Result result = CoercionUtil.coerceUserList(expressions, typeManager);

    // Then:
    final SqlStruct sqlStruct = SqlTypes.struct().field("a", SqlTypes.BIGINT).build();
    assertThat(result.commonType(), is(Optional.of(
        sqlStruct)));
    assertThat(result.expressions(), is(ImmutableList.of(
        cast(new CreateStructExpression(
            ImmutableList.of(
                new Field("a", new IntegerLiteral(10))
            )
        ), sqlStruct),
        cast(new CreateStructExpression(
            ImmutableList.of(
                new Field("a", new StringLiteral("123456789000"))
            )
        ), sqlStruct)
    )));
  }

  @Test
  public void shouldNotCoerceStructOfIncompatibleLiterals() {
    // Given:
    final ImmutableList<Expression> expressions = ImmutableList.of(
        new CreateStructExpression(
            ImmutableList.of(
                new Field("a", new IntegerLiteral(10))
            )
        ),
        new CreateStructExpression(
            ImmutableList.of(
                new Field("a", new BooleanLiteral(false))
            )
        )
    );

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> CoercionUtil.coerceUserList(expressions, typeManager)
    );

    // Then:
    assertThat(e.getMessage(),
        startsWith("operator does not exist: STRUCT<`a` INTEGER> = STRUCT<`a` BOOLEAN> (STRUCT(a:=false))"));
  }

  @Test
  public void shouldNotCoerceStructWithDifferentExpression() {
    // Given:
    final ImmutableList<Expression> expressions = ImmutableList.of(
        new CreateStructExpression(
            ImmutableList.of(
                new Field("a", new IntegerLiteral(10))
            )
        ),
        new CreateStructExpression(
            ImmutableList.of(
                new Field("a", STRING_EXPRESSION)
            )
        )
    );

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> CoercionUtil.coerceUserList(expressions, typeManager)
    );

    // Then:
    assertThat(e.getMessage(),
        startsWith("operator does not exist: STRUCT<`a` INTEGER> = STRUCT<`a` STRING> (STRUCT(a:=STR))"));
  }

  @Test
  public void shouldCoerceLambdaVariables() {
    // Given:
    final ImmutableList<Expression> expressions = ImmutableList.of(
        BIGINT_EXPRESSION,
        new LambdaVariable("X"),
        INT_EXPRESSION
    );

    // When:
    final Result result = CoercionUtil.coerceUserList(
        expressions,
        typeManager,
        Collections.singletonMap("X", SqlTypes.INTEGER)
    );

    // Then:
    assertThat(result.commonType(), is(Optional.of(SqlTypes.BIGINT)));
    assertThat(result.expressions(), is(ImmutableList.of(
        BIGINT_EXPRESSION,
        cast(new LambdaVariable("X"), SqlTypes.BIGINT),
        cast(INT_EXPRESSION, SqlTypes.BIGINT)
    )));
  }

  private static Cast cast(final Expression expression, final SqlType sqlType) {
    return new Cast(expression, new Type(sqlType));
  }
}
