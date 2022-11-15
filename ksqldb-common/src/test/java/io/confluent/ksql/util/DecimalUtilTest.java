/*
 * Copyright 2019 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"; you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.util;

import static io.confluent.ksql.util.DecimalUtil.builder;
import static io.confluent.ksql.util.DecimalUtil.cast;
import static io.confluent.ksql.util.DecimalUtil.ensureFit;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.assertThrows;

import io.confluent.ksql.schema.ksql.types.SqlDecimal;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.schema.utils.SchemaException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.junit.Test;

@SuppressWarnings("ConstantConditions")
public class DecimalUtilTest {

  private static final Schema DECIMAL_SCHEMA = DecimalUtil.builder(2, 1).build();

  @Test
  public void shouldBuildCorrectSchema() {
    // Then:
    assertThat(DECIMAL_SCHEMA, is(Decimal.builder(1).parameter("connect.decimal.precision", "2").optional().build()));
  }

  @Test
  public void shouldCopyBuilder() {
    // When:
    final Schema copy = DecimalUtil.builder(DECIMAL_SCHEMA).build();

    // Then:
    assertThat(copy, is(DECIMAL_SCHEMA));
  }

  @Test
  public void shouldCheckWhetherSchemaIsDecimal() {
    // Then:
    assertThat("Expected DECIMAL_SCHEMA to be isDecimal", DecimalUtil.isDecimal(DECIMAL_SCHEMA));
  }

  @Test
  public void shouldNotCheckSchemaForNonDecimals() {
    // Given:
    final Schema notDecimal = Schema.OPTIONAL_STRING_SCHEMA;

    // Then:
    assertThat("String should not be decimal schema", !DecimalUtil.isDecimal(notDecimal));
  }

  @Test
  public void shouldExtractScaleFromDecimalSchema() {
    // When:
    final int scale = DecimalUtil.scale(DECIMAL_SCHEMA);

    // Then:
    assertThat(scale, is(1));
  }

  @Test
  public void shouldExtractPrecisionFromDecimalSchema() {
    // When:
    final int precision = DecimalUtil.precision(DECIMAL_SCHEMA);

    // Then:
    assertThat(precision, is(2));
  }

  @Test
  public void shouldUseDefaultPrecisionIfNotPresentInSchema() {
    // When:
    final int precision = DecimalUtil.precision(decimalSchemaWithoutPrecision(3));

    // Then:
    assertThat(precision, is(64));
  }

  @Test
  public void shouldExtractPrecisionFromZeroValue() {
    // When:
    final SqlType zeroDecimal = DecimalUtil
        .fromValue(BigDecimal.ZERO.setScale(2, RoundingMode.UNNECESSARY));

    // Then:
    assertThat(zeroDecimal, is(SqlTypes.decimal(3, 2)));
  }

  @Test
  public void shouldCastDecimal() {
    // When:
    final BigDecimal decimal = DecimalUtil.cast(new BigDecimal("1.1"), 3, 2);

    // Then:
    assertThat(decimal, is(new BigDecimal("1.10")));
  }

  @Test
  public void shouldCastNullDecimal() {
    // When:
    final BigDecimal decimal = DecimalUtil.cast((BigDecimal) null, 3, 2);

    // Then:
    assertThat(decimal, is(nullValue()));
  }

  @Test
  public void shouldCastDecimalNoOp() {
    // When:
    final BigDecimal decimal = DecimalUtil.cast(new BigDecimal("1.1"), 2, 1);

    // Then:
    assertThat(decimal, sameInstance(decimal));
  }

  @Test
  public void shouldCastDecimalNegative() {
    // When:
    final BigDecimal decimal = DecimalUtil.cast(new BigDecimal("-1.1"), 3, 2);

    // Then:
    assertThat(decimal, is(new BigDecimal("-1.10")));
  }

  @Test
  public void shouldCastDecimalRoundingDown() {
    // When:
    final BigDecimal decimal = DecimalUtil.cast(new BigDecimal("1.12"), 2, 1);

    // Then:
    assertThat(decimal, is(new BigDecimal("1.1")));
  }

  @Test
  public void shouldCastDecimalRoundingUpNegative() {
    // When:
    final BigDecimal decimal = DecimalUtil.cast(new BigDecimal("-1.12"), 2, 1);

    // Then:
    assertThat(decimal, is(new BigDecimal("-1.1")));
  }

  @Test
  public void shouldCastDecimalRoundingUp() {
    // When:
    final BigDecimal decimal = DecimalUtil.cast(new BigDecimal("1.19"), 2, 1);

    // Then:
    assertThat(decimal, is(new BigDecimal("1.2")));
  }

  @Test
  public void shouldCastDecimalRoundingDownNegative() {
    // When:
    final BigDecimal decimal = DecimalUtil.cast(new BigDecimal("-1.19"), 2, 1);

    // Then:
    assertThat(decimal, is(new BigDecimal("-1.2")));
  }

  @Test
  public void shouldCastInt() {
    // When:
    final BigDecimal decimal = DecimalUtil.cast((Integer)1, 2, 1);

    // Then:
    assertThat(decimal, is(new BigDecimal("1.0")));
  }

  @Test
  public void shouldCastNullInt() {
    // When:
    final BigDecimal decimal = DecimalUtil.cast((Integer)null, 2, 1);

    // Then:
    assertThat(decimal, is(nullValue()));
  }

  @Test
  public void shouldCastIntNegative() {
    // When:
    final BigDecimal decimal = DecimalUtil.cast(-1, 2, 1);

    // Then:
    assertThat(decimal, is(new BigDecimal("-1.0")));
  }

  @Test
  public void shouldCastBigInt() {
    // When:
    final BigDecimal decimal = DecimalUtil.cast((Long)1L, 2, 1);

    // Then:
    assertThat(decimal, is(new BigDecimal("1.0")));
  }

  @Test
  public void shouldCastNullBigInt() {
    // When:
    final BigDecimal decimal = DecimalUtil.cast((Long)null, 2, 1);

    // Then:
    assertThat(decimal, is(nullValue()));
  }

  @Test
  public void shouldCastDouble() {
    // When:
    final BigDecimal decimal = DecimalUtil.cast((Double)1.1, 2, 1);

    // Then:
    assertThat(decimal, is(new BigDecimal("1.1")));
  }

  @Test
  public void shouldCastNullDouble() {
    // When:
    final BigDecimal decimal = DecimalUtil.cast((Double)null, 2, 1);

    // Then:
    assertThat(decimal, is(nullValue()));
  }

  @Test
  public void shouldCastDoubleNegative() {
    // When:
    final BigDecimal decimal = DecimalUtil.cast(-1.1, 2, 1);

    // Then:
    assertThat(decimal, is(new BigDecimal("-1.1")));
  }

  @Test
  public void shouldCastDoubleRoundDown() {
    // When:
    final BigDecimal decimal = DecimalUtil.cast(1.11, 2, 1);

    // Then:
    assertThat(decimal, is(new BigDecimal("1.1")));
  }

  @Test
  public void shouldCastDoubleRoundUp() {
    // When:
    final BigDecimal decimal = DecimalUtil.cast(1.19, 2, 1);

    // Then:
    assertThat(decimal, is(new BigDecimal("1.2")));
  }

  @Test
  public void shouldCastString() {
    // When:
    final BigDecimal decimal = DecimalUtil.cast("1.1", 3, 2);

    // Then:
    assertThat(decimal, is(new BigDecimal("1.10")));
  }

  @Test
  public void shouldCastNullString() {
    // When:
    final BigDecimal decimal = DecimalUtil.cast((String)null, 3, 2);

    // Then:
    assertThat(decimal, is(nullValue()));
  }

  @Test
  public void shouldCastStringNegative() {
    // When:
    final BigDecimal decimal = DecimalUtil.cast("-1.1", 3, 2);

    // Then:
    assertThat(decimal, is(new BigDecimal("-1.10")));
  }

  @Test
  public void shouldCastStringRoundDown() {
    // When:
    final BigDecimal decimal = DecimalUtil.cast("1.12", 2, 1);

    // Then:
    assertThat(decimal, is(new BigDecimal("1.1")));
  }

  @Test
  public void shouldCastStringRoundUp() {
    // When:
    final BigDecimal decimal = DecimalUtil.cast("1.19", 2, 1);

    // Then:
    assertThat(decimal, is(new BigDecimal("1.2")));
  }

  @Test
  public void shouldConvertIntegerToSqlDecimal() {
    // When:
    final SqlDecimal decimal = DecimalUtil.toSqlDecimal(SqlTypes.INTEGER);

    // Then:
    assertThat(decimal, is(SqlTypes.decimal(10, 0)));
  }

  @Test
  public void shouldConvertLongToSqlDecimal() {
    // When:
    final SqlDecimal decimal = DecimalUtil.toSqlDecimal(SqlTypes.BIGINT);

    // Then:
    assertThat(decimal, is(SqlTypes.decimal(19, 0)));
  }

  @Test
  public void shouldConvertDecimalToSqlDecimal() {
    // Given:
    final SqlDecimal given = SqlTypes.decimal(2, 2);

    // When:
    final SqlDecimal decimal = DecimalUtil.toSqlDecimal(given);

    // Then:
    assertThat(decimal, is(SqlTypes.decimal(2, 2)));
  }

  @Test
  public void shouldEnsureFitIfExactMatch() {
    // No Exception When:
    DecimalUtil.ensureFit(new BigDecimal("1.2"), DECIMAL_SCHEMA);
  }

  @Test
  public void shouldGetSchemaFromDecimal2_2() {
    // When:
    final SqlType schema = DecimalUtil.fromValue(new BigDecimal(".12"));

    // Note: this behavior is different from the SQL specification, where
    // we expect precision = 2, scale = 2. This difference is because we use
    // BigDecimal in our implementation, which treats precision differently.

    // Then:
    assertThat(schema, is(SqlTypes.decimal(3, 2)));
  }

  @Test
  public void shouldGetSchemaFromDecimal2_0() {
    // When:
    final SqlType schema = DecimalUtil.fromValue(new BigDecimal("12."));

    // Then:
    assertThat(schema, is(SqlTypes.decimal(2, 0)));
  }

  @Test
  public void shouldGetSchemaFromDecimal1_0() {
    // When:
    final SqlType schema = DecimalUtil.fromValue(new BigDecimal("0"));

    // Then:
    assertThat(schema, is(SqlTypes.decimal(1, 0)));
  }

  @Test
  public void shouldGetSchemaFromDecimal10_5() {
    // When:
    final SqlType schema = DecimalUtil.fromValue(new BigDecimal("12345.12345"));

    // Then:
    assertThat(schema, is(SqlTypes.decimal(10, 5)));
  }

  @Test
  public void shouldGetSchemaFromDecimal4_3() {
    // When:
    final SqlType schema = DecimalUtil.fromValue(new BigDecimal("0.005"));

    // Then:
    assertThat(schema, is(SqlTypes.decimal(4, 3)));
  }

  @Test
  public void shouldFailIfBuilderWithZeroPrecision() {
    // When:
    final Exception e = assertThrows(
        SchemaException.class,
        () -> builder(0, 0)
    );

    // Then:
    assertThat(e.getMessage(), containsString("DECIMAL precision must be >= 1"));
  }

  @Test
  public void shouldFailIfBuilderWithNegativeScale() {
    // When:
    final Exception e = assertThrows(
        SchemaException.class,
        () -> builder(1, -1)
    );

    // Then:
    assertThat(e.getMessage(), containsString("DECIMAL scale must be >= 0"));
  }

  @Test
  public void shouldFailIfBuilderWithScaleGTPrecision() {
    // When:
    final Exception e = assertThrows(
        SchemaException.class,
        () -> builder(1, 2)
    );

    // Then:
    assertThat(e.getMessage(), containsString("DECIMAL precision must be >= scale"));
  }

  @Test
  public void shouldFailFitIfNotExactMatchMoreDigits() {
    // When:
    final Exception e = assertThrows(
        ArithmeticException.class,
        () -> ensureFit(new BigDecimal("12"), DECIMAL_SCHEMA)
    );

    // Then:
    assertThat(e.getMessage(), containsString("Numeric field overflow: A field with precision 2 and "
        + "scale 1 must round to an absolute value less than 10^1. Got 12"));
  }

  @Test
  public void shouldFailFitIfTruncationNecessary() {
    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> ensureFit(new BigDecimal("1.23"), DECIMAL_SCHEMA)
    );

    // Then:
    assertThat(e.getMessage(), containsString("Cannot fit decimal '1.23' into DECIMAL(2, 1) without rounding."));
  }

  @Test
  public void shouldNotCastDecimalTooBig() {
    // When:
    final Exception e = assertThrows(
        ArithmeticException.class,
        () -> cast(new BigDecimal(10), 2, 1)
    );

    // Then:
    assertThat(e.getMessage(), containsString("Numeric field overflow"));
  }

  @Test
  public void shouldNotCastDecimalTooNegative() {
    // When:
    final Exception e = assertThrows(
        ArithmeticException.class,
        () -> cast(new BigDecimal(-10), 2, 1)
    );

    // Then:
    assertThat(e.getMessage(), containsString("Numeric field overflow"));
  }

  @Test
  public void shouldNotCastIntTooBig() {
    // When:
    final Exception e = assertThrows(
        ArithmeticException.class,
        () -> cast(10, 2, 1)
    );

    // Then:
    assertThat(e.getMessage(), containsString("Numeric field overflow"));
  }

  @Test
  public void shouldNotCastIntTooNegative() {
    // When:
    final Exception e = assertThrows(
        ArithmeticException.class,
        () -> cast(-10, 2, 1)
    );

    // Then:
    assertThat(e.getMessage(), containsString("Numeric field overflow"));
  }

  @Test
  public void shouldNotCastDoubleTooBig() {
    // When:
    final Exception e = assertThrows(
        ArithmeticException.class,
        () -> cast(10.0, 2, 1)
    );

    // Then:
    assertThat(e.getMessage(), containsString("Numeric field overflow"));
  }

  @Test
  public void shouldNotCastDoubleTooNegative() {
    // When:
    final Exception e = assertThrows(
        ArithmeticException.class,
        () -> cast(-10.0, 2, 1)
    );

    // Then:
    assertThat(e.getMessage(), containsString("Numeric field overflow"));
  }

  @Test
  public void shouldNotCastStringTooBig() {
    // When:
    final Exception e = assertThrows(
        ArithmeticException.class,
        () -> cast("10", 2, 1)
    );

    // Then:
    assertThat(e.getMessage(), containsString("Numeric field overflow"));
  }

  @Test
  public void shouldNotCastStringTooNegative() {
    // When:
    final Exception e = assertThrows(
        ArithmeticException.class,
        () -> cast("-10", 2, 1)
    );

    // Then:
    assertThat(e.getMessage(), containsString("Numeric field overflow"));
  }

  @Test
  public void shouldNotCastStringNonNumber() {
    // When:
    assertThrows(
        NumberFormatException.class,
        () -> cast("abc", 2, 1)
    );
  }

  @Test
  public void shouldAllowImplicitlyCastOnEqualSchema() {
    // Given:
    final SqlDecimal s1 = SqlTypes.decimal(5, 2);
    final SqlDecimal s2 = SqlTypes.decimal(5, 2);

    // When:
    final boolean compatible = DecimalUtil.canImplicitlyCast(s1, s2);

    // Then:
    assertThat(compatible, is(true));
  }

  @Test
  public void shouldAllowImplicitlyCastOnHigherPrecisionAndScale() {
    // Given:
    final SqlDecimal s1 = SqlTypes.decimal(5, 2);
    final SqlDecimal s2 = SqlTypes.decimal(6, 3);

    // When:
    final boolean compatible = DecimalUtil.canImplicitlyCast(s1, s2);

    // Then:
    assertThat(compatible, is(true));
  }

  @Test
  public void shouldAllowImplicitlyCastOnHigherScale() {
    // Given:
    final SqlDecimal s1 = SqlTypes.decimal(2, 1);
    final SqlDecimal s2 = SqlTypes.decimal(2, 2);

    // When:
    final boolean compatible = DecimalUtil.canImplicitlyCast(s1, s2);

    // Then:
    assertThat(compatible, is(false));
  }

  @Test
  public void shouldAllowImplicitlyCastOnLowerPrecision() {
    // Given:
    final SqlDecimal s1 = SqlTypes.decimal(2, 1);
    final SqlDecimal s2 = SqlTypes.decimal(1, 1);

    // When:
    final boolean compatible = DecimalUtil.canImplicitlyCast(s1, s2);

    // Then:
    assertThat(compatible, is(false));
  }

  @Test
  public void shouldWidenDecimalAndDecimal() {
    assertThat(
        "first contained",
        DecimalUtil.widen(
            SqlTypes.decimal(1, 0),
            SqlTypes.decimal(14, 3)
        ),
        is(SqlTypes.decimal(14, 3))
    );

    assertThat(
        "second contained",
        DecimalUtil.widen(
            SqlTypes.decimal(14, 3),
            SqlTypes.decimal(1, 0)
        ),
        is(SqlTypes.decimal(14, 3))
    );

    assertThat(
        "fractional",
        DecimalUtil.widen(
            SqlTypes.decimal(14, 14),
            SqlTypes.decimal(1, 1)
        ),
        is(SqlTypes.decimal(14, 14))
    );

    assertThat(
        "overlap",
        DecimalUtil.widen(
            SqlTypes.decimal(14, 4),
            SqlTypes.decimal(14, 5)
        ),
        is(SqlTypes.decimal(15, 5))
    );
  }

  @Test
  public void shouldWidenIntAndLong() {
    assertThat(
        DecimalUtil.widen(SqlTypes.BIGINT, SqlTypes.INTEGER),
        is(SqlTypes.BIGINT_UPCAST_TO_DECIMAL)
    );

    assertThat(
        DecimalUtil.widen(SqlTypes.INTEGER, SqlTypes.BIGINT),
        is(SqlTypes.BIGINT_UPCAST_TO_DECIMAL)
    );
  }

  @Test
  public void shouldWidenIntAndDecimal() {
    // Given:
    final SqlDecimal smallerPrecision = SqlTypes.decimal(4, 3);
    final SqlDecimal largerPrecision = SqlTypes.decimal(11, 0);

    // Then:
    assertThat(
        DecimalUtil.widen(smallerPrecision, SqlTypes.INTEGER),
        is(SqlTypes.decimal(13, 3))
    );

    assertThat(
        DecimalUtil.widen(SqlTypes.INTEGER, largerPrecision),
        is(SqlTypes.decimal(11, 0))
    );
  }

  @Test
  public void shouldWidenBigIntAndDecimal() {
    // Given:
    final SqlDecimal smallerPrecision = SqlTypes.decimal(14, 3);
    final SqlDecimal largerPrecision = SqlTypes.decimal(20, 0);

    // Then:
    assertThat(
        DecimalUtil.widen(smallerPrecision, SqlTypes.BIGINT),
        is(SqlTypes.decimal(22, 3))
    );

    assertThat(
        DecimalUtil.widen(SqlTypes.BIGINT, largerPrecision),
        is(SqlTypes.decimal(20, 0))
    );
  }

  @Test
  public void shouldConvertFromBigDecimalWithNegativeScale() {
    assertThat(
        DecimalUtil.fromValue(new BigDecimal("1e3")),
        is(SqlTypes.decimal(4, 0))
    );
  }

  private static Schema decimalSchemaWithoutPrecision(final int scale) {
    return org.apache.kafka.connect.data.Decimal
        .builder(scale)
        .optional()
        .build();
  }
}