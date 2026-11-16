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

import static io.confluent.ksql.schema.ksql.types.SqlDecimal.validateParameters;

import io.confluent.ksql.schema.ksql.types.SqlBaseType;
import io.confluent.ksql.schema.ksql.types.SqlDecimal;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Schema.Type;
import org.apache.kafka.connect.data.SchemaBuilder;

public final class DecimalUtil {

  public static final String PRECISION_FIELD = "connect.decimal.precision";
  public static final int PRECISION_DEFAULT = 64;

  private DecimalUtil() {
  }

  /**
   * @param schema the schema to test
   * @return whether or not the schema in question represents a decimal
   */
  public static boolean isDecimal(final Schema schema) {
    return schema.type() == Type.BYTES
        && Decimal.LOGICAL_NAME.equals(schema.name());
  }

  /**
   * @param schema the schema to test
   * @throws KsqlException if the schema does not match {@link #isDecimal(Schema)}
   */
  public static void requireDecimal(final Schema schema) {
    KsqlPreconditions.checkArgument(
        isDecimal(schema),
        String.format("Expected schema of type DECIMAL but got a schema of type %s and name %s",
            schema.type(),
            schema.name()));
  }

  /**
   * Returns a builder with a copy of the input schema.
   *
   * @param schema the input schema
   * @return a builder copy of the input schema, if it is a decimal schema
   */
  public static SchemaBuilder builder(final Schema schema) {
    requireDecimal(schema);
    return builder(precision(schema), scale(schema));
  }

  public static SchemaBuilder builder(final int precision, final int scale) {
    validateParameters(precision, scale);
    return org.apache.kafka.connect.data.Decimal
        .builder(scale)
        .optional()
        .parameter(PRECISION_FIELD, Integer.toString(precision));
  }

  /**
   * @param schema the schema
   * @return the scale for the schema
   */
  public static int scale(final Schema schema) {
    requireDecimal(schema);
    final String scaleString = schema.parameters()
        .get(org.apache.kafka.connect.data.Decimal.SCALE_FIELD);
    if (scaleString == null) {
      throw new KsqlException("Invalid Decimal schema: scale parameter not found.");
    }

    try {
      return Integer.parseInt(scaleString);
    } catch (final NumberFormatException e) {
      throw new KsqlException("Invalid scale parameter found in Decimal schema: ", e);
    }
  }

  /**
   * @param schema the schema
   * @return the precision for the schema
   */
  public static int precision(final Schema schema) {
    requireDecimal(schema);
    final String precisionString = schema.parameters().get(PRECISION_FIELD);
    if (precisionString == null) {
      return PRECISION_DEFAULT;
    }

    try {
      return Integer.parseInt(precisionString);
    } catch (final NumberFormatException e) {
      throw new KsqlException("Invalid precision parameter found in Decimal schema: ", e);
    }
  }

  /**
   * @see #ensureFit(BigDecimal, SqlDecimal)
   */
  public static BigDecimal ensureFit(final BigDecimal value, final Schema schema) {
    return ensureFit(value, precision(schema), scale(schema));
  }

  public static BigDecimal ensureFit(final BigDecimal value, final SqlDecimal schema) {
    return ensureFit(value, schema.getPrecision(), schema.getScale());
  }

  /**
   * @param value     a decimal value
   * @param precision the target precision
   * @param scale     the target scale
   *
   * @return the decimal value if it fits
   * @throws KsqlException if the value does not fit
   */
  public static BigDecimal ensureFit(final BigDecimal value, final int precision, final int scale) {
    if (value == null) {
      return null;
    }

    validateParameters(precision, scale);
    ensureMax(value, precision, scale);

    // we only support exact decimal conversions for now - in the future, we can
    // encode the rounding mode in the decimal type
    try {
      return value.setScale(scale, RoundingMode.UNNECESSARY);
    } catch (final ArithmeticException e) {
      throw new KsqlException(
          String.format(
              "Cannot fit decimal '%s' into DECIMAL(%d, %d) without rounding. (Requires %d,%d)",
              value.toPlainString(),
              precision,
              scale,
              value.precision(),
              value.scale()));
    }
  }

  /**
   * Converts a SQL type to a sql decimal with set precision/scale without losing scale or
   * precision.
   *
   * @param type the type to convert
   * @return the sql decimal
   * @throws KsqlException if the type cannot safely be converted to decimal
   */
  public static SqlDecimal toSqlDecimal(final SqlType type) {
    switch (type.baseType()) {
      case DECIMAL: return (SqlDecimal) type;
      case INTEGER: return SqlTypes.INT_UPCAST_TO_DECIMAL;
      case BIGINT:  return SqlTypes.BIGINT_UPCAST_TO_DECIMAL;
      default:
        throw new KsqlException(
            "Cannot convert " + type.baseType() + " to " + SqlBaseType.DECIMAL + ".");
    }
  }

  /**
   * Returns True if {@code s1} can be implicitly cast to {@code s2}.
   * </p>
   * A decimal {@code s1} can be implicitly cast if precision/scale fits into the {@code s2}
   * precision/scale.
   * <ul>
   *   <li>{@code s1} scale <= {@code s2} scale</li>
   *   <li>{@code s1} left digits <= {@code s2} left digits</li>
   * </ul>
   */
  public static boolean canImplicitlyCast(final SqlDecimal s1, final SqlDecimal s2) {
    return s1.getScale() <= s2.getScale()
        && (s1.getPrecision() - s1.getScale()) <= (s2.getPrecision() - s2.getScale());
  }

  public static BigDecimal cast(final Integer value, final int precision, final int scale) {
    if (value == null) {
      return null;
    }

    return cast(value.longValue(), precision, scale);
  }

  public static BigDecimal cast(final Long value, final int precision, final int scale) {
    if (value == null) {
      return null;
    }

    return cast(value.longValue(), precision, scale);
  }

  public static BigDecimal cast(final long value, final int precision, final int scale) {
    validateParameters(precision, scale);
    final BigDecimal decimal = new BigDecimal(value, new MathContext(precision));
    ensureMax(decimal, precision, scale);
    return decimal.setScale(scale, RoundingMode.UNNECESSARY);
  }

  public static BigDecimal cast(final Double value, final int precision, final int scale) {
    if (value == null) {
      return null;
    }

    return cast(value.doubleValue(), precision, scale);
  }

  public static BigDecimal cast(final double value, final int precision, final int scale) {
    validateParameters(precision, scale);
    final BigDecimal decimal = new BigDecimal(value, new MathContext(precision));
    ensureMax(decimal, precision, scale);
    return decimal.setScale(scale, RoundingMode.HALF_UP);
  }

  public static BigDecimal cast(final BigDecimal value, final int precision, final int scale) {
    if (value == null) {
      return null;
    }

    if (precision == value.precision() && scale == value.scale()) {
      return value;
    }

    validateParameters(precision, scale);
    ensureMax(value, precision, scale);
    return value.setScale(scale, RoundingMode.HALF_UP);
  }

  public static BigDecimal cast(final String value, final int precision, final int scale) {
    if (value == null) {
      return null;
    }

    validateParameters(precision, scale);
    final BigDecimal decimal = new BigDecimal(value.trim());
    ensureMax(decimal, precision, scale);
    return decimal.setScale(scale, RoundingMode.HALF_UP);
  }

  private static void ensureMax(final BigDecimal value, final int precision, final int scale) {
    final int digits = precision - scale;
    final BigDecimal maxValue = BigDecimal.valueOf(Math.pow(10, digits));
    if (maxValue.compareTo(value.abs()) < 1) {
      throw new ArithmeticException(
          String.format("Numeric field overflow: A field with precision %d and scale %d "
              + "must round to an absolute value less than 10^%d. Got %s",
              precision,
              scale,
              digits,
              value.toPlainString()));
    }
  }

  public static SqlType fromValue(final BigDecimal value) {
    // SqlDecimal does not support negative scale:
    final BigDecimal decimal = value.scale() < 0
        ? value.setScale(0, RoundingMode.UNNECESSARY)
        : value;

    /* We can't use BigDecimal.precision() directly for all cases, since it defines
     * precision differently from SQL Decimal.
     * In particular, if the decimal is between -0.1 and 0.1, BigDecimal precision can be
     * lower than scale, which is disallowed in SQL Decimal. For example, 0.005 in
     * BigDecimal has a precision,scale of 1,3; whereas we expect 4,3.
     * If the decimal is in (-1,1) but outside (-0.1,0.1), the code doesn't throw, but
     * gives lower precision than expected (e.g., 0.8 has precision 1 instead of 2).
     * To account for this edge case, we just take the scale and add one and use that
     * for the precision instead. This works since BigDecimal defines scale as the
     * number of digits to the right of the period; which is one lower than the precision for
     * anything in the range (-1, 1).
     * This covers the case where BigDecimal has a value of 0.
     * Note: This solution differs from the SQL definition in that it returns (4, 3) for
     * both "0.005" and ".005", whereas SQL expects (3, 3) for the latter. This is unavoidable
     * if we use BigDecimal as an intermediate representation, since the two strings are parsed
     * identically by it to have precision 1.
     */
    if (decimal.compareTo(BigDecimal.ONE) < 0 && decimal.compareTo(BigDecimal.ONE.negate()) > 0) {
      return SqlTypes.decimal(decimal.scale() + 1, decimal.scale());
    }
    return SqlTypes.decimal(decimal.precision(), Math.max(decimal.scale(), 0));
  }

  /**
   * Return a {@link SqlDecimal} wide enough to hold either the {@code t0} or {@code t1} types.
   *
   * <p>Both sides must support implicit casting to {@link SqlDecimal}.
   *
   * @param t0 the first type
   * @param t1 the second type
   * @return a type wide enough to hold either type.
   */
  public static SqlDecimal widen(final SqlType t0, final SqlType t1) {
    final SqlDecimal lDecimal = DecimalUtil.toSqlDecimal(t0);
    final SqlDecimal rDecimal = DecimalUtil.toSqlDecimal(t1);

    final int wholePrecision = Math.max(
        lDecimal.getPrecision() - lDecimal.getScale(),
        rDecimal.getPrecision() - rDecimal.getScale()
    );

    final int scale = Math.max(lDecimal.getScale(), rDecimal.getScale());

    return SqlTypes.decimal(wholePrecision + scale, scale);
  }
}
