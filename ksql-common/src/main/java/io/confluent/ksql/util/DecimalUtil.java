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

import io.confluent.ksql.schema.ksql.types.SqlDecimal;
import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.text.DecimalFormat;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Schema.Type;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.errors.DataException;

public final class DecimalUtil {

  private static final String PRECISION_FIELD = "connect.decimal.precision";

  private DecimalUtil() { }

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
      throw new DataException("Invalid Decimal schema: scale parameter not found.");
    }

    try {
      return Integer.parseInt(scaleString);
    } catch (NumberFormatException e) {
      throw new DataException("Invalid scale parameter found in Decimal schema: ", e);
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
      throw new DataException("Invalid Decimal schema: precision parameter not found.");
    }

    try {
      return Integer.parseInt(precisionString);
    } catch (NumberFormatException e) {
      throw new DataException("Invalid precision parameter found in Decimal schema: ", e);
    }
  }

  /**
   * Formats the decimal string, padding zeros if necessary.
   *
   * @param value the value
   * @return the formatted string
   */
  public static String format(final int precision, final int scale, final BigDecimal value) {
    final DecimalFormat format = new DecimalFormat();
    format.setMinimumIntegerDigits(precision - scale);
    format.setMinimumFractionDigits(scale);

    return format.format(value);
  }

  /**
   * @param value  a decimal value
   * @param schema the schema that it should fit within
   * @return the decimal value if it fits
   * @throws KsqlException if the value does not fit
   */
  public static BigDecimal ensureFit(final BigDecimal value, final Schema schema) {
    if (value == null) {
      return null;
    }

    final int precision = precision(schema);
    final int scale = scale(schema);

    validateParameters(precision, scale);
    ensureMax(value, precision, scale);

    // we only support exact decimal conversions for now - in the future, we can
    // encode the rounding mode in the decimal type
    try {
      return value.setScale(scale, RoundingMode.UNNECESSARY);
    } catch (final ArithmeticException e) {
      throw new KsqlException(
          String.format(
              "Cannot fit decimal '%s' into DECIMAL(%d, %d) without rounding.",
              value.toPlainString(),
              precision,
              scale));
    }
  }

  /**
   * Converts a schema to a decimal schema with set precision/scale without losing
   * scale or precision.
   *
   * @param schema the schema
   * @return the decimal schema
   * @throws KsqlException if the schema cannot safely be converted to decimal
   */
  public static Schema toDecimal(final Schema schema) {
    switch (schema.type()) {
      case BYTES:
        requireDecimal(schema);
        return schema;
      case INT32:
        return builder(10, 0).build();
      case INT64:
        return builder(19, 0).build();
      default:
        throw new KsqlException("Cannot convert schema of type " + schema.type() + " to decimal.");
    }
  }

  /**
   * Converts a schema to a sql decimal with set precision/scale without losing
   * scale or precision.
   *
   * @param schema the schema
   * @return the sql decimal
   * @throws KsqlException if the schema cannot safely be converted to decimal
   */
  public static SqlDecimal toSqlDecimal(final Schema schema) {
    switch (schema.type()) {
      case BYTES:
        requireDecimal(schema);
        return SqlDecimal.of(precision(schema), scale(schema));
      case INT32:
        return SqlDecimal.of(10, 0);
      case INT64:
        return SqlDecimal.of(19, 0);
      default:
        throw new KsqlException("Cannot convert schema of type " + schema.type() + " to decimal.");
    }
  }

  public static BigDecimal cast(final long value, final int precision, final int scale) {
    validateParameters(precision, scale);
    final BigDecimal decimal = new BigDecimal(value, new MathContext(precision));
    ensureMax(decimal, precision, scale);
    return decimal.setScale(scale, RoundingMode.UNNECESSARY);
  }

  public static BigDecimal cast(final double value, final int precision, final int scale) {
    validateParameters(precision, scale);
    final BigDecimal decimal = new BigDecimal(value, new MathContext(precision));
    ensureMax(decimal, precision, scale);
    return decimal.setScale(scale, RoundingMode.HALF_UP);
  }

  public static BigDecimal cast(final BigDecimal value, final int precision, final int scale) {
    if (precision == value.precision() && scale == value.scale()) {
      return value;
    }

    validateParameters(precision, scale);
    ensureMax(value, precision, scale);
    return value.setScale(scale, RoundingMode.HALF_UP);
  }

  public static BigDecimal cast(final String value, final int precision, final int scale) {
    validateParameters(precision, scale);
    final BigDecimal decimal = new BigDecimal(value);
    ensureMax(decimal, precision, scale);
    return decimal.setScale(scale, RoundingMode.HALF_UP);
  }

  private static void ensureMax(final BigDecimal value, final int precision, final int scale) {
    final int digits = precision - scale;
    final BigDecimal maxValue = new BigDecimal(Math.pow(10, digits));
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

  public static void validateParameters(final int precision, final int scale) {
    KsqlPreconditions.checkArgument(precision > 0,
        String.format("DECIMAL precision must be >= 1: DECIMAL(%d,%d)", precision, scale));
    KsqlPreconditions.checkArgument(scale >= 0,
        String.format("DECIMAL scale must be >= 0: DECIMAL(%d,%d)", precision, scale));
    KsqlPreconditions.checkArgument(precision >= scale,
        String.format("DECIMAL precision must be >= scale: DECIMAL(%d,%d)", precision, scale));
  }
}
