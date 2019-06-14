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

import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
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

    validateParameters(precision(schema), scale(schema));

    final int leftDigits = value.precision() - value.scale();
    final int fitLeftDigits = precision - scale;
    KsqlPreconditions.checkArgument(leftDigits <= fitLeftDigits,
        String.format("Cannot fit decimal '%s' into DECIMAL(%d, %d) as it would truncate left "
            + "digits to %d.", value.toPlainString(), precision, scale, fitLeftDigits));

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

  public static BigDecimal cast(final BigDecimal val, final int precision, final int scale) {
    if (val.scale() > scale || val.precision() - val.scale() > precision - scale) {
      throw new ArithmeticException(
          String.format("Rounding necessary to fit %s into DECIMAL(%d, %d)", val, precision, scale)
      );
    }

    return val.setScale(scale, RoundingMode.UNNECESSARY);
  }

  public static BigDecimal cast(final long val, final int precision, final int scale) {
    if (val >= maxFit(precision, scale)) {
      throw new ArithmeticException(
          String.format("Rounding necessary to fit %d into DECIMAL(%d, %d)", val, precision, scale)
      );
    }

    return new BigDecimal(val, new MathContext(precision - scale, RoundingMode.UNNECESSARY))
        .setScale(scale, RoundingMode.UNNECESSARY);
  }

  public static BigDecimal cast(final double val, final int precision, final int scale) {
    if (val >= maxFit(precision, scale)) {
      throw new ArithmeticException(
          String.format("Rounding necessary to fit %f into DECIMAL(%d, %d)", val, precision, scale)
      );
    }

    // since decimals are floating point numbers, we always need to round decimals
    return new BigDecimal(val, new MathContext(precision, RoundingMode.HALF_UP))
        .setScale(scale, RoundingMode.UNNECESSARY);
  }

  public static BigDecimal cast(final String val, final int precision, final int scale) {
    int numDigits = 0;
    for (int i = 0; i < val.length(); i++) {
      numDigits += Character.isDigit(val.charAt(i)) ? 1 : 0;
    }

    if (numDigits > precision) {
      throw new ArithmeticException(
          String.format("Rounding necessary to fit %s into DECIMAL(%d, %d)", val, precision, scale)
      );
    }

    final BigDecimal decimal =
        new BigDecimal(val, new MathContext(precision, RoundingMode.UNNECESSARY))
            .setScale(scale, RoundingMode.UNNECESSARY);

    if (decimal.compareTo(new BigDecimal(maxFit(precision, scale))) >= 0) {
      throw new ArithmeticException(
          String.format("Rounding necessary to fit %s into DECIMAL(%d, %d)", val, precision, scale)
      );
    }

    return decimal;
  }

  private static double maxFit(final int precision, final int scale) {
    // BigDecimal allows storing values with negative scale - this means that storing
    // the number 10 in 1 precision is possible. Since KSQL does not allow negative scales,
    // we need to manually check to make sure that the val is smaller than 10^precision-scale
    final int digitsLeftOfZero = precision - scale;
    return Math.pow(10, digitsLeftOfZero);
  }

  private static void validateParameters(final int precision, final int scale) {
    KsqlPreconditions.checkArgument(precision > 0,
        String.format("DECIMAL precision must be >= 1: DECIMAL(%d,%d)", precision, scale));
    KsqlPreconditions.checkArgument(scale >= 0,
        String.format("DECIMAL scale must be >= 0: DECIMAL(%d,%d)", precision, scale));
    KsqlPreconditions.checkArgument(precision >= scale,
        String.format("DECIMAL precision must be >= scale: DECIMAL(%d,%d)", precision, scale));
  }
}
