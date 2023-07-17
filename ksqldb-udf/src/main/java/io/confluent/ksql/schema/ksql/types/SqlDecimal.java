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

package io.confluent.ksql.schema.ksql.types;

import com.google.common.collect.ImmutableMap;
import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.schema.utils.FormatOptions;
import io.confluent.ksql.schema.utils.SchemaException;
import java.util.Objects;

@Immutable
public final class SqlDecimal extends SqlType {

  private final int precision;
  private final int scale;
  public static final String PRECISION = "precision";
  public static final String SCALE = "scale";


  public static SqlDecimal of(final int precision, final int scale) {
    return new SqlDecimal(precision, scale);
  }

  private SqlDecimal(final int precision, final int scale) {
    super(SqlBaseType.DECIMAL);
    this.precision = precision;
    this.scale = scale;

    validateParameters(precision, scale);
  }

  public static void validateParameters(final int precision, final int scale) {
    checkCondition(precision > 0, String.format("DECIMAL precision must be >= 1: DECIMAL(%d,%d)",
                                                precision, scale));
    checkCondition(scale >= 0,
                   String.format("DECIMAL scale must be >= 0: DECIMAL(%d,%d)", precision, scale));
    checkCondition(precision >= scale,
                   String.format("DECIMAL precision must be >= scale: DECIMAL(%d,%d)",
                                 precision, scale));
  }

  public int getPrecision() {
    return precision;
  }

  public int getScale() {
    return scale;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final SqlDecimal that = (SqlDecimal) o;
    return precision == that.precision
        && scale == that.scale;
  }

  @Override
  public int hashCode() {
    return Objects.hash(precision, scale);
  }

  @Override
  public String toString() {
    return "DECIMAL(" + precision + ", " + scale + ')';
  }

  @Override
  public String toString(final FormatOptions formatOptions) {
    return toString();
  }

  public ImmutableMap<String, Object> toParametersMap() {
    return ImmutableMap.of(SqlDecimal.PRECISION, precision, SqlDecimal.SCALE, scale);
  }

  /**
   * Determine the decimal type should two decimals be added together.
   *
   * @param left the left side decimal.
   * @param right the right side decimal.
   * @return the resulting decimal type.
   */
  public static SqlDecimal add(final SqlDecimal left, final SqlDecimal right) {
    final int precision = Math.max(left.scale, right.scale)
        + Math.max(left.precision - left.scale, right.precision - right.scale)
        + 1;

    final int scale = Math.max(left.scale, right.scale);
    return SqlDecimal.of(precision, scale);
  }

  /**
   * Determine the decimal type should one decimal be subtracted from another.
   *
   * @param left the left side decimal.
   * @param right the right side decimal.
   * @return the resulting decimal type.
   */
  public static SqlDecimal subtract(final SqlDecimal left, final SqlDecimal right) {
    return add(left, right);
  }

  /**
   * Determine the decimal type should one decimal be multiplied by another.
   *
   * @param left the left side decimal.
   * @param right the right side decimal.
   * @return the resulting decimal type.
   */
  public static SqlDecimal multiply(final SqlDecimal left, final SqlDecimal right) {
    final int precision = left.precision + right.precision + 1;
    final int scale = left.scale + right.scale;
    return SqlDecimal.of(precision, scale);
  }

  /**
   * Determine the decimal type should one decimal be divided by another.
   *
   * @param left the left side decimal.
   * @param right the right side decimal.
   * @return the resulting decimal type.
   */
  public static SqlDecimal divide(final SqlDecimal left, final SqlDecimal right) {
    final int precision = left.precision - left.scale + right.scale
        + Math.max(6, left.scale + right.precision + 1);

    final int scale = Math.max(6, left.scale + right.precision + 1);
    return SqlDecimal.of(precision, scale);
  }

  /**
   * Determine the decimal result type when calculating the remainder of dividing one decimal by
   * another.
   *
   * @param left the left side decimal.
   * @param right the right side decimal.
   * @return the resulting decimal type.
   */
  public static SqlDecimal modulus(final SqlDecimal left, final SqlDecimal right) {
    final int precision = Math.min(left.precision - left.scale, right.precision - right.scale)
        + Math.max(left.scale, right.scale);

    final int scale = Math.max(left.scale, right.scale);
    return SqlDecimal.of(precision, scale);
  }

  private static void checkCondition(final boolean expression, final String message) {
    if (!expression) {
      throw new SchemaException(message);
    }
  }
}
