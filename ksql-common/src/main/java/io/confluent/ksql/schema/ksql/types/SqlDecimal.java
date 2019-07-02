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

import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.schema.ksql.FormatOptions;
import io.confluent.ksql.schema.ksql.SqlBaseType;
import io.confluent.ksql.util.DecimalUtil;
import java.util.Objects;

@Immutable
public final class SqlDecimal extends SqlType {

  private final int precision;
  private final int scale;

  public static SqlDecimal of(final int precision, final int scale) {
    return new SqlDecimal(precision, scale);
  }

  private SqlDecimal(final int precision, final int scale) {
    super(SqlBaseType.DECIMAL);
    this.precision = precision;
    this.scale = scale;

    DecimalUtil.validateParameters(precision, scale);
  }

  public int getPrecision() {
    return precision;
  }

  public int getScale() {
    return scale;
  }

  @Override
  public boolean supportsCast() {
    return true;
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
}
