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

package io.confluent.ksql.schema.ksql;

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.schema.ksql.types.SqlDecimal;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.util.KsqlException;
import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

public final class DefaultSqlValueCoercer implements SqlValueCoercer {

  private static final Map<SqlBaseType, Function<Number, Number>> UPCASTER =
      ImmutableMap.<SqlBaseType, Function<Number, Number>>builder()
          .put(SqlBaseType.INTEGER, Number::intValue)
          .put(SqlBaseType.BIGINT, Number::longValue)
          .put(SqlBaseType.DOUBLE, Number::doubleValue)
          .build();

  public <T> Optional<T> coerce(final Object value, final SqlType targetType) {
    if (targetType.baseType() == SqlBaseType.ARRAY
        || targetType.baseType() == SqlBaseType.MAP
        || targetType.baseType() == SqlBaseType.STRUCT
    ) {
      throw new KsqlException("Unsupported SQL type: " + targetType.baseType());
    }

    final SqlBaseType valueSqlType = SchemaConverters.javaToSqlConverter()
        .toSqlType(value.getClass());

    if (valueSqlType.equals(targetType.baseType())) {
      return optional(value);
    }

    if (targetType.baseType() == SqlBaseType.DECIMAL) {
      return coerceDecimal(value, (SqlDecimal) targetType);
    }

    if (!(value instanceof Number) || !valueSqlType.canUpCast(targetType.baseType())) {
      return Optional.empty();
    }

    final Number result = UPCASTER.get(targetType.baseType()).apply((Number) value);
    return optional(result);
  }

  private static <T> Optional<T> coerceDecimal(final Object value, final SqlDecimal targetType) {
    final int precision = targetType.getPrecision();
    final int scale = targetType.getScale();

    if (value instanceof String) {
      try {
        return optional(new BigDecimal((String) value, new MathContext(precision))
            .setScale(scale, RoundingMode.UNNECESSARY));
      } catch (final NumberFormatException e) {
        throw new KsqlException("Cannot coerce value to DECIMAL: " + value, e);
      }
    }

    if (value instanceof Number) {
      return optional(
          new BigDecimal(
              ((Number) value).doubleValue(),
              new MathContext(precision))
              .setScale(scale, RoundingMode.UNNECESSARY));
    }

    return Optional.empty();
  }

  @SuppressWarnings("unchecked")
  private static <T> Optional<T> optional(final Object value) {
    return Optional.of((T)value);
  }
}
