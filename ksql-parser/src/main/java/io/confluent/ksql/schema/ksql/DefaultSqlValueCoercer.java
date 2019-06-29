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
import io.confluent.ksql.util.DecimalUtil;
import io.confluent.ksql.util.KsqlException;
import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import org.apache.kafka.connect.data.Schema;

public final class DefaultSqlValueCoercer implements SqlValueCoercer {

  private static final Map<SqlBaseType, Function<Number, Number>> UPCASTER =
      ImmutableMap.<SqlBaseType, Function<Number, Number>>builder()
          .put(SqlBaseType.INTEGER, Number::intValue)
          .put(SqlBaseType.BIGINT, Number::longValue)
          .put(SqlBaseType.DOUBLE, Number::doubleValue)
          .build();

  public <T> Optional<T> coerce(final Object value, final Schema targetSchema) {
    final SqlBaseType valueSqlType = SchemaConverters.javaToSqlConverter()
        .toSqlType(value.getClass());
    final SqlBaseType targetSqlType = SchemaConverters.logicalToSqlConverter()
        .toSqlType(targetSchema)
        .baseType();

    if (valueSqlType.equals(targetSqlType)) {
      return optional(value);
    }

    if (DecimalUtil.isDecimal(targetSchema)) {
      final int precision = DecimalUtil.precision(targetSchema);
      final int scale = DecimalUtil.scale(targetSchema);
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

    if (!(value instanceof Number) || !valueSqlType.canUpCast(targetSqlType)) {
      return Optional.empty();
    }

    final Number result = UPCASTER.get(targetSqlType).apply((Number) value);
    return optional(result);
  }

  @SuppressWarnings("unchecked")
  private static <T> Optional<T> optional(final Object value) {
    return Optional.of((T)value);
  }
}
