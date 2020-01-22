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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.schema.ksql.types.SqlArray;
import io.confluent.ksql.schema.ksql.types.SqlDecimal;
import io.confluent.ksql.schema.ksql.types.SqlMap;
import io.confluent.ksql.schema.ksql.types.SqlStruct;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.util.DecimalUtil;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;

public enum DefaultSqlValueCoercer implements SqlValueCoercer {

  INSTANCE;

  private static final Map<SqlBaseType, BiFunction<Number, SqlType, Optional<Number>>> UPCASTER =
      ImmutableMap.<SqlBaseType, BiFunction<Number, SqlType, Optional<Number>>>builder()
          .put(SqlBaseType.INTEGER, (num, type) -> Optional.of(num.intValue()))
          .put(SqlBaseType.BIGINT, (num, type) -> Optional.of(num.longValue()))
          .put(SqlBaseType.DOUBLE, (num, type) -> Optional.of(num.doubleValue()))
          .put(SqlBaseType.DECIMAL, (num, type) -> {
            try {
              return Optional.ofNullable(
                  DecimalUtil.ensureFit(
                      new BigDecimal(String.format("%s", num)),
                      (SqlDecimal) type));
            } catch (final Exception e) {
              return Optional.empty();
            }
          }).build();

  @Override
  public Optional<?> coerce(final Object value, final SqlType targetType) {
    return doCoerce(value, targetType);
  }

  private static Optional<?> doCoerce(final Object value, final SqlType targetType) {
    switch (targetType.baseType()) {
      case ARRAY:
        return coerceArray(value, (SqlArray) targetType);
      case MAP:
        return coerceMap(value, (SqlMap) targetType);
      case STRUCT:
        return coerceStruct(value, (SqlStruct) targetType);
      default:
        break;
    }

    final SqlBaseType valueSqlType = SchemaConverters.javaToSqlConverter()
        .toSqlType(value.getClass());

    if (valueSqlType.equals(targetType.baseType())) {
      return Optional.of(value);
    }

    if (!(value instanceof Number) || !valueSqlType.canImplicitlyCast(targetType.baseType())) {
      return Optional.empty();
    }

    return UPCASTER.get(targetType.baseType()).apply((Number) value, targetType);
  }

  private static Optional<?> coerceStruct(final Object value, final SqlStruct targetType) {
    if (!(value instanceof Struct)) {
      return Optional.empty();
    }

    final Struct struct = (Struct) value;
    final Struct coerced = new Struct(
        SchemaConverters.sqlToConnectConverter().toConnectSchema(targetType)
    );

    for (final Field field : coerced.schema().fields()) {
      final Optional<io.confluent.ksql.schema.ksql.types.Field> sqlField =
          targetType.field(field.name());

      if (!sqlField.isPresent()) {
        // if there was a field in the struct that wasn't in the schema
        // we cannot coerce
        return Optional.empty();
      } else if (struct.schema().field(field.name()) == null) {
        // if we cannot find the field in the struct, we can ignore it
        continue;
      }

      final Optional<?> val = doCoerce(struct.get(field), sqlField.get().type());
      val.ifPresent(v -> coerced.put(field.name(), v));
    }

    return Optional.of(coerced);
  }

  private static Optional<?> coerceArray(final Object value, final SqlArray targetType) {
    if (!(value instanceof List<?>)) {
      return Optional.empty();
    }

    final List<?> list = (List<?>) value;
    final ImmutableList.Builder<Object> coerced = ImmutableList.builder();
    for (final Object el : list) {
      final Optional<?> coercedEl = doCoerce(el, targetType.getItemType());
      if (!coercedEl.isPresent()) {
        return Optional.empty();
      }
      coerced.add(coercedEl.get());
    }

    return Optional.of(coerced.build());
  }

  private static Optional<?> coerceMap(final Object value, final SqlMap targetType) {
    if (!(value instanceof Map<?, ?>)) {
      return Optional.empty();
    }

    final Map<?, ?> map = (Map<?, ?>) value;
    final HashMap<Object, Object> coerced = new HashMap<>();
    for (final Map.Entry entry : map.entrySet()) {
      final Optional<?> coercedKey = doCoerce(entry.getKey(), SqlTypes.STRING);
      final Optional<?> coercedValue = doCoerce(entry.getValue(), targetType.getValueType());
      if (!coercedKey.isPresent() || !coercedValue.isPresent()) {
        return Optional.empty();
      }
      coerced.put(coercedKey.get(), coercedValue.get());
    }

    return Optional.of(coerced);
  }
}
