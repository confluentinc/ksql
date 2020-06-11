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
import io.confluent.ksql.schema.ksql.types.SqlArray;
import io.confluent.ksql.schema.ksql.types.SqlBaseType;
import io.confluent.ksql.schema.ksql.types.SqlDecimal;
import io.confluent.ksql.schema.ksql.types.SqlMap;
import io.confluent.ksql.schema.ksql.types.SqlStruct;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.util.DecimalUtil;
import io.confluent.ksql.util.ParserUtil;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;

public enum DefaultSqlValueCoercer implements SqlValueCoercer {

  INSTANCE;

  private static final Map<SqlBaseType, BiFunction<Number, SqlType, Result>> UPCASTER =
      ImmutableMap.<SqlBaseType, BiFunction<Number, SqlType, Result>>builder()
          .put(SqlBaseType.INTEGER, (num, type) -> Result.of(num.intValue()))
          .put(SqlBaseType.BIGINT, (num, type) -> Result.of(num.longValue()))
          .put(SqlBaseType.DOUBLE, (num, type) -> Result.of(num.doubleValue()))
          .put(SqlBaseType.DECIMAL, (num, type) -> {
            try {
              return Result.of(
                  DecimalUtil.ensureFit(
                      new BigDecimal(String.format("%s", num)),
                      (SqlDecimal) type));
            } catch (final Exception e) {
              return Result.failure();
            }
          }).build();

  @Override
  public Result coerce(final Object value, final SqlType targetType) {
    return doCoerce(value, targetType);
  }

  private static Result doCoerce(final Object value, final SqlType targetType) {
    if (value == null) {
      // NULL can be cast to any type:
      return Result.nullResult();
    }

    switch (targetType.baseType()) {
      case ARRAY:
        return coerceArray(value, (SqlArray) targetType);
      case MAP:
        return coerceMap(value, (SqlMap) targetType);
      case STRUCT:
        return coerceStruct(value, (SqlStruct) targetType);
      default:
        return coerceOther(value, targetType);
    }
  }

  private static Result coerceOther(final Object value, final SqlType targetType) {
    final SqlBaseType valueSqlType = SchemaConverters.javaToSqlConverter()
        .toSqlType(value.getClass());

    if (valueSqlType.equals(targetType.baseType())) {
      return Result.of(value);
    }

    if (!(value instanceof Number) || !valueSqlType.canImplicitlyCast(targetType.baseType())) {
      return Result.failure();
    }

    return UPCASTER.get(targetType.baseType())
        .apply((Number) value, targetType);
  }

  private static Result coerceStruct(final Object value, final SqlStruct targetType) {
    final StructObject struct;
    if (value instanceof Struct) {
      struct = new ConnectStructObject((Struct) value);
    } else if (value instanceof Map<?, ?>) {
      struct = new MapStructObject((Map<?, ?>) value);
    } else {
      return Result.failure();
    }

    final Struct coerced = new Struct(
        SchemaConverters.sqlToConnectConverter().toConnectSchema(targetType)
    );

    for (final Field field : coerced.schema().fields()) {
      final Optional<io.confluent.ksql.schema.ksql.types.Field> sqlField =
          targetType.field(field.name());

      if (!sqlField.isPresent()) {
        // if there was a field in the struct that wasn't in the schema
        // we cannot coerce
        return Result.failure();
      }

      if (!struct.contains(field)) {
        // if we cannot find the field in the struct, we can ignore it
        continue;
      }

      final Result val = doCoerce(struct.get(field), sqlField.get().type());
      if (val.failed()) {
        return Result.failure();
      }

      val.value().ifPresent(v -> coerced.put(field.name(), v));
    }

    return Result.of(coerced);
  }

  private static Result coerceArray(final Object value, final SqlArray targetType) {
    if (!(value instanceof List<?>)) {
      return Result.failure();
    }

    final List<?> list = (List<?>) value;
    final List<Object> coerced = new ArrayList<>(list.size());
    for (final Object el : list) {
      final Result result = doCoerce(el, targetType.getItemType());
      if (result.failed()) {
        return Result.failure();
      }

      coerced.add(result.value().orElse(null));
    }

    return Result.of(coerced);
  }

  private static Result coerceMap(final Object value, final SqlMap targetType) {
    if (!(value instanceof Map<?, ?>)) {
      return Result.failure();
    }

    final Map<?, ?> map = (Map<?, ?>) value;
    final HashMap<Object, Object> coerced = new HashMap<>();
    for (final Map.Entry<?, ?> entry : map.entrySet()) {
      final Result coercedKey = doCoerce(entry.getKey(), SqlTypes.STRING);
      final Result coercedValue = doCoerce(entry.getValue(), targetType.getValueType());
      if (coercedKey.failed() || coercedValue.failed()) {
        return Result.failure();
      }

      coerced.put(coercedKey.value().orElse(null), coercedValue.value().orElse(null));
    }

    return Result.of(coerced);
  }

  private interface StructObject {
    boolean contains(Field field);

    Object get(Field field);
  }

  private static class ConnectStructObject implements StructObject {

    private final Struct struct;

    ConnectStructObject(final Struct struct) {
      this.struct = struct;
    }

    @Override
    public boolean contains(final Field field) {
      return struct.schema().field(field.name()) != null;
    }

    @Override
    public Object get(final Field field) {
      return struct.get(field);
    }
  }

  private static class MapStructObject implements StructObject {

    private final Map<String, Object> map;

    @SuppressWarnings("unchecked") // TODO: how to avoid?
    MapStructObject(final Map<?, ?> map) {
      this.map = ParserUtil.convertMapKeyCase((Map<String, Object>) map);
    }

    @Override
    public boolean contains(final Field field) {
      return map.containsKey(field.name());
    }

    @Override
    public Object get(final Field field) {
      return map.get(field.name());
    }
  }
}
