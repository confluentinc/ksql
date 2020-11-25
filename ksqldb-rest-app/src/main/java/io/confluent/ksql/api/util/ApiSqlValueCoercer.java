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

package io.confluent.ksql.api.util;

import io.confluent.ksql.schema.ksql.DefaultSqlValueCoercer;
import io.confluent.ksql.schema.ksql.SchemaConverters;
import io.confluent.ksql.schema.ksql.SqlValueCoercer;
import io.confluent.ksql.schema.ksql.types.SqlArray;
import io.confluent.ksql.schema.ksql.types.SqlDecimal;
import io.confluent.ksql.schema.ksql.types.SqlMap;
import io.confluent.ksql.schema.ksql.types.SqlStruct;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.util.JsonUtil;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;

/**
 * SQL value coercer used by the server API
 *
 * <p>Extends the {@link DefaultSqlValueCoercer} to support vertex {@link JsonObject} and
 * {@link JsonArray} types and to allow coercion of SQL {@code STRING}s and {@code DOUBLE}s to
 * {@code DECIMAL}.
 */
public enum ApiSqlValueCoercer implements SqlValueCoercer {

  INSTANCE;

  private static final SqlValueCoercer DEFAULT_COERCER = DefaultSqlValueCoercer.STRICT;

  @Override
  public Result coerce(final Object value, final SqlType targetType) {
    return doCoerce(value, targetType);
  }

  private Result doCoerce(final Object value, final SqlType targetType) {
    if (value == null) {
      // NULL can be cast to any type:
      return Result.nullResult();
    }

    switch (targetType.baseType()) {
      case DECIMAL:
        return coerceDecimal(value, (SqlDecimal) targetType);
      case ARRAY:
        return coerceArray(value, (SqlArray) targetType);
      case MAP:
        return coerceMap(value, (SqlMap) targetType);
      case STRUCT:
        return coerceStruct(value, (SqlStruct) targetType);
      default:
        return defaultCoerce(value, targetType);
    }
  }

  private static Result coerceDecimal(final Object value, final SqlDecimal targetType) {
    if (value instanceof Double) {
      return Result.of(new BigDecimal(String.valueOf(value))
          .setScale(targetType.getScale(), RoundingMode.HALF_UP));
    }

    if (value instanceof String) {
      return Result.of(new BigDecimal((String) value)
          .setScale(targetType.getScale(), RoundingMode.HALF_UP));
    }

    return defaultCoerce(value, targetType);
  }

  private Result coerceStruct(final Object value, final SqlStruct targetType) {
    if (!(value instanceof JsonObject)) {
      return Result.failure();
    }

    final JsonObject struct = JsonUtil.convertJsonFieldCase((JsonObject) value);

    final Struct coerced = new Struct(
        SchemaConverters.sqlToConnectConverter().toConnectSchema(targetType)
    );

    for (final Field field : coerced.schema().fields()) {
      final Optional<SqlStruct.Field> sqlField =
          targetType.field(field.name());

      if (!sqlField.isPresent()) {
        // if there was a field in the struct that wasn't in the schema
        // we cannot coerce
        return Result.failure();
      }

      if (!struct.fieldNames().contains(field.name())) {
        // if we cannot find the field in the struct, we can ignore it
        continue;
      }

      final Result val = doCoerce(struct.getValue(field.name()), sqlField.get().type());
      if (val.failed()) {
        return Result.failure();
      }

      val.value().ifPresent(v -> coerced.put(field.name(), v));
    }

    return Result.of(coerced);
  }

  private Result coerceArray(final Object value, final SqlArray targetType) {
    if (!(value instanceof JsonArray)) {
      return Result.failure();
    }

    final JsonArray array = (JsonArray) value;

    final List<Object> coerced = new ArrayList<>(array.size());
    for (int i = 0; i < array.size(); i++) {
      final Result result = doCoerce(array.getValue(i), targetType.getItemType());
      if (result.failed()) {
        return Result.failure();
      }

      coerced.add(result.value().orElse(null));
    }

    return Result.of(coerced);
  }

  private Result coerceMap(final Object value, final SqlMap targetType) {
    if (!(value instanceof JsonObject)) {
      return Result.failure();
    }

    final JsonObject map = (JsonObject) value;

    final HashMap<Object, Object> coerced = new HashMap<>();
    for (final String key : map.fieldNames()) {
      final Result coercedKey = doCoerce(key, targetType.getKeyType());
      final Result coercedValue = doCoerce(map.getValue(key), targetType.getValueType());
      if (coercedKey.failed() || coercedValue.failed()) {
        return Result.failure();
      }

      coerced.put(coercedKey.value().orElse(null), coercedValue.value().orElse(null));
    }

    return Result.of(coerced);
  }

  private static Result defaultCoerce(final Object value, final SqlType targetType) {
    if (value instanceof JsonObject || value instanceof JsonArray) {
      return Result.failure();
    }

    return DEFAULT_COERCER.coerce(value, targetType);
  }
}
