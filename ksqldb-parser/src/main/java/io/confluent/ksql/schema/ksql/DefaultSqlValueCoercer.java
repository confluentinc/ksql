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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.schema.ksql.types.SqlArray;
import io.confluent.ksql.schema.ksql.types.SqlBaseType;
import io.confluent.ksql.schema.ksql.types.SqlDecimal;
import io.confluent.ksql.schema.ksql.types.SqlMap;
import io.confluent.ksql.schema.ksql.types.SqlStruct;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.util.DecimalUtil;
import io.confluent.ksql.util.ParserUtil;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;

public enum DefaultSqlValueCoercer implements SqlValueCoercer {

  INSTANCE(false),
  API_INSTANCE(true);

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

  private final boolean allowCastStringAndDoubleToDecimal;

  DefaultSqlValueCoercer(final boolean allowCastStringAndDoubleToDecimal) {
    this.allowCastStringAndDoubleToDecimal = allowCastStringAndDoubleToDecimal;
  }

  @VisibleForTesting
  boolean isAllowCastStringAndDoubleToDecimal() {
    return allowCastStringAndDoubleToDecimal;
  }

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

  private Result coerceOther(final Object value, final SqlType targetType) {
    if ((targetType instanceof SqlDecimal) && allowCastStringAndDoubleToDecimal) {
      final SqlDecimal decType = (SqlDecimal) targetType;
      if (value instanceof Double) {
        return Result.of(new BigDecimal(String.valueOf(value))
            .setScale(decType.getScale(), RoundingMode.HALF_UP));
      } else if (value instanceof String) {
        return Result.of(new BigDecimal((String) value)
            .setScale(decType.getScale(), RoundingMode.HALF_UP));
      }
    }

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

  private Result coerceStruct(final Object value, final SqlStruct targetType) {
    final StructObject struct;
    if (value instanceof Struct) {
      struct = ConnectStructObject.of((Struct) value);
    } else if (value instanceof JsonObject) {
      struct = JsonStructObject.of((JsonObject) value);
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

  private Result coerceArray(final Object value, final SqlArray targetType) {
    final ListObject list;
    if (value instanceof List<?>) {
      list = JavaListObject.of((List<?>) value);
    } else if (value instanceof JsonArray) {
      list = JsonListObject.of((JsonArray) value);
    } else {
      return Result.failure();
    }

    final List<Object> coerced = new ArrayList<>(list.size());
    for (int i = 0; i < list.size(); i++) {
      final Result result = doCoerce(list.get(i), targetType.getItemType());
      if (result.failed()) {
        return Result.failure();
      }

      coerced.add(result.value().orElse(null));
    }

    return Result.of(coerced);
  }

  private Result coerceMap(final Object value, final SqlMap targetType) {
    final MapObject map;
    if (value instanceof Map<?, ?>) {
      map = JavaMapObject.of((Map<?, ?>) value);
    } else if (value instanceof JsonObject) {
      map = JsonMapObject.of((JsonObject) value);
    } else {
      return Result.failure();
    }

    final HashMap<Object, Object> coerced = new HashMap<>();
    for (final Object key : map.keys()) {
      final Result coercedKey = doCoerce(key, targetType.getKeyType());
      final Result coercedValue = doCoerce(map.get(key), targetType.getValueType());
      if (coercedKey.failed() || coercedValue.failed()) {
        return Result.failure();
      }

      coerced.put(coercedKey.value().orElse(null), coercedValue.value().orElse(null));
    }

    return Result.of(coerced);
  }

  private interface ListObject {
    int size();

    Object get(int index);
  }

  private static final class JavaListObject implements ListObject {

    private final List<?> list;

    private JavaListObject(final List<?> list) {
      this.list = list;
    }

    @Override
    public int size() {
      return list.size();
    }

    @Override
    public Object get(final int index) {
      return list.get(index);
    }

    static JavaListObject of(final List<?> list) {
      return new JavaListObject(list);
    }
  }

  private static final class JsonListObject implements ListObject {

    private final JsonArray array;

    private JsonListObject(final JsonArray array) {
      this.array = array;
    }

    @Override
    public int size() {
      return array.size();
    }

    @Override
    public Object get(final int index) {
      return array.getValue(index);
    }

    static JsonListObject of(final JsonArray array) {
      return new JsonListObject(array);
    }
  }

  private interface MapObject {
    Set<?> keys();

    Object get(Object key);
  }

  private static final class JavaMapObject implements MapObject {

    private final Map<?, ?> map;

    private JavaMapObject(final Map<?, ?> map) {
      this.map = map;
    }

    @Override
    public Set<?> keys() {
      return map.keySet();
    }

    @Override
    public Object get(final Object key) {
      return map.get(key);
    }

    static JavaMapObject of(final Map<?, ?> map) {
      return new JavaMapObject(map);
    }
  }

  private static final class JsonMapObject implements MapObject {

    private final JsonObject obj;

    private JsonMapObject(final JsonObject obj) {
      this.obj = obj;
    }

    @Override
    public Set<?> keys() {
      return obj.fieldNames();
    }

    @Override
    public Object get(final Object key) {
      return obj.getValue(key.toString());
    }

    static JsonMapObject of(final JsonObject obj) {
      return new JsonMapObject(obj);
    }
  }

  private interface StructObject {
    boolean contains(Field field);

    Object get(Field field);
  }

  private static final class ConnectStructObject implements StructObject {

    private final Struct struct;

    private ConnectStructObject(final Struct struct) {
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

    static ConnectStructObject of(final Struct struct) {
      return new ConnectStructObject(struct);
    }
  }

  private static final class JsonStructObject implements StructObject {

    private final JsonObject obj;

    private JsonStructObject(final JsonObject obj) {
      // Coercion of JsonObject fields is case-insensitive, as this code path does not go through
      // the parser (which handles case sensitivity automatically for Connect Structs)
      this.obj = ParserUtil.convertJsonFieldCase(obj);
    }

    @Override
    public boolean contains(final Field field) {
      return obj.containsKey(field.name());
    }

    @Override
    public Object get(final Field field) {
      return obj.getValue(field.name());
    }

    static JsonStructObject of(final JsonObject obj) {
      return new JsonStructObject(obj);
    }
  }
}
