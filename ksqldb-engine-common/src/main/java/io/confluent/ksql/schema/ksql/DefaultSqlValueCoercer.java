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

import static io.confluent.ksql.schema.ksql.types.SqlBaseType.ARRAY;
import static io.confluent.ksql.schema.ksql.types.SqlBaseType.BIGINT;
import static io.confluent.ksql.schema.ksql.types.SqlBaseType.BOOLEAN;
import static io.confluent.ksql.schema.ksql.types.SqlBaseType.BYTES;
import static io.confluent.ksql.schema.ksql.types.SqlBaseType.DATE;
import static io.confluent.ksql.schema.ksql.types.SqlBaseType.DECIMAL;
import static io.confluent.ksql.schema.ksql.types.SqlBaseType.DOUBLE;
import static io.confluent.ksql.schema.ksql.types.SqlBaseType.INTEGER;
import static io.confluent.ksql.schema.ksql.types.SqlBaseType.MAP;
import static io.confluent.ksql.schema.ksql.types.SqlBaseType.STRING;
import static io.confluent.ksql.schema.ksql.types.SqlBaseType.STRUCT;
import static io.confluent.ksql.schema.ksql.types.SqlBaseType.TIME;
import static io.confluent.ksql.schema.ksql.types.SqlBaseType.TIMESTAMP;
import static java.util.Objects.requireNonNull;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Streams;
import io.confluent.ksql.schema.ksql.types.SqlArray;
import io.confluent.ksql.schema.ksql.types.SqlBaseType;
import io.confluent.ksql.schema.ksql.types.SqlDecimal;
import io.confluent.ksql.schema.ksql.types.SqlMap;
import io.confluent.ksql.schema.ksql.types.SqlStruct;
import io.confluent.ksql.schema.ksql.types.SqlStruct.Builder;
import io.confluent.ksql.schema.ksql.types.SqlStruct.Field;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.util.BytesUtils;
import io.confluent.ksql.util.BytesUtils.Encoding;
import io.confluent.ksql.util.DecimalUtil;
import io.confluent.ksql.util.KsqlException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import org.apache.kafka.connect.data.Struct;

@SuppressWarnings("UnstableApiUsage")
public enum DefaultSqlValueCoercer implements SqlValueCoercer {

  /**
   * Disallows conversion from {@code STRING} to {@code BOOLEAN} and numeric types.
   *
   * <p/>Generally used for everything not related to user entered literals.
   */
  STRICT,

  /**
   * Allows conversion between {@code STRING} and {@code BOOLEAN} as well as {@code STRING} and
   * numeric types.
   *
   * <p/>Generally used for things related to user entered literals to allow for easier user input.
   */
  LAX;

  private ImmutableMap<SupportedCoercion, Coercer> rules;

  @Override
  public Result coerce(final Object value, final SqlType to) {
    return doCoerce(value, to);
  }

  /**
   * Test is a SQL type can be coerced to another.
   *
   * <p>Just because two types can be coerced does not mean all attempts to coerce a value of
   * one type to the other will succeed. For example, coercing a {@code BIGINT} to a {@code INTEGER}
   * will only succeed if the value fits.
   *
   * @param from the type of the values being coerced.
   * @param to the type the values need coercing to.
   * @return type capable of holding both sides, or {@link Result#failure()} if not compatible.
   */
  public Optional<SqlType> canCoerce(final SqlType from, final SqlType to) {
    return getCoercerFor(from.baseType(), to.baseType())
        .flatMap(coercer -> coercer.canCoerce(this, from, to));
  }

  private Result doCoerce(final Object value, final SqlType to) {
    if (value == null) {
      // NULL can be cast to any type:
      return Result.nullResult();
    }

    final SqlBaseType from = SchemaConverters.javaToSqlConverter()
        .toSqlType(value.getClass());

    return getCoercerFor(from, to.baseType())
        .map(c -> c.coerce(this, value, to))
        .orElseGet(Result::failure);
  }

  private Optional<Coercer> getCoercerFor(final SqlBaseType from, final SqlBaseType to) {
    if (rules == null) {
      rules = Rules.buildRules(this == LAX);
    }

    return Optional.ofNullable(rules.get(key(from, to)));
  }

  private static Optional<SqlType> coerceToDecimalCheck(
      final DefaultSqlValueCoercer coercer,
      final SqlType from,
      final SqlType to
  ) {
    if (to.baseType() == DOUBLE) {
      return Optional.of(to);
    }
    return Optional.of(DecimalUtil.widen(from, to));
  }

  private static Result coerceToDecimal(final Object value, final SqlType sqlType) {
    try {
      final SqlDecimal sqlDecimal = (SqlDecimal) sqlType;
      return Result.of(DecimalUtil.ensureFit(new BigDecimal(String.valueOf(value)), sqlDecimal));
    } catch (final KsqlException | NumberFormatException | ArithmeticException e) {
      return Result.failure();
    }
  }

  private static Result anyToString(final Object value) {
    return Result.of(value.toString());
  }

  private Optional<SqlType> canCoerceToArray(final SqlType from, final SqlType to) {
    final SqlArray fromArray = (SqlArray) from;
    final SqlArray toArray = (SqlArray) to;

    return canCoerce(fromArray.getItemType(), toArray.getItemType())
        .map(SqlTypes::array);
  }

  private Result coerceToArray(final Object value, final SqlType to) {
    if (!(value instanceof List<?>)) {
      return Result.failure();
    }

    final SqlType itemType = ((SqlArray) to).getItemType();

    final List<?> list = (List<?>) value;
    final List<Object> coerced = new ArrayList<>(list.size());
    for (final Object el : list) {
      final Result result = doCoerce(el, itemType);
      if (result.failed()) {
        return Result.failure();
      }

      coerced.add(result.value().orElse(null));
    }

    return Result.of(coerced);
  }

  private Optional<SqlType> canCoerceToMap(final SqlType from, final SqlType to) {
    final SqlMap fromMap = (SqlMap) from;
    final SqlMap toMap = (SqlMap) to;

    return canCoerce(fromMap.getKeyType(), toMap.getKeyType())
        .flatMap(keyType -> canCoerce(fromMap.getValueType(), toMap.getValueType())
            .map(valType -> SqlTypes.map(keyType, valType))
        );
  }

  private Result coerceToMap(final Object value, final SqlType to) {
    if (!(value instanceof Map<?, ?>)) {
      return Result.failure();
    }

    final SqlType keyType = ((SqlMap) to).getKeyType();
    final SqlType valType = ((SqlMap) to).getValueType();

    final Map<?, ?> map = (Map<?, ?>) value;
    final Map<Object, Object> coerced = new HashMap<>();
    for (final Map.Entry<?, ?> entry : map.entrySet()) {
      final Result coercedKey = doCoerce(entry.getKey(), keyType);
      final Result coercedValue = doCoerce(entry.getValue(), valType);
      if (coercedKey.failed() || coercedValue.failed()) {
        return Result.failure();
      }

      coerced.put(coercedKey.value().orElse(null), coercedValue.value().orElse(null));
    }

    return Result.of(coerced);
  }

  private Optional<SqlType> canCoerceToStruct(final SqlType from, final SqlType to) {
    final SqlStruct fromStruct = (SqlStruct) from;
    final SqlStruct toStruct = (SqlStruct) to;

    final List<String> fieldNames = Streams
        .concat(fromStruct.fields().stream(), toStruct.fields().stream())
        .map(Field::name)
        .distinct()
        .collect(Collectors.toList());

    final Builder builder = SqlTypes.struct();
    for (final String fieldName : fieldNames) {
      final Optional<Field> toField = toStruct.field(fieldName);
      final Optional<Field> fromField = fromStruct.field(fieldName);

      final SqlType fieldType;
      if (!fromField.isPresent()) {
        fieldType = toField.orElseThrow(IllegalStateException::new).type();
      } else if (!toField.isPresent()) {
        fieldType = fromField.orElseThrow(IllegalStateException::new).type();
      } else {
        final Optional<SqlType> common = canCoerce(fromField.get().type(), toField.get().type());
        if (!common.isPresent()) {
          // Field can not be coerced:
          return Optional.empty();
        }

        fieldType = common.get();
      }

      builder.field(fieldName, fieldType);
    }

    return Optional.of(builder.build());
  }

  private Result coerceToStruct(final Object value, final SqlType to) {
    if (!(value instanceof Struct)) {
      return Result.failure();
    }

    final Struct struct = (Struct) value;

    final SqlStruct fromStruct = (SqlStruct) SchemaConverters.connectToSqlConverter()
        .toSqlType(struct.schema());

    final SqlStruct toStruct = (SqlStruct) to;

    final Struct coerced =
        new Struct(SchemaConverters.sqlToConnectConverter().toConnectSchema(toStruct));

    for (final Field toField : toStruct.fields()) {

      final Result fieldValue = fromStruct.field(toField.name())
          .map(f -> struct.get(f.name()))
          .map(v -> doCoerce(v, toField.type()))
          .orElse(Result.nullResult());

      if (fieldValue.failed()) {
        return Result.failure();
      }

      fieldValue.value()
          .ifPresent(v -> coerced.put(toField.name(), v));
    }

    return Result.of(coerced);
  }

  private static boolean parseBoolean(final String v) {
    return SqlBooleans.parseBooleanExact(v.trim())
        .orElseThrow(IllegalArgumentException::new);
  }

  private static final class Rules {

    private static final ImmutableMap<SupportedCoercion, Coercer> STRICT_SUPPORTED =
        ImmutableMap.<SupportedCoercion, Coercer>builder()
            // BOOLEAN:
            .put(key(BOOLEAN, BOOLEAN), Coercer.PASS_THROUGH)
            // INTEGER:
            .put(key(INTEGER, INTEGER), Coercer.PASS_THROUGH)
            .put(key(INTEGER, BIGINT), coercer((c, v, t) -> Result.of(((Number) v).longValue())))
            .put(key(INTEGER, DECIMAL), decimalCoercer())
            .put(key(INTEGER, DOUBLE), coercer((c, v, t) -> Result.of(((Number) v).doubleValue())))
            // BIGINT:
            .put(key(BIGINT, BIGINT), Coercer.PASS_THROUGH)
            .put(key(BIGINT, DECIMAL), decimalCoercer())
            .put(key(BIGINT, DOUBLE), coercer((c, v, t) -> Result.of(((Number) v).doubleValue())))
            // DECIMAL:
            .put(key(DECIMAL, DECIMAL), decimalCoercer())
            .put(key(DECIMAL, DOUBLE), coercer((c, v, t) -> Result.of(((Number) v).doubleValue())))
            // DOUBLE:
            .put(key(DOUBLE, DOUBLE), Coercer.PASS_THROUGH)
            // STRING:
            .put(key(STRING, STRING), Coercer.PASS_THROUGH)
            .put(key(STRING, TIMESTAMP), parser((v, t) -> SqlTimeTypes.parseTimestamp(v)))
            .put(key(STRING, TIME), parser((v, t) -> SqlTimeTypes.parseTime(v)))
            .put(key(STRING, DATE), parser((v, t) -> SqlTimeTypes.parseDate(v)))
            .put(key(STRING, BYTES), parser((v, t) ->
                ByteBuffer.wrap(BytesUtils.decode(v, Encoding.BASE64))))
            // ARRAY:
            .put(key(ARRAY, ARRAY), coercer(
                DefaultSqlValueCoercer::canCoerceToArray,
                DefaultSqlValueCoercer::coerceToArray
            ))
            // MAP:
            .put(key(MAP, MAP), coercer(
                DefaultSqlValueCoercer::canCoerceToMap,
                DefaultSqlValueCoercer::coerceToMap
            ))
            // STRUCT:
            .put(key(STRUCT, STRUCT), coercer(
                DefaultSqlValueCoercer::canCoerceToStruct,
                DefaultSqlValueCoercer::coerceToStruct
            ))
            // TIME:
            .put(key(TIMESTAMP, TIMESTAMP), Coercer.PASS_THROUGH)
            .put(key(TIME, TIME), Coercer.PASS_THROUGH)
            .put(key(DATE, DATE), Coercer.PASS_THROUGH)
            // BYTES:
            .put(key(BYTES, BYTES), Coercer.PASS_THROUGH)
            .build();

    private static final ImmutableMap<SupportedCoercion, Coercer> LAX_ADDITIONAL =
        ImmutableMap.<SupportedCoercion, Coercer>builder()
            // BOOLEAN:
            .put(key(BOOLEAN, STRING), coercer((c, v, t) -> anyToString(v)))
            // BIGINT:
            .put(key(INTEGER, STRING), coercer((c, v, t) -> anyToString(v)))
            // BIGINT:
            .put(key(BIGINT, STRING), coercer((c, v, t) -> anyToString(v)))
            // DECIMAL:
            .put(key(DECIMAL, STRING),
                coercer((c, v, t) -> Result.of(((BigDecimal) v).toPlainString())))
            // DOUBLE:
            .put(key(DOUBLE, STRING), coercer((c, v, t) -> anyToString(v)))
            // STRING:
            .put(key(STRING, BOOLEAN), parser((v, t) -> DefaultSqlValueCoercer.parseBoolean(v)))
            .put(key(STRING, INTEGER), parser((v, t) -> Integer.parseInt(v)))
            .put(key(STRING, BIGINT), parser((v, t) -> Long.parseLong(v)))
            .put(key(STRING, DECIMAL), parser((v, t) -> DecimalUtil
                .ensureFit(new BigDecimal(v), (SqlDecimal) t)))
            .put(key(STRING, DOUBLE), parser((v, t) -> SqlDoubles.parseDouble(v)))
            // TIME:
            .put(key(TIMESTAMP, STRING), coercer((c, v, t)
                -> Result.of(SqlTimeTypes.formatTimestamp((Timestamp) v))))
            .put(key(TIME, STRING), coercer((c, v, t)
                -> Result.of(SqlTimeTypes.formatTime((Time) v))))
            .put(key(DATE, STRING), coercer((c, v, t)
                -> Result.of(SqlTimeTypes.formatDate((Date) v))))
            .build();

    private static Coercer parser(final BiFunction<String, SqlType, Object> parserFunction) {
      return coercer((c, v, t) -> {
        try {
          return Result.of(parserFunction.apply(((String) v).trim(), t));
        } catch (final Exception e) {
          return Result.failure();
        }
      });
    }

    static ImmutableMap<SupportedCoercion, Coercer> buildRules(final boolean lax) {
      return !lax
          ? STRICT_SUPPORTED
          : ImmutableMap.<SupportedCoercion, Coercer>builder()
              .putAll(STRICT_SUPPORTED)
              .putAll(LAX_ADDITIONAL)
              .build();
    }
  }

  private static SupportedCoercion key(final SqlBaseType from, final SqlBaseType to) {
    return new SupportedCoercion(from, to);
  }

  private static final class SupportedCoercion {

    private final SqlBaseType from;
    private final SqlBaseType to;

    SupportedCoercion(final SqlBaseType from, final SqlBaseType to) {
      this.from = requireNonNull(from, "from");
      this.to = requireNonNull(to, "to");
    }

    @Override
    public boolean equals(final Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      final SupportedCoercion that = (SupportedCoercion) o;
      return from == that.from && to == that.to;
    }

    @Override
    public int hashCode() {
      return Objects.hash(from, to);
    }
  }

  private interface CheckFunction {

    CheckFunction PASS_THROUGH = (coercer, from, to) -> Optional.of(to);

    Optional<SqlType> canCoerce(
        DefaultSqlValueCoercer coercer,
        SqlType from,
        SqlType to
    );
  }

  private interface CoerceFunction {

    CoerceFunction PASS_THROUGH = (coercer, value, to) -> Result.of(value);

    Result coerce(DefaultSqlValueCoercer coercer, Object value, SqlType to);
  }

  private static Coercer coercer(final CoerceFunction coerceFunction) {
    return new Coercer(CheckFunction.PASS_THROUGH, coerceFunction);
  }

  private static Coercer coercer(
      final CheckFunction checkFunction,
      final CoerceFunction coerceFunction
  ) {
    return new Coercer(checkFunction, coerceFunction);
  }

  private static Coercer decimalCoercer() {
    return coercer(
        DefaultSqlValueCoercer::coerceToDecimalCheck,
        (c, v, t) -> coerceToDecimal(v, t)
    );
  }

  private static final class Coercer {

    static final Coercer PASS_THROUGH = coercer(CoerceFunction.PASS_THROUGH);

    private final CheckFunction checkFunction;
    private final CoerceFunction coerceFunction;

    Coercer(final CheckFunction checkFunction, final CoerceFunction coerceFunction) {
      this.checkFunction = requireNonNull(checkFunction, "checkFunction");
      this.coerceFunction = requireNonNull(coerceFunction, "coerceFunction");
    }

    Optional<SqlType> canCoerce(
        final DefaultSqlValueCoercer coercer,
        final SqlType from,
        final SqlType to
    ) {
      return checkFunction.canCoerce(coercer, from, to);
    }

    Result coerce(
        final DefaultSqlValueCoercer coercer,
        final Object value,
        final SqlType to
    ) {
      return coerceFunction.coerce(coercer, value, to);
    }
  }
}
