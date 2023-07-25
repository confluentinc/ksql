/*
 * Copyright 2020 Confluent Inc.
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

package io.confluent.ksql.execution.codegen.helpers;

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
import io.confluent.ksql.schema.ksql.SchemaConverters;
import io.confluent.ksql.schema.ksql.types.SqlArray;
import io.confluent.ksql.schema.ksql.types.SqlBaseType;
import io.confluent.ksql.schema.ksql.types.SqlDecimal;
import io.confluent.ksql.schema.ksql.types.SqlMap;
import io.confluent.ksql.schema.ksql.types.SqlStruct;
import io.confluent.ksql.schema.ksql.types.SqlStruct.Field;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

/**
 * Used in the code generation to evaluate SQL {@code CAST} expressions.
 *
 * @see io.confluent.ksql.execution.expression.tree.Cast
 */
public final class CastEvaluator {

  /**
   * Map of source SQL type to the supported target types and their handler.
   */
  private static final ImmutableMap<SupportedCast, CastFunction> SUPPORTED = ImmutableMap
      .<SupportedCast, CastFunction>builder()
      // BOOLEAN:
      .put(key(BOOLEAN, STRING), CastEvaluator::castToString)
      // INT:
      .put(key(INTEGER, BIGINT), nonNullSafeCode("%s.longValue()"))
      .put(key(INTEGER, DECIMAL), CastEvaluator::castToDecimal)
      .put(key(INTEGER, DOUBLE), nonNullSafeCode("%s.doubleValue()"))
      .put(key(INTEGER, STRING), CastEvaluator::castToString)
      // BIGINT:
      .put(key(BIGINT, INTEGER), nonNullSafeCode("%s.intValue()"))
      .put(key(BIGINT, DECIMAL), CastEvaluator::castToDecimal)
      .put(key(BIGINT, DOUBLE), nonNullSafeCode("%s.doubleValue()"))
      .put(key(BIGINT, STRING), CastEvaluator::castToString)
      // DECIMAL:
      .put(key(DECIMAL, INTEGER), nonNullSafeCode("%s.intValue()"))
      .put(key(DECIMAL, BIGINT), nonNullSafeCode("%s.longValue()"))
      .put(key(DECIMAL, DECIMAL), CastEvaluator::castToDecimal)
      .put(key(DECIMAL, DOUBLE), nonNullSafeCode("%s.doubleValue()"))
      .put(key(DECIMAL, STRING), nonNullSafeCode("%s.toPlainString()"))
      // DOUBLE:
      .put(key(DOUBLE, INTEGER), nonNullSafeCode("%s.intValue()"))
      .put(key(DOUBLE, BIGINT), nonNullSafeCode("%s.longValue()"))
      .put(key(DOUBLE, DECIMAL), CastEvaluator::castToDecimal)
      .put(key(DOUBLE, STRING), CastEvaluator::castToString)
      // STRING:
      .put(key(STRING, BOOLEAN), nonNullSafeCode("SqlBooleans.parseBoolean(%s.trim())"))
      .put(key(STRING, INTEGER), nonNullSafeCode("Integer.parseInt(%s.trim())"))
      .put(key(STRING, BIGINT), nonNullSafeCode("Long.parseLong(%s.trim())"))
      .put(key(STRING, DECIMAL), CastEvaluator::castToDecimal)
      .put(key(STRING, DOUBLE), nonNullSafeCode("SqlDoubles.parseDouble(%s.trim())"))
      .put(key(STRING, TIMESTAMP), nonNullSafeCode("SqlTimeTypes.parseTimestamp(%s.trim())"))
      .put(key(STRING, TIME), nonNullSafeCode("SqlTimeTypes.parseTime(%s.trim())"))
      .put(key(STRING, DATE), nonNullSafeCode("SqlTimeTypes.parseDate(%s.trim())"))
      .put(key(STRING, BYTES) ,nonNullSafeCode(
          "ByteBuffer.wrap(BytesUtils.decode(%s.trim(), BytesUtils.Encoding.BASE64))"))
      // ARRAY:
      .put(key(ARRAY, ARRAY), CastEvaluator::castArrayToArray)
      .put(key(ARRAY, STRING), CastEvaluator::castToString)
      // MAP:
      .put(key(MAP, MAP), CastEvaluator::castMapToMap)
      .put(key(MAP, STRING), CastEvaluator::castToString)
      // STRUCT:
      .put(key(STRUCT, STRUCT), CastEvaluator::castStructToStruct)
      .put(key(STRUCT, STRING), CastEvaluator::castToString)
      // TIME:
      .put(key(TIMESTAMP, STRING), nonNullSafeCode("SqlTimeTypes.formatTimestamp(%s)"))
      .put(key(TIMESTAMP, TIME), nonNullSafeCode("SqlTimeTypes.timestampToTime(%s)"))
      .put(key(TIMESTAMP, DATE), nonNullSafeCode("SqlTimeTypes.timestampToDate(%s)"))
      .put(key(DATE, TIMESTAMP), nonNullSafeCode("new Timestamp(%s.getTime())"))
      .put(key(TIME, STRING), nonNullSafeCode("SqlTimeTypes.formatTime(%s)"))
      .put(key(DATE, STRING), nonNullSafeCode("SqlTimeTypes.formatDate(%s)"))
      .build();

  private CastEvaluator() {
  }

  /**
   * Generates code to handle cast.
   *
   * @param innerCode the code that needs casting.
   * @param from the type the {@code innerCode} compiles to.
   * @param to the type to cast to.
   * @param config the config to use.
   * @return the generated code
   */
  public static String generateCode(
      final String innerCode,
      final SqlType from,
      final SqlType to,
      final KsqlConfig config
  ) {
    if (from == null || to.equals(from)) {
      // from is null if source is SQL NULL
      return innerCode;
    }

    return Optional.ofNullable(SUPPORTED.get(key(from.baseType(), to.baseType())))
        .orElseThrow(() -> new UnsupportedCastException(from, to))
        .generateCode(innerCode, from, to, config);
  }

  /**
   * Called by the code generated by {@link #generateCode} to handle casting arrays.
   */
  public static <I, O> List<O> castArray(final List<I> array, final Function<I, O> mapper) {
    if (array == null) {
      return null;
    }

    final List<O> result = new ArrayList<>(array.size());
    array.forEach(e -> result.add(
        mapper.apply(e)
    ));
    return result;
  }

  /**
   * Called by the code generated by {@link #generateCode} to handle casting maps.
   */
  public static <K1, V1, K2, V2> Map<K2, V2> castMap(
      final Map<K1, V1> map,
      final Function<K1, K2> keyMapper,
      final Function<V1, V2> valueMapper
  ) {
    if (map == null) {
      return null;
    }

    final Map<K2, V2> result = new HashMap<>(map.size());
    map.forEach((k, v) -> result.put(
        keyMapper.apply(k),
        valueMapper.apply(v)
    ));
    return result;
  }

  /**
   * Called by the code generated by {@link #generateCode} to handle casting structs.
   */
  public static Struct castStruct(
      final Struct struct,
      final Map<String, Function<Object, Object>> fieldMappers,
      final Schema structSchema
  ) {
    if (struct == null) {
      return null;
    }

    final Struct result = new Struct(structSchema);

    fieldMappers
        .forEach((name, mapper) -> result.put(name, mapper.apply(struct.get(name))));

    return result;
  }

  private static CastFunction nonNullSafeCode(final String code) {
    return (innerCode, from, to, config) -> {
      final Class<?> fromJavaType = SchemaConverters.sqlToJavaConverter().toJavaType(from);
      final Class<?> toJavaType = SchemaConverters.sqlToJavaConverter().toJavaType(to);

      final String lambdaBody = String.format(code, "val");
      final String function = LambdaUtil.toJavaCode("val", fromJavaType, lambdaBody);
      return NullSafe.generateApply(innerCode, function, toJavaType);
    };
  }

  private static String castToDecimal(
      final String innerCode,
      final SqlType from,
      final SqlType to,
      final KsqlConfig config
  ) {
    final SqlDecimal decimal = (SqlDecimal) to;
    return "(DecimalUtil.cast("
        + innerCode + ", "
        + decimal.getPrecision() + ", "
        + decimal.getScale() + "))";
  }

  private static String castToString(
      final String innerCode,
      final SqlType from,
      final SqlType to,
      final KsqlConfig config
  ) {
    return config.getBoolean(KsqlConfig.KSQL_STRING_CASE_CONFIG_TOGGLE)
        ? "Objects.toString(" + innerCode + ", null)"
        : "String.valueOf(" + innerCode + ")";
  }

  private static String castArrayToArray(
      final String innerCode,
      final SqlType from,
      final SqlType to,
      final KsqlConfig config
  ) {
    final SqlArray fromArray = (SqlArray) from;
    final SqlArray toArray = (SqlArray) to;

    try {
      return "CastEvaluator.castArray("
          + innerCode + ", "
          + mapperFunction(fromArray.getItemType(), toArray.getItemType(), config)
          + ")";
    } catch (final UnsupportedCastException e) {
      throw new UnsupportedCastException(from, to, e);
    }
  }

  private static String castMapToMap(
      final String innerCode,
      final SqlType from,
      final SqlType to,
      final KsqlConfig config
  ) {
    final SqlMap fromMap = (SqlMap) from;
    final SqlMap toMap = (SqlMap) to;

    try {
      return "CastEvaluator.castMap("
          + innerCode + ", "
          + mapperFunction(fromMap.getKeyType(), toMap.getKeyType(), config) + ", "
          + mapperFunction(fromMap.getValueType(), toMap.getValueType(), config)
          + ")";
    } catch (final UnsupportedCastException e) {
      throw new UnsupportedCastException(from, to, e);
    }
  }

  @SuppressWarnings("OptionalGetWithoutIsPresent")
  private static String castStructToStruct(
      final String innerCode,
      final SqlType from,
      final SqlType to,
      final KsqlConfig config
  ) {
    final SqlStruct fromStruct = (SqlStruct) from;
    final SqlStruct toStruct = (SqlStruct) to;

    try {
      final String mappers = fromStruct.fields().stream()
          .filter(fromField -> toStruct.field(fromField.name()).isPresent())
          .map(fromField -> castFieldToField(
              fromField,
              toStruct.field(fromField.name()).get(),
              config
          ))
          .collect(Collectors.joining(
              System.lineSeparator(),
              "ImmutableMap.builder()\n\t\t",
              "\n\t\t.build()"
          ));

      // Inefficient, but only way to pass type until SqlToJavaVisitor supports passing
      // additional parameters to the generated code. Hopefully, JVM optimises this away.
      final String schemaCode =  "SchemaConverters.sqlToConnectConverter()"
          + ".toConnectSchema(" + SqlTypeCodeGen.generateCode(toStruct) + ")";

      return "CastEvaluator.castStruct(" + innerCode + ", " + mappers + "," + schemaCode + ")";
    } catch (final UnsupportedCastException e) {
      throw new UnsupportedCastException(from, to, e);
    }
  }

  private static String castFieldToField(
      final Field fromField,
      final Field toField,
      final KsqlConfig config
  ) {
    final String mapper = mapperFunction(fromField.type(), toField.type(), config);
    return ".put(\"" + fromField.name() + "\"," + mapper + ")";
  }

  private static String mapperFunction(
      final SqlType fromItemType,
      final SqlType toItemType,
      final KsqlConfig config
  ) {
    final Class<?> javaType = SchemaConverters.sqlToJavaConverter().toJavaType(fromItemType);

    final String lambdaBody = generateCode("val", fromItemType, toItemType, config);

    return LambdaUtil.toJavaCode("val", javaType, lambdaBody);
  }

  @FunctionalInterface
  private interface CastFunction {

    String generateCode(
        String innerCode,
        SqlType from,
        SqlType to,
        KsqlConfig config
    );
  }

  private static final class UnsupportedCastException extends KsqlException {

    UnsupportedCastException(
        final SqlType from,
        final SqlType to
    ) {
      super(buildMessage(from, to));
    }

    UnsupportedCastException(
        final SqlType from,
        final SqlType to,
        final UnsupportedCastException e
    ) {
      super(buildMessage(from, to), e);
    }

    private static String buildMessage(final SqlType from, final SqlType to) {
      return "Cast of " + from + " to " + to + " is not supported";
    }
  }

  private static SupportedCast key(final SqlBaseType from, final SqlBaseType to) {
    return new SupportedCast(from, to);
  }

  private static final class SupportedCast {

    private final SqlBaseType from;
    private final SqlBaseType to;

    SupportedCast(final SqlBaseType from, final SqlBaseType to) {
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
      final SupportedCast that = (SupportedCast) o;
      return from == that.from && to == that.to;
    }

    @Override
    public int hashCode() {
      return Objects.hash(from, to);
    }
  }
}
