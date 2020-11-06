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

import static java.util.Objects.requireNonNull;

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.function.KsqlFunctionException;
import io.confluent.ksql.schema.ksql.types.SqlBaseType;
import io.confluent.ksql.schema.ksql.types.SqlDecimal;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.Pair;

/**
 * Used in the code generation to evaluate SQL {@code CAST} expressions.
 *
 * @see io.confluent.ksql.execution.expression.tree.Cast
 */
public final class CastEvaluator {

  /**
   * Map of source SQL type to the supported target types and their handler.
   */
  private static final ImmutableMap<SqlBaseType, SupportedCasts> SUPPORTED_CASTS = ImmutableMap
      .<SqlBaseType, SupportedCasts>builder()
      .put(SqlBaseType.BOOLEAN, SupportedCasts.builder()
          .put(SqlBaseType.STRING, CastEvaluator::castToString)
          .build())
      .put(SqlBaseType.INTEGER, SupportedCasts.builder()
          .put(SqlBaseType.BIGINT, formattedCode("(new Integer(%s).longValue())"))
          .put(SqlBaseType.DECIMAL, CastEvaluator::castToDecimal)
          .put(SqlBaseType.DOUBLE, formattedCode("(new Integer(%s).doubleValue())"))
          .put(SqlBaseType.STRING, CastEvaluator::castToString)
          .build())
      .put(SqlBaseType.BIGINT, SupportedCasts.builder()
          .put(SqlBaseType.INTEGER, formattedCode("(new Long(%s).intValue())"))
          .put(SqlBaseType.DECIMAL, CastEvaluator::castToDecimal)
          .put(SqlBaseType.DOUBLE, formattedCode("(new Long(%s).doubleValue())"))
          .put(SqlBaseType.STRING, CastEvaluator::castToString)
          .build())
      .put(SqlBaseType.DECIMAL, SupportedCasts.builder()
          .put(SqlBaseType.INTEGER, formattedCode("((%s).intValue())"))
          .put(SqlBaseType.BIGINT, formattedCode("((%s).longValue())"))
          .put(SqlBaseType.DOUBLE, formattedCode("((%s).doubleValue())"))
          .put(SqlBaseType.STRING, formattedCode("%s.toPlainString()"))
          .build())
      .put(SqlBaseType.DOUBLE, SupportedCasts.builder()
          .put(SqlBaseType.INTEGER, formattedCode("(new Double(%s).intValue())"))
          .put(SqlBaseType.BIGINT, formattedCode("(new Double(%s).longValue())"))
          .put(SqlBaseType.DECIMAL, CastEvaluator::castToDecimal)
          .put(SqlBaseType.STRING, CastEvaluator::castToString)
          .build())
      .put(SqlBaseType.STRING, SupportedCasts.builder()
          .put(SqlBaseType.BOOLEAN, formattedCode("Boolean.parseBoolean(%s)"))
          .put(SqlBaseType.INTEGER, formattedCode("Integer.parseInt(%s)"))
          .put(SqlBaseType.BIGINT, formattedCode("Long.parseLong(%s)"))
          .put(SqlBaseType.DECIMAL, CastEvaluator::castToDecimal)
          .put(SqlBaseType.DOUBLE, formattedCode("Double.parseDouble(%s)"))
          .build())
      .put(SqlBaseType.ARRAY, SupportedCasts.builder()
          .put(SqlBaseType.STRING, CastEvaluator::castToString)
          .build())
      .put(SqlBaseType.MAP, SupportedCasts.builder()
          .put(SqlBaseType.STRING, CastEvaluator::castToString)
          .build())
      .put(SqlBaseType.STRUCT, SupportedCasts.builder()
          .put(SqlBaseType.STRING, CastEvaluator::castToString)
          .build())
      .build();

  private static final SupportedCasts UNSUPPORTED = SupportedCasts.builder().build();

  private CastEvaluator() {
  }

  public static Pair<String, SqlType> eval(
      final Pair<String, SqlType> expr,
      final SqlType returnType,
      final KsqlConfig config
  ) {
    final SqlType sourceType = expr.getRight();
    if (sourceType == null || returnType.equals(sourceType)) {
      // sourceType is null if source is SQL NULL
      return new Pair<>(expr.getLeft(), returnType);
    }

    final String generated = SUPPORTED_CASTS
        .getOrDefault(sourceType.baseType(), UNSUPPORTED)
        .generate(expr.getLeft(), sourceType, returnType, config);

    return Pair.of(generated, returnType);
  }

  private static CastFunction formattedCode(final String code) {
    return (innerCode, returnType, config) -> String.format(code, innerCode);
  }

  private static String castToDecimal(
      final String innerCode,
      final SqlType returnType,
      final KsqlConfig config
  ) {
    final SqlDecimal decimal = (SqlDecimal) returnType;
    return "(DecimalUtil.cast("
        + innerCode + ", "
        + decimal.getPrecision() + ", "
        + decimal.getScale() + "))";
  }

  private static String castToString(
      final String innerCode,
      final SqlType returnType,
      final KsqlConfig config
  ) {
    return config.getBoolean(KsqlConfig.KSQL_STRING_CASE_CONFIG_TOGGLE)
        ? "Objects.toString(" + innerCode + ", null)"
        : "String.valueOf(" + innerCode + ")";
  }

  @FunctionalInterface
  private interface CastFunction {

    String cast(
        String innerCode,
        SqlType returnType,
        KsqlConfig config
    );
  }

  private static final class SupportedCasts {

    private final ImmutableMap<SqlBaseType, CastFunction> casts;

    SupportedCasts(final ImmutableMap<SqlBaseType, CastFunction> casts) {
      this.casts = requireNonNull(casts, "casts");
    }

    public static Builder builder() {
      return new Builder();
    }

    public String generate(
        final String innerCode,
        final SqlType sourceType,
        final SqlType returnType,
        final KsqlConfig config
    ) {
      final CastFunction castFunction = casts.get(returnType.baseType());
      if (castFunction == null) {
        throw new KsqlFunctionException("Cast of " + sourceType + " to " + returnType
            + " is not supported");
      }

      return castFunction.cast(innerCode, returnType, config);
    }

    static final class Builder {

      private final ImmutableMap.Builder<SqlBaseType, CastFunction> casts = ImmutableMap.builder();

      Builder put(final SqlBaseType type, final CastFunction function) {
        casts.put(type, function);
        return this;
      }

      SupportedCasts build() {
        return new SupportedCasts(casts.build());
      }
    }
  }
}
