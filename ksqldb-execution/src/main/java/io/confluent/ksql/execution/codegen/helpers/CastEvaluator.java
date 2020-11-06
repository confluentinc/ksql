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

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.function.KsqlFunctionException;
import io.confluent.ksql.schema.ksql.types.SqlBaseType;
import io.confluent.ksql.schema.ksql.types.SqlDecimal;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.schema.utils.FormatOptions;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.Pair;

/**
 * Used in the code generation to evaluate SQL {@code CAST} expressions.
 *
 * @see io.confluent.ksql.execution.expression.tree.Cast
 */
public final class CastEvaluator {

  private static final ImmutableMap<SqlBaseType, CastEvaluator.CastFunction> CASTERS =
      ImmutableMap.<SqlBaseType, CastEvaluator.CastFunction>builder()
          .put(SqlBaseType.STRING, CastEvaluator::castString)
          .put(SqlBaseType.BOOLEAN, CastEvaluator::castBoolean)
          .put(SqlBaseType.INTEGER, CastEvaluator::castInteger)
          .put(SqlBaseType.BIGINT, CastEvaluator::castLong)
          .put(SqlBaseType.DOUBLE, CastEvaluator::castDouble)
          .put(SqlBaseType.DECIMAL, CastEvaluator::castDecimal)
          .build();

  private CastEvaluator() {
  }

  public static Pair<String, SqlType> eval(
      final Pair<String, SqlType> expr,
      final SqlType sqlType,
      final KsqlConfig ksqlConfig
  ) {
    final SqlType sourceType = expr.getRight();
    if (sourceType == null || sqlType.equals(sourceType)) {
      // sourceType is null if source is SQL NULL
      return new Pair<>(expr.getLeft(), sqlType);
    }

    return CASTERS.getOrDefault(sqlType.baseType(), CastEvaluator::unsupportedCast)
        .cast(expr, sqlType, ksqlConfig);
  }

  private static Pair<String, SqlType> unsupportedCast(
      final Pair<String, SqlType> expr, final SqlType returnType, final KsqlConfig ksqlConfig
  ) {
    throw new KsqlFunctionException("Cast of " + expr.getRight()
        + " to " + returnType + " is not supported");
  }

  private static Pair<String, SqlType> castString(
      final Pair<String, SqlType> expr, final SqlType returnType, final KsqlConfig ksqlConfig
  ) {
    final SqlType schema = expr.getRight();
    final String exprStr;
    if (schema.baseType() == SqlBaseType.DECIMAL) {
      exprStr = expr.getLeft() + ".toPlainString()";
    } else {
      if (ksqlConfig.getBoolean(KsqlConfig.KSQL_STRING_CASE_CONFIG_TOGGLE)) {
        exprStr = "Objects.toString(" + expr.getLeft() + ", null)";
      } else {
        exprStr = "String.valueOf(" + expr.getLeft() + ")";
      }
    }
    return new Pair<>(exprStr, returnType);
  }

  private static Pair<String, SqlType> castBoolean(
      final Pair<String, SqlType> expr, final SqlType returnType, final KsqlConfig ksqlConfig
  ) {
    return new Pair<>(getCastToBooleanString(expr.getRight(), expr.getLeft()), returnType);
  }

  private static Pair<String, SqlType> castInteger(
      final Pair<String, SqlType> expr, final SqlType returnType, final KsqlConfig ksqlConfig
  ) {
    final String exprStr = getCastString(
        expr.getRight(),
        expr.getLeft(),
        "intValue()",
        "Integer.parseInt"
    );
    return new Pair<>(exprStr, returnType);
  }

  private static Pair<String, SqlType> castLong(
      final Pair<String, SqlType> expr, final SqlType returnType, final KsqlConfig ksqlConfig
  ) {
    final String exprStr = getCastString(
        expr.getRight(),
        expr.getLeft(),
        "longValue()",
        "Long.parseLong"
    );
    return new Pair<>(exprStr, returnType);
  }

  private static Pair<String, SqlType> castDouble(
      final Pair<String, SqlType> expr, final SqlType returnType, final KsqlConfig ksqlConfig
  ) {
    final String exprStr = getCastString(
        expr.getRight(),
        expr.getLeft(),
        "doubleValue()",
        "Double.parseDouble"
    );
    return new Pair<>(exprStr, returnType);
  }

  private static Pair<String, SqlType> castDecimal(
      final Pair<String, SqlType> expr, final SqlType returnType, final KsqlConfig ksqlConfig
  ) {
    if (!(returnType instanceof SqlDecimal)) {
      throw new KsqlException("Expected decimal type: " + returnType);
    }

    final SqlDecimal sqlDecimal = (SqlDecimal) returnType;

    if (expr.getRight().baseType() == SqlBaseType.DECIMAL && expr.right.equals(sqlDecimal)) {
      return expr;
    }

    return new Pair<>(
        getDecimalCastString(expr.getRight(), expr.getLeft(), sqlDecimal),
        returnType
    );
  }

  private static String getCastToBooleanString(final SqlType schema, final String exprStr) {
    if (schema.baseType() == SqlBaseType.STRING) {
      return "Boolean.parseBoolean(" + exprStr + ")";
    } else {
      throw new KsqlFunctionException(
          "Invalid cast operation: Cannot cast " + exprStr + " to boolean.");
    }
  }

  private static String getCastString(
      final SqlType schema,
      final String exprStr,
      final String javaTypeMethod,
      final String javaStringParserMethod
  ) {
    if (schema.baseType() == SqlBaseType.DECIMAL) {
      return "((" + exprStr + ")." + javaTypeMethod + ")";
    }

    switch (schema.baseType()) {
      case INTEGER:
        return "(new Integer(" + exprStr + ")." + javaTypeMethod + ")";
      case BIGINT:
        return "(new Long(" + exprStr + ")." + javaTypeMethod + ")";
      case DOUBLE:
        return "(new Double(" + exprStr + ")." + javaTypeMethod + ")";
      case STRING:
        return javaStringParserMethod + "(" + exprStr + ")";
      default:
        throw new KsqlFunctionException(
            "Invalid cast operation: Cannot cast "
                + exprStr + " to " + schema.toString(FormatOptions.noEscape()) + "."
        );
    }
  }

  private static String getDecimalCastString(
      final SqlType schema,
      final String exprStr,
      final SqlDecimal target
  ) {
    switch (schema.baseType()) {
      case INTEGER:
      case BIGINT:
      case DOUBLE:
      case STRING:
      case DECIMAL:
        return String.format(
            "(DecimalUtil.cast(%s, %d, %d))",
            exprStr,
            target.getPrecision(),
            target.getScale()
        );
      default:
        throw new KsqlFunctionException(
            "Invalid cast operation: Cannot cast " + exprStr + " to " + schema);
    }
  }

  @FunctionalInterface
  private interface CastFunction {

    Pair<String, SqlType> cast(
        Pair<String, SqlType> expr,
        SqlType returnType,
        KsqlConfig ksqlConfig
    );
  }
}
