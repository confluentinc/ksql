/*
 * Copyright 2021 Confluent Inc.
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

package io.confluent.ksql.execution.interpreter;

import io.confluent.ksql.execution.expression.tree.ComparisonExpression;
import io.confluent.ksql.execution.interpreter.CastInterpreter.ConversionType;
import io.confluent.ksql.execution.interpreter.CastInterpreter.NumberConversions;
import io.confluent.ksql.schema.ksql.SqlTimestamps;
import io.confluent.ksql.schema.ksql.types.SqlBaseType;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.Pair;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Optional;

/**
 * Contains lots of the logic for doing comparisons in the interpreter.
 */
public final class ComparisonInterpreter {

  private ComparisonInterpreter() { }

  /**
   * After a comparison has been done between two expressions of comparable types, this
   * evaluates the result according to the requested operation.
   * @param type The comparison type
   * @param compareTo The result of calling left.compareTo(right)
   * @return The evaluated result
   */
  public static boolean doComparisonCheck(
      final ComparisonExpression.Type type,
      final SqlType leftType,
      final SqlType rightType,
      final int compareTo
  ) {
    switch (type) {
      case EQUAL:
        return compareTo == 0;
      case NOT_EQUAL:
      case IS_DISTINCT_FROM:
        return compareTo != 0;
      case GREATER_THAN_OR_EQUAL:
        return compareTo >= 0;
      case GREATER_THAN:
        return compareTo > 0;
      case LESS_THAN_OR_EQUAL:
        return compareTo <= 0;
      case LESS_THAN:
        return compareTo < 0;
      default:
        throw new KsqlException(String.format("Unsupported comparison between %s and %s: %s",
            leftType, rightType, type));
    }
  }

  public static boolean doEqualsCheck(
      final ComparisonExpression.Type type,
      final SqlType leftType,
      final SqlType rightType,
      final boolean equals
  ) {
    switch (type) {
      case EQUAL:
        return equals;
      case NOT_EQUAL:
      case IS_DISTINCT_FROM:
        return !equals;
      default:
        throw new KsqlException(String.format("Unsupported comparison between %s and %s: %s",
            leftType, rightType, type));
    }
  }

  public static BigDecimal toDecimal(final Object object, final SqlType from,
      final ConversionType type) {
    if (object instanceof BigDecimal) {
      return (BigDecimal) object;
    } else if (object instanceof Double) {
      return BigDecimal.valueOf((Double) object);
    } else if (object instanceof Integer) {
      return new BigDecimal((Integer) object);
    } else if (object instanceof Long) {
      return new BigDecimal((Long) object);
    } else if (object instanceof String) {
      return new BigDecimal((String) object);
    } else {
      throw new KsqlException(String.format("Unsupported comparison between %s and %s", from,
          SqlBaseType.DECIMAL));
    }
  }

  public static Timestamp toTimestamp(final Object object, final SqlType from,
      final ConversionType type) {
    if (object instanceof Timestamp) {
      return (Timestamp) object;
    } else if (object instanceof String) {
      return SqlTimestamps.parseTimestamp((String) object);
    } else {
      throw new KsqlException(String.format("Unsupported comparison between %s and %s", from,
          SqlTypes.TIMESTAMP));
    }
  }

  public static Optional<Integer> doCompareTo(
      final Pair<Object, SqlType> left,
      final Pair<Object, SqlType> right) {
    final SqlBaseType leftType = left.getRight().baseType();
    final SqlBaseType rightType = right.getRight().baseType();
    final Object leftObject = left.getLeft();
    final Object rightObject = right.getLeft();
    if (either(leftType, rightType, SqlBaseType.DECIMAL)) {
      return doCompareTo(ComparisonInterpreter::toDecimal, left, right);
    } else if (either(leftType, rightType, SqlBaseType.TIMESTAMP)) {
      return doCompareTo(ComparisonInterpreter::toTimestamp, left, right);
    } else if (leftType == SqlBaseType.STRING) {
      return Optional.of(leftObject.toString().compareTo(rightObject.toString()));
    } else if (either(leftType, rightType, SqlBaseType.DOUBLE)) {
      return doCompareTo(NumberConversions::toDouble, left, right);
    } else if (either(leftType, rightType, SqlBaseType.BIGINT)) {
      return doCompareTo(NumberConversions::toLong, left, right);
    } else if (either(leftType, rightType, SqlBaseType.INTEGER)) {
      return doCompareTo(NumberConversions::toInteger, left, right);
    }
    return Optional.empty();
  }

  private static <T extends Comparable<T>> Optional<Integer> doCompareTo(
      final Conversion<T> conversion,
      final Pair<Object, SqlType> left,
      final Pair<Object, SqlType> right) {
    final Object leftObject = left.getLeft();
    final Object rightObject = right.getLeft();
    return Optional.of(
        conversion.convert(leftObject, left.getRight(), ConversionType.COMPARISON).compareTo(
            conversion.convert(rightObject, right.getRight(), ConversionType.COMPARISON)));
  }

  private static boolean either(
      final SqlBaseType leftType,
      final SqlBaseType rightType,
      final SqlBaseType value) {
    return leftType == value || rightType == value;
  }

  public static Optional<Boolean> doEquals(
      final Pair<Object, SqlType> left,
      final Pair<Object, SqlType> right
  ) {
    final SqlBaseType leftType = left.getRight().baseType();
    final Object leftObject = left.getLeft();
    final Object rightObject = right.getLeft();

    switch (leftType) {
      case ARRAY:
      case MAP:
      case STRUCT:
      case BOOLEAN:
        return Optional.of(leftObject.equals(rightObject));
      default:
        return Optional.empty();
    }
  }

  public interface Conversion<T extends Comparable> {
    T convert(Object object, SqlType from, ConversionType type);
  }
}
