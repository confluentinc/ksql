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
import io.confluent.ksql.execution.interpreter.CastInterpreter.NumberConversions;
import io.confluent.ksql.execution.interpreter.terms.BasicTerms.BooleanTerm;
import io.confluent.ksql.execution.interpreter.terms.ComparisonTerm.CompareToTerm;
import io.confluent.ksql.execution.interpreter.terms.ComparisonTerm.ComparisonCheckFunction;
import io.confluent.ksql.execution.interpreter.terms.ComparisonTerm.ComparisonFunction;
import io.confluent.ksql.execution.interpreter.terms.ComparisonTerm.ComparisonNullCheckFunction;
import io.confluent.ksql.execution.interpreter.terms.ComparisonTerm.EqualsCheckFunction;
import io.confluent.ksql.execution.interpreter.terms.ComparisonTerm.EqualsFunction;
import io.confluent.ksql.execution.interpreter.terms.ComparisonTerm.EqualsTerm;
import io.confluent.ksql.execution.interpreter.terms.Term;
import io.confluent.ksql.schema.ksql.SqlTimestamps;
import io.confluent.ksql.schema.ksql.types.SqlBaseType;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.util.KsqlException;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Optional;

/**
 * Contains lots of the logic for doing comparisons in the interpreter.
 */
public final class ComparisonInterpreter {

  private ComparisonInterpreter() { }

  /**
   * Does a comparison between the two terms
   * @param type The type of the comparison
   * @param left Left term
   * @param right Right term
   * @return The term representing the result of the comparison
   */
  public static BooleanTerm doComparison(
      final ComparisonExpression.Type type,
      final Term left,
      final Term right
  ) {
    final ComparisonNullCheckFunction nullCheckFunction = getNullCheckFunction(type);

    final Optional<ComparisonFunction> compareTo = doCompareTo(left, right);
    if (compareTo.isPresent()) {
      return new CompareToTerm(left, right, nullCheckFunction, compareTo.get(),
          getComparisonCheckFunction(type, left, right));
    }

    final Optional<EqualsFunction> equals = doEquals(left, right);
    if (equals.isPresent()) {
      return new EqualsTerm(left, right, nullCheckFunction, equals.get(),
          getEqualsCheckFunction(type, left, right));
    }

    throw new KsqlException("Unsupported comparison between " + left.getSqlType() + " and "
        + right.getSqlType());
  }

  private static BigDecimal toDecimal(final Object object, final SqlType from) {
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

  private static Timestamp toTimestamp(final Object object, final SqlType from) {
    if (object instanceof Timestamp) {
      return (Timestamp) object;
    } else if (object instanceof String) {
      return SqlTimestamps.parseTimestamp((String) object);
    } else {
      throw new KsqlException(String.format("Unsupported comparison between %s and %s", from,
          SqlTypes.TIMESTAMP));
    }
  }

  private static ComparisonNullCheckFunction getNullCheckFunction(
      final ComparisonExpression.Type type
  ) {
    if (type == ComparisonExpression.Type.IS_DISTINCT_FROM) {
      return (c, l, r) -> {
        final Object leftObject = l.getValue(c);
        final Object rightObject = r.getValue(c);
        if (leftObject == null || rightObject == null) {
          return Optional.of((leftObject == null) ^ (rightObject == null));
        }
        return Optional.empty();
      };
    }
    return (c, l, r) -> {
      if (l.getValue(c) == null || r.getValue(c) == null) {
        return Optional.of(false);
      }
      return Optional.empty();
    };
  }

  private static Optional<ComparisonFunction> doCompareTo(final Term left, final Term right) {
    final SqlBaseType leftType = left.getSqlType().baseType();
    final SqlBaseType rightType = right.getSqlType().baseType();
    if (either(leftType, rightType, SqlBaseType.DECIMAL)) {
      return Optional.of((c, l, r) -> doCompareTo(c, ComparisonInterpreter::toDecimal, l, r));
    } else if (either(leftType, rightType, SqlBaseType.TIMESTAMP)) {
      return Optional.of((c, l, r) -> doCompareTo(c, ComparisonInterpreter::toTimestamp, l, r));
    } else if (leftType == SqlBaseType.STRING) {
      return Optional.of((c, l, r) -> l.getValue(c).toString().compareTo(r.getValue(c).toString()));
    } else if (either(leftType, rightType, SqlBaseType.DOUBLE)) {
      return Optional.of((c, l, r) -> doCompareTo(c, NumberConversions::toDouble, l, r));
    } else if (either(leftType, rightType, SqlBaseType.BIGINT)) {
      return Optional.of((c, l, r) -> doCompareTo(c, NumberConversions::toLong, l, r));
    } else if (either(leftType, rightType, SqlBaseType.INTEGER)) {
      return Optional.of((c, l, r) -> doCompareTo(c, NumberConversions::toInteger, l, r));
    }
    return Optional.empty();
  }

  private static <T extends Comparable<T>> int doCompareTo(
      final TermEvaluationContext context,
      final Conversion<T> conversion,
      final Term left,
      final Term right) {
    final Object leftObject = left.getValue(context);
    final Object rightObject = right.getValue(context);
    return conversion.convert(leftObject, left.getSqlType()).compareTo(
        conversion.convert(rightObject, right.getSqlType()));
  }

  private static boolean either(
      final SqlBaseType leftType,
      final SqlBaseType rightType,
      final SqlBaseType value) {
    return leftType == value || rightType == value;
  }

  private static Optional<EqualsFunction> doEquals(final Term left, final Term right) {
    final SqlBaseType leftType = left.getSqlType().baseType();
    switch (leftType) {
      case ARRAY:
      case MAP:
      case STRUCT:
      case BOOLEAN:
        return Optional.of((c, l, r) -> l.getValue(c).equals(r.getValue(c)));
      default:
        return Optional.empty();
    }
  }

  private static ComparisonCheckFunction getComparisonCheckFunction(
      final ComparisonExpression.Type type,
      final Term left,
      final Term right
  ) {
    switch (type) {
      case EQUAL:
        return compareTo -> compareTo == 0;
      case NOT_EQUAL:
      case IS_DISTINCT_FROM:
        return compareTo -> compareTo != 0;
      case GREATER_THAN_OR_EQUAL:
        return compareTo -> compareTo >= 0;
      case GREATER_THAN:
        return compareTo -> compareTo > 0;
      case LESS_THAN_OR_EQUAL:
        return compareTo -> compareTo <= 0;
      case LESS_THAN:
        return compareTo -> compareTo < 0;
      default:
        throw new KsqlException(String.format("Unsupported comparison between %s and %s: %s",
            left.getSqlType(), right.getSqlType(), type));
    }
  }

  private static EqualsCheckFunction getEqualsCheckFunction(
      final ComparisonExpression.Type type,
      final Term left,
      final Term right
  ) {
    switch (type) {
      case EQUAL:
        return equals -> equals;
      case NOT_EQUAL:
      case IS_DISTINCT_FROM:
        return equals -> !equals;
      default:
        throw new KsqlException(String.format("Unsupported comparison between %s and %s: %s",
            left.getSqlType(), right.getSqlType(), type));
    }
  }

  private interface Conversion<T extends Comparable> {
    T convert(Object object, SqlType from);
  }
}
