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

import static io.confluent.ksql.execution.interpreter.CastInterpreter.castToBigDecimalFunction;
import static io.confluent.ksql.execution.interpreter.CastInterpreter.castToDoubleFunction;
import static io.confluent.ksql.execution.interpreter.CastInterpreter.castToIntegerFunction;
import static io.confluent.ksql.execution.interpreter.CastInterpreter.castToLongFunction;
import static io.confluent.ksql.execution.interpreter.CastInterpreter.castToTimestampFunction;

import io.confluent.ksql.execution.expression.tree.ComparisonExpression;
import io.confluent.ksql.execution.interpreter.terms.CastTerm.ComparableCastFunction;
import io.confluent.ksql.execution.interpreter.terms.ComparisonTerm.CompareToTerm;
import io.confluent.ksql.execution.interpreter.terms.ComparisonTerm.ComparisonCheckFunction;
import io.confluent.ksql.execution.interpreter.terms.ComparisonTerm.ComparisonFunction;
import io.confluent.ksql.execution.interpreter.terms.ComparisonTerm.ComparisonNullCheckFunction;
import io.confluent.ksql.execution.interpreter.terms.ComparisonTerm.EqualsCheckFunction;
import io.confluent.ksql.execution.interpreter.terms.ComparisonTerm.EqualsFunction;
import io.confluent.ksql.execution.interpreter.terms.ComparisonTerm.EqualsTerm;
import io.confluent.ksql.execution.interpreter.terms.Term;
import io.confluent.ksql.schema.ksql.types.SqlBaseType;
import io.confluent.ksql.util.KsqlException;
import java.math.BigDecimal;
import java.util.Date;
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
  public static Term doComparison(
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

  private static ComparisonNullCheckFunction getNullCheckFunction(
      final ComparisonExpression.Type type
  ) {
    if (type == ComparisonExpression.Type.IS_DISTINCT_FROM) {
      return (leftObject, rightObject) -> {
        if (leftObject == null || rightObject == null) {
          return Optional.of((leftObject == null) ^ (rightObject == null));
        }
        return Optional.empty();
      };
    }
    return (leftObject, rightObject) -> {
      if (leftObject == null || rightObject == null) {
        return Optional.of(false);
      }
      return Optional.empty();
    };
  }

  private static Optional<ComparisonFunction> doCompareTo(final Term left, final Term right) {
    final SqlBaseType leftType = left.getSqlType().baseType();
    final SqlBaseType rightType = right.getSqlType().baseType();
    if (either(leftType, rightType, SqlBaseType.DECIMAL)) {
      final ComparableCastFunction<BigDecimal> castLeft = castToBigDecimalFunction(
          left.getSqlType());
      final ComparableCastFunction<BigDecimal> castRight = castToBigDecimalFunction(
          right.getSqlType());
      return Optional.of((o1, o2) -> castLeft.cast(o1).compareTo(castRight.cast(o2)));
    } else if (either(leftType, rightType, SqlBaseType.TIMESTAMP)) {
      final ComparableCastFunction<Date> castLeft = castToTimestampFunction(left.getSqlType());
      final ComparableCastFunction<Date> castRight = castToTimestampFunction(right.getSqlType());
      return Optional.of((o1, o2) -> castLeft.cast(o1).compareTo(castRight.cast(o2)));
    } else if (leftType == SqlBaseType.STRING) {
      return Optional.of((o1, o2) -> o1.toString().compareTo(o2.toString()));
    } else if (either(leftType, rightType, SqlBaseType.DOUBLE)) {
      final ComparableCastFunction<Double> castLeft = castToDoubleFunction(left.getSqlType());
      final ComparableCastFunction<Double> castRight = castToDoubleFunction(right.getSqlType());
      return Optional.of((o1, o2) -> castLeft.cast(o1).compareTo(castRight.cast(o2)));
    } else if (either(leftType, rightType, SqlBaseType.BIGINT)) {
      final ComparableCastFunction<Long> castLeft = castToLongFunction(left.getSqlType());
      final ComparableCastFunction<Long> castRight = castToLongFunction(right.getSqlType());
      return Optional.of((o1, o2) -> castLeft.cast(o1).compareTo(castRight.cast(o2)));
    } else if (either(leftType, rightType, SqlBaseType.INTEGER)) {
      final ComparableCastFunction<Integer> castLeft = castToIntegerFunction(left.getSqlType());
      final ComparableCastFunction<Integer> castRight = castToIntegerFunction(right.getSqlType());
      return Optional.of((o1, o2) -> castLeft.cast(o1).compareTo(castRight.cast(o2)));
    }
    return Optional.empty();
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
        return Optional.of(Object::equals);
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
}
