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

import static io.confluent.ksql.execution.interpreter.CastInterpreter.castToDoubleFunction;
import static io.confluent.ksql.execution.interpreter.CastInterpreter.castToIntegerFunction;
import static io.confluent.ksql.execution.interpreter.CastInterpreter.castToLongFunction;

import io.confluent.ksql.execution.expression.tree.ArithmeticUnaryExpression.Sign;
import io.confluent.ksql.execution.interpreter.terms.ArithmeticBinaryTerm;
import io.confluent.ksql.execution.interpreter.terms.ArithmeticBinaryTerm.ArithmeticBinaryFunction;
import io.confluent.ksql.execution.interpreter.terms.ArithmeticUnaryTerm;
import io.confluent.ksql.execution.interpreter.terms.ArithmeticUnaryTerm.ArithmeticUnaryFunction;
import io.confluent.ksql.execution.interpreter.terms.CastTerm;
import io.confluent.ksql.execution.interpreter.terms.CastTerm.ComparableCastFunction;
import io.confluent.ksql.execution.interpreter.terms.Term;
import io.confluent.ksql.schema.Operator;
import io.confluent.ksql.schema.ksql.types.SqlBaseType;
import io.confluent.ksql.schema.ksql.types.SqlDecimal;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.util.DecimalUtil;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;

public final class ArithmeticInterpreter {
  private ArithmeticInterpreter() { }

  /**
   * Creates a term representing unary arithmetic on the given input term.
   * @param sign The sign of the unary operation
   * @param value The input term
   * @return The resulting term
   */
  public static Term doUnaryArithmetic(final Sign sign, final Term value) {
    final ArithmeticUnaryFunction function;
    switch (sign) {
      case MINUS:
        function = getUnaryMinusFunction(value);
        break;
      case PLUS:
        function = getUnaryPlusFunction(value);
        break;
      default:
        throw new UnsupportedOperationException("Unsupported sign: " + sign);
    }
    return new ArithmeticUnaryTerm(value, function);
  }

  /**
   * Creates a term representing binary arithmetic on the given input term.
   * @param operator The operator in use
   * @param left The left term
   * @param right The right term
   * @param resultType The type of the resulting operation
   * @param ksqlConfig The ksqlconfig
   * @return
   */
  public static Term doBinaryArithmetic(
      final Operator operator,
      final Term left,
      final Term right,
      final SqlType resultType,
      final KsqlConfig ksqlConfig
  ) {
    if (resultType.baseType() == SqlBaseType.DECIMAL) {
      final SqlDecimal decimal = (SqlDecimal) resultType;
      final CastTerm leftTerm = CastInterpreter.cast(left, left.getSqlType(),
          DecimalUtil.toSqlDecimal(left.getSqlType()), ksqlConfig);
      final CastTerm rightTerm = CastInterpreter.cast(right, right.getSqlType(),
          DecimalUtil.toSqlDecimal(right.getSqlType()), ksqlConfig);
      final TypedArithmeticBinaryFunction<BigDecimal> fn = getDecimalFunction(decimal, operator);
      return new ArithmeticBinaryTerm(leftTerm, rightTerm,
          (o1, o2) -> fn.doFunction((BigDecimal) o1, (BigDecimal) o2),
          resultType);
    } else {
      final Term leftTerm =
          left.getSqlType().baseType() == SqlBaseType.DECIMAL
              ? CastInterpreter.cast(left, left.getSqlType(), SqlTypes.DOUBLE, ksqlConfig)
              : left;
      final Term rightTerm =
          right.getSqlType().baseType() == SqlBaseType.DECIMAL
              ? CastInterpreter.cast(right, right.getSqlType(), SqlTypes.DOUBLE, ksqlConfig)
              : right;
      return new ArithmeticBinaryTerm(leftTerm, rightTerm,
          getNonDecimalArithmeticFunction(operator, leftTerm.getSqlType(), rightTerm.getSqlType()),
          resultType);
    }
  }

  private static ArithmeticBinaryFunction getNonDecimalArithmeticFunction(
      final Operator operator,
      final SqlType leftType,
      final SqlType rightType) {
    final SqlBaseType leftBaseType = leftType.baseType();
    final SqlBaseType rightBaseType = rightType.baseType();
    if (leftBaseType == SqlBaseType.STRING && rightBaseType == SqlBaseType.STRING) {
      return (o1, o2) -> (String) o1 + (String) o2;
    } else if (leftBaseType == SqlBaseType.DOUBLE || rightBaseType == SqlBaseType.DOUBLE) {
      final TypedArithmeticBinaryFunction<Double> fn = getDoubleFunction(operator);
      final ComparableCastFunction<Double> castLeft = castToDoubleFunction(leftType);
      final ComparableCastFunction<Double> castRight = castToDoubleFunction(rightType);
      return (o1, o2) -> fn.doFunction(castLeft.cast(o1), castRight.cast(o2));
    } else if (leftBaseType == SqlBaseType.BIGINT || rightBaseType == SqlBaseType.BIGINT) {
      final TypedArithmeticBinaryFunction<Long> fn = getLongFunction(operator);
      final ComparableCastFunction<Long> castLeft = castToLongFunction(leftType);
      final ComparableCastFunction<Long> castRight = castToLongFunction(rightType);
      return (o1, o2) -> fn.doFunction(castLeft.cast(o1), castRight.cast(o2));
    } else if (leftBaseType == SqlBaseType.INTEGER || rightBaseType == SqlBaseType.INTEGER) {
      final TypedArithmeticBinaryFunction<Integer> fn = getIntegerFunction(operator);
      final ComparableCastFunction<Integer> castLeft = castToIntegerFunction(leftType);
      final ComparableCastFunction<Integer> castRight = castToIntegerFunction(rightType);
      return (o1, o2) -> fn.doFunction(castLeft.cast(o1), castRight.cast(o2));
    } else {
      throw new KsqlException("Can't do arithmetic for types " + leftType + " and " + rightType);
    }
  }

  private static ArithmeticUnaryFunction getUnaryMinusFunction(final Term term) {
    if (term.getSqlType().baseType() == SqlBaseType.DECIMAL) {
      return o -> ((BigDecimal) o).negate(
          new MathContext(((SqlDecimal) term.getSqlType()).getPrecision(),
              RoundingMode.UNNECESSARY));
    } else if (term.getSqlType().baseType() == SqlBaseType.DOUBLE) {
      return o -> -((Double) o);
    } else if (term.getSqlType().baseType() == SqlBaseType.INTEGER) {
      return o -> -((Integer) o);
    } else if (term.getSqlType().baseType() == SqlBaseType.BIGINT) {
      return o -> -((Long) o);
    } else {
      throw new UnsupportedOperationException("Negation on unsupported type: "
          + term.getSqlType());
    }
  }

  private static ArithmeticUnaryFunction getUnaryPlusFunction(final Term term) {
    if (term.getSqlType().baseType() == SqlBaseType.DECIMAL) {
      return o -> ((BigDecimal) o)
          .plus(new MathContext(((SqlDecimal) term.getSqlType()).getPrecision(),
              RoundingMode.UNNECESSARY));
    } else if (term.getSqlType().baseType() == SqlBaseType.DOUBLE
        || term.getSqlType().baseType() == SqlBaseType.INTEGER
        || term.getSqlType().baseType() == SqlBaseType.BIGINT) {
      // https://docs.oracle.com/javase/specs/jls/se8/html/jls-15.html#jls-15.15.3
      // For integer, long, and double types, type promotion has no effect
      return o -> o;
    } else {
      throw new UnsupportedOperationException("Unary plus on unsupported type: "
          + term.getSqlType());
    }
  }

  private static TypedArithmeticBinaryFunction<Double> getDoubleFunction(final Operator operator) {
    switch (operator) {
      case ADD:
        return (a, b) -> a + b;
      case SUBTRACT:
        return (a, b) -> a - b;
      case MULTIPLY:
        return (a, b) -> a * b;
      case DIVIDE:
        return (a, b) -> a / b;
      case MODULUS:
        return (a, b) -> a % b;
      default:
        throw new KsqlException("Unknown operator " + operator);
    }
  }

  private static TypedArithmeticBinaryFunction<Integer> getIntegerFunction(
      final Operator operator
  ) {
    switch (operator) {
      case ADD:
        return (a, b) -> a + b;
      case SUBTRACT:
        return (a, b) -> a - b;
      case MULTIPLY:
        return (a, b) -> a * b;
      case DIVIDE:
        return (a, b) -> a / b;
      case MODULUS:
        return (a, b) -> a % b;
      default:
        throw new KsqlException("Unknown operator " + operator);
    }
  }

  private static TypedArithmeticBinaryFunction<Long> getLongFunction(final Operator operator) {
    switch (operator) {
      case ADD:
        return (a, b) -> a + b;
      case SUBTRACT:
        return (a, b) -> a - b;
      case MULTIPLY:
        return (a, b) -> a * b;
      case DIVIDE:
        return (a, b) -> a / b;
      case MODULUS:
        return (a, b) -> a % b;
      default:
        throw new KsqlException("Unknown operator " + operator);
    }
  }

  private static TypedArithmeticBinaryFunction<BigDecimal> getDecimalFunction(
      final SqlDecimal decimal,
      final Operator operator
  ) {
    final MathContext mc = new MathContext(decimal.getPrecision(),
        RoundingMode.UNNECESSARY);
    switch (operator) {
      case ADD:
        return (a, b) -> a.add(b, mc).setScale(decimal.getScale());
      case SUBTRACT:
        return (a, b) -> a.subtract(b, mc).setScale(decimal.getScale());
      case MULTIPLY:
        return (a, b) -> a.multiply(b, mc).setScale(decimal.getScale());
      case DIVIDE:
        return (a, b) -> a.divide(b, mc).setScale(decimal.getScale());
      case MODULUS:
        return (a, b) -> a.remainder(b, mc).setScale(decimal.getScale());
      default:
        throw new KsqlException("DECIMAL operator not supported: " + operator);
    }
  }

  private interface TypedArithmeticBinaryFunction<T> {
    T doFunction(T o1, T o2);
  }
}
