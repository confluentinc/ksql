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

import static io.confluent.ksql.execution.interpreter.CastInterpreter.NumberConversions.toDouble;
import static io.confluent.ksql.execution.interpreter.CastInterpreter.NumberConversions.toInteger;
import static io.confluent.ksql.execution.interpreter.CastInterpreter.NumberConversions.toLong;

import io.confluent.ksql.execution.expression.tree.ArithmeticBinaryExpression;
import io.confluent.ksql.execution.interpreter.CastInterpreter.ConversionType;
import io.confluent.ksql.execution.interpreter.terms.ArithmeticBinaryTerm;
import io.confluent.ksql.execution.interpreter.terms.ArithmeticBinaryTerm.ArithmeticBinaryFunction;
import io.confluent.ksql.execution.interpreter.terms.CastTerm;
import io.confluent.ksql.execution.interpreter.terms.Term;
import io.confluent.ksql.schema.Operator;
import io.confluent.ksql.schema.ksql.types.SqlBaseType;
import io.confluent.ksql.schema.ksql.types.SqlDecimal;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.util.DecimalUtil;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.Pair;
import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;

public final class ArithmeticInterpreter {
  private ArithmeticInterpreter() { }

  public static Term doArithmetic(
      final ArithmeticBinaryExpression node,
      final Pair<Term, SqlType> left,
      final Pair<Term, SqlType> right,
      final SqlType resultType,
      final KsqlConfig ksqlConfig) {
    if (resultType.baseType() == SqlBaseType.DECIMAL) {
      final SqlDecimal decimal = (SqlDecimal) resultType;
      final CastTerm leftTerm = CastInterpreter.cast(left.left, left.right,
          DecimalUtil.toSqlDecimal(left.right), ksqlConfig);
      final CastTerm rightTerm = CastInterpreter.cast(right.left, right.right,
          DecimalUtil.toSqlDecimal(right.right), ksqlConfig);
      final DecimalArithmeticBinaryFunction fn = getDecimalFunction(decimal, node.getOperator());
      return new ArithmeticBinaryTerm(leftTerm, rightTerm,
          (o1, o2) -> fn.doFunction((BigDecimal) o1, (BigDecimal) o2),
          resultType);
    } else {
      final Term leftTerm =
          left.getRight().baseType() == SqlBaseType.DECIMAL
              ? CastInterpreter.cast(left.left, left.right, SqlTypes.DOUBLE, ksqlConfig)
              : left.getLeft();
      final Term rightTerm =
          right.getRight().baseType() == SqlBaseType.DECIMAL
              ? CastInterpreter.cast(right.left, right.right, SqlTypes.DOUBLE, ksqlConfig)
              : right.getLeft();
      return new ArithmeticBinaryTerm(leftTerm, rightTerm,
          ArithmeticInterpreter.getNonDecimalArithmeticFunction(
              node, left.getRight(), right.getRight()),
          resultType);
    }
  }

  private static ArithmeticBinaryFunction getNonDecimalArithmeticFunction(
      final ArithmeticBinaryExpression node,
      final SqlType leftType,
      final SqlType rightType) {
    final SqlBaseType leftBaseType = leftType.baseType();
    final SqlBaseType rightBaseType = rightType.baseType();
    if (leftBaseType == SqlBaseType.STRING && rightBaseType == SqlBaseType.STRING) {
      return (o1, o2) -> (String) o1 + (String) o2;
    } else if (leftBaseType == SqlBaseType.DOUBLE || rightBaseType == SqlBaseType.DOUBLE) {
      final DoubleArithmeticBinaryFunction fn = getDoubleFunction(node.getOperator());
      return (o1, o2) -> fn.doFunction(
          toDouble(o1, leftType, ConversionType.ARITHMETIC),
          toDouble(o2, rightType, ConversionType.ARITHMETIC));
    } else if (leftBaseType == SqlBaseType.BIGINT || rightBaseType == SqlBaseType.BIGINT) {
      final LongArithmeticBinaryFunction fn = getLongFunction(node.getOperator());
      return (o1, o2) -> fn.doFunction(
          toLong(o1, leftType, ConversionType.ARITHMETIC),
          toLong(o2, rightType, ConversionType.ARITHMETIC));
    } else if (leftBaseType == SqlBaseType.INTEGER || rightBaseType == SqlBaseType.INTEGER) {
      final IntegerArithmeticBinaryFunction fn = getIntegerFunction(node.getOperator());
      return (o1, o2) -> fn.doFunction(
          toInteger(o1, leftType, ConversionType.ARITHMETIC),
          toInteger(o2, rightType, ConversionType.ARITHMETIC));
    } else {
      throw new KsqlException("Can't do arithmetic for types " + leftType + " and " + rightType);
    }
  }

  private static DoubleArithmeticBinaryFunction getDoubleFunction(final Operator operator) {
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

  private static IntegerArithmeticBinaryFunction getIntegerFunction(final Operator operator) {
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

  private static LongArithmeticBinaryFunction getLongFunction(final Operator operator) {
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

  private static DecimalArithmeticBinaryFunction getDecimalFunction(
      final SqlDecimal decimal,
      final Operator operator) {
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

  private interface IntegerArithmeticBinaryFunction {
    Integer doFunction(Integer o1, Integer o2);
  }

  private interface DoubleArithmeticBinaryFunction {
    Double doFunction(Double o1, Double o2);
  }

  private interface LongArithmeticBinaryFunction {
    Long doFunction(Long o1, Long o2);
  }

  private interface DecimalArithmeticBinaryFunction {
    BigDecimal doFunction(BigDecimal o1, BigDecimal o2);
  }
}
