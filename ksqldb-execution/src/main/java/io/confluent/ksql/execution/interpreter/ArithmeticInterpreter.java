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

  public static Object doArithmetic(
      final ArithmeticBinaryExpression node,
      final Pair<Object, SqlType> left,
      final Pair<Object, SqlType> right,
      final SqlType resultType,
      final KsqlConfig ksqlConfig) {
    if (resultType.baseType() == SqlBaseType.DECIMAL) {
      final SqlDecimal decimal = (SqlDecimal) resultType;
      final BigDecimal leftExpr = (BigDecimal) CastInterpreter.cast(left.left, left.right,
          DecimalUtil.toSqlDecimal(left.right), ksqlConfig);
      final BigDecimal rightExpr = (BigDecimal) CastInterpreter.cast(right.left, right.right,
          DecimalUtil.toSqlDecimal(right.right), ksqlConfig);
      return ArithmeticInterpreter.apply(decimal, node.getOperator(), leftExpr, rightExpr);
    } else {
      final Object leftObject =
          left.getRight().baseType() == SqlBaseType.DECIMAL
              ? CastInterpreter.cast(left.left, left.right, SqlTypes.DOUBLE, ksqlConfig)
              : left.getLeft();
      final Object rightObject =
          right.getRight().baseType() == SqlBaseType.DECIMAL
              ? CastInterpreter.cast(right.left, right.right, SqlTypes.DOUBLE, ksqlConfig)
              : right.getLeft();
      return ArithmeticInterpreter.doArithmeticNonDecimal(
          node, left.getRight(), right.getRight(), leftObject, rightObject);
    }
  }

  private static Object doArithmeticNonDecimal(
      final ArithmeticBinaryExpression node,
      final SqlType leftType,
      final SqlType rightType,
      final Object leftObject,
      final Object rightObject) {
    final SqlBaseType leftBaseType = leftType.baseType();
    final SqlBaseType rightBaseType = rightType.baseType();
    if (leftBaseType == SqlBaseType.STRING && rightBaseType == SqlBaseType.STRING) {
      return (String) leftObject + (String) rightObject;
    } else if (leftBaseType == SqlBaseType.DOUBLE || rightBaseType == SqlBaseType.DOUBLE) {
      return ArithmeticInterpreter.apply(node.getOperator(),
          toDouble(leftObject, leftType, ConversionType.ARITHMETIC),
          toDouble(rightObject, rightType, ConversionType.ARITHMETIC));
    } else if (leftBaseType == SqlBaseType.BIGINT || rightBaseType == SqlBaseType.BIGINT) {
      return ArithmeticInterpreter.apply(node.getOperator(),
          toLong(leftObject, leftType, ConversionType.ARITHMETIC),
          toLong(rightObject, rightType, ConversionType.ARITHMETIC));
    } else if (leftBaseType == SqlBaseType.INTEGER || rightBaseType == SqlBaseType.INTEGER) {
      return ArithmeticInterpreter.apply(node.getOperator(),
          toInteger(leftObject, leftType, ConversionType.ARITHMETIC),
          toInteger(rightObject, rightType, ConversionType.ARITHMETIC));
    } else {
      throw new KsqlException("Can't do arithmetic for types " + leftType + " and " + rightType);
    }
  }

  private static Double apply(final Operator operator, final Double a, final Double b) {
    switch (operator) {
      case ADD:
        return a + b;
      case SUBTRACT:
        return a - b;
      case MULTIPLY:
        return a * b;
      case DIVIDE:
        return a / b;
      case MODULUS:
        return a % b;
      default:
        throw new KsqlException("Unknown operator " + operator);
    }
  }

  private static Integer apply(final Operator operator, final Integer a, final Integer b) {
    switch (operator) {
      case ADD:
        return a + b;
      case SUBTRACT:
        return a - b;
      case MULTIPLY:
        return a * b;
      case DIVIDE:
        return a / b;
      case MODULUS:
        return a % b;
      default:
        throw new KsqlException("Unknown operator " + operator);
    }
  }

  private static Long apply(final Operator operator, final Long a, final Long b) {
    switch (operator) {
      case ADD:
        return a + b;
      case SUBTRACT:
        return a - b;
      case MULTIPLY:
        return a * b;
      case DIVIDE:
        return a / b;
      case MODULUS:
        return a % b;
      default:
        throw new KsqlException("Unknown operator " + operator);
    }
  }

  private static BigDecimal apply(
      final SqlDecimal decimal,
      final Operator operator,
      final BigDecimal a,
      final BigDecimal b) {
    final MathContext mc = new MathContext(decimal.getPrecision(),
        RoundingMode.UNNECESSARY);
    switch (operator) {
      case ADD:
        return a.add(b, mc).setScale(decimal.getScale());
      case SUBTRACT:
        return a.subtract(b, mc).setScale(decimal.getScale());
      case MULTIPLY:
        return a.multiply(b, mc).setScale(decimal.getScale());
      case DIVIDE:
        return a.divide(b, mc).setScale(decimal.getScale());
      case MODULUS:
        return a.remainder(b, mc).setScale(decimal.getScale());
      default:
        throw new KsqlException("DECIMAL operator not supported: " + operator);
    }
  }
}
