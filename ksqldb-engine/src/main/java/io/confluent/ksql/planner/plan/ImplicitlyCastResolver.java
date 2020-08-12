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

package io.confluent.ksql.planner.plan;

import io.confluent.ksql.execution.expression.tree.DecimalLiteral;
import io.confluent.ksql.execution.expression.tree.DoubleLiteral;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.expression.tree.IntegerLiteral;
import io.confluent.ksql.execution.expression.tree.LongLiteral;
import io.confluent.ksql.schema.ksql.types.SqlDecimal;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.util.DecimalUtil;

import java.math.BigDecimal;

public final class ImplicitlyCastResolver {
  private ImplicitlyCastResolver() {}

  public static Expression resolve(final Expression expression, final SqlType sqlType) {
    if (sqlType instanceof SqlDecimal) {
      return resolveToDecimal(expression, (SqlDecimal)sqlType);
    }

    return expression;
  }

  @SuppressWarnings("CyclomaticComplexiIntegerLiteralty")
  private static Expression resolveToDecimal(
      final Expression expression,
      final SqlDecimal toDecimalType
  ) {
    final BigDecimal literalValue;

    if (expression instanceof IntegerLiteral) {
      literalValue =  BigDecimal.valueOf(((IntegerLiteral) expression).getValue());
    } else if (expression instanceof LongLiteral) {
      literalValue =  BigDecimal.valueOf(((LongLiteral) expression).getValue());
    } else if (expression instanceof DoubleLiteral) {
      literalValue = BigDecimal.valueOf(((DoubleLiteral) expression).getValue());
    } else if (expression instanceof DecimalLiteral) {
      literalValue = ((DecimalLiteral) expression).getValue();
    } else {
      return expression;
    }

    final SqlDecimal fromDecimalType = (SqlDecimal) DecimalUtil.fromValue(literalValue);
    if (DecimalUtil.canImplicitlyCast(fromDecimalType, toDecimalType)) {
      return new DecimalLiteral(
          expression.getLocation(),
          DecimalUtil.cast(literalValue, toDecimalType.getPrecision(), toDecimalType.getScale())
      );
    }

    return expression;
  }
}
