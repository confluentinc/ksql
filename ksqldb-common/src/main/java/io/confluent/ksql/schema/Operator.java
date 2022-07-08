/*
 * Copyright 2019 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"; you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.schema;

import static io.confluent.ksql.util.DecimalUtil.toSqlDecimal;
import static java.util.Objects.requireNonNull;

import io.confluent.ksql.schema.ksql.types.SqlBaseType;
import io.confluent.ksql.schema.ksql.types.SqlDecimal;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.util.KsqlException;
import java.util.function.BinaryOperator;

public enum Operator {
  ADD("+", SqlDecimal::add) {
    @Override
    public SqlType resultType(final SqlType left, final SqlType right) throws KsqlException {
      checkForNullTypes(left, right);

      if (left.baseType() == SqlBaseType.STRING && right.baseType() == SqlBaseType.STRING) {
        return SqlTypes.STRING;
      }

      return super.resultType(left, right);
    }
  },
  SUBTRACT("-", SqlDecimal::subtract),
  MULTIPLY("*", SqlDecimal::multiply),
  DIVIDE("/", SqlDecimal::divide),
  MODULUS("%", SqlDecimal::modulus);

  private final String symbol;
  private final BinaryOperator<SqlDecimal> binaryResolver;

  Operator(final String symbol, final BinaryOperator<SqlDecimal> binaryResolver) {
    this.symbol = requireNonNull(symbol, "symbol");
    this.binaryResolver = requireNonNull(binaryResolver, "binaryResolver");
  }

  public String getSymbol() {
    return symbol;
  }

  /**
   * Determine the result type for the given parameters.
   *
   * @param left the left side of the operation.
   * @param right the right side of the operation.
   * @return the result schema.
   */
  public SqlType resultType(final SqlType left, final SqlType right) throws KsqlException {
    checkForNullTypes(left, right);

    if (left.baseType().isNumber() && right.baseType().isNumber()) {
      if (left.baseType().canImplicitlyCast(right.baseType())) {
        if (right.baseType() != SqlBaseType.DECIMAL) {
          return right;
        }

        return binaryResolver.apply(toSqlDecimal(left), (SqlDecimal) right);
      }

      if (right.baseType().canImplicitlyCast(left.baseType())) {
        if (left.baseType() != SqlBaseType.DECIMAL) {
          return left;
        }

        return binaryResolver.apply((SqlDecimal) left, toSqlDecimal(right));
      }
    }

    throw new KsqlException(
        "Unsupported arithmetic types. " + left.baseType() + " " + right.baseType());
  }

  private static void checkForNullTypes(final SqlType left, final SqlType right)
          throws KsqlException {
    if (left == null || right == null) {
      throw new KsqlException(
              String.format("Arithmetic on types %s and %s are not supported.", left, right));
    }
    return;
  }
}
