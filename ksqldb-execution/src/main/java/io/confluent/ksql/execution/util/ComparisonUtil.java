/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.ksql.execution.util;

import io.confluent.ksql.execution.expression.tree.ComparisonExpression;
import io.confluent.ksql.schema.ksql.SqlBaseType;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.util.KsqlException;

final class ComparisonUtil {

  private ComparisonUtil() {

  }

  static boolean isValidComparison(
      final SqlType left, final ComparisonExpression.Type operator, final SqlType right
  ) {
    if (left.baseType().isNumber() && right.baseType().isNumber()) {
      return true;
    }

    if (left.baseType() == SqlBaseType.STRING && right.baseType() == SqlBaseType.STRING) {
      return true;
    }

    if (left.baseType() == SqlBaseType.BOOLEAN && right.baseType() == SqlBaseType.BOOLEAN) {
      if (operator == ComparisonExpression.Type.EQUAL
          || operator == ComparisonExpression.Type.NOT_EQUAL) {
        return true;
      }
    }

    throw new KsqlException(
        "Operator " + operator + " cannot be used to compare "
            + left.baseType() + " and " + right.baseType()
    );
  }
}
