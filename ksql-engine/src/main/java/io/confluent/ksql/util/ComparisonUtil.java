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

package io.confluent.ksql.util;

import io.confluent.ksql.parser.tree.ComparisonExpression;
import io.confluent.ksql.schema.ksql.SchemaConverters;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Schema.Type;

final class ComparisonUtil {

  private ComparisonUtil() {

  }

  static boolean isValidComparison(
      final Schema left,
      final ComparisonExpression.Type operator,
      final Schema right
  ) {
    if (SchemaUtil.isNumber(left) && SchemaUtil.isNumber(right)) {
      return true;
    }

    if (left.type() == Type.STRING && right.type() == Type.STRING) {
      return  true;
    }

    if (left.type() == Type.BOOLEAN && right.type() == Type.BOOLEAN) {
      if (operator == ComparisonExpression.Type.EQUAL
          || operator == ComparisonExpression.Type.NOT_EQUAL) {
        return true;
      }
    }

    throw new KsqlException("Operator " + operator + " cannot be used to compare "
        + SchemaConverters.logicalToSqlConverter().toSqlType(left).baseType()
        + " and "
        + SchemaConverters.logicalToSqlConverter().toSqlType(right).baseType()
    );
  }
}
