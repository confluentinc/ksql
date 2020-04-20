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

import static java.util.Objects.requireNonNull;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.execution.expression.tree.ComparisonExpression;
import io.confluent.ksql.execution.expression.tree.ComparisonExpression.Type;
import io.confluent.ksql.schema.ksql.SqlBaseType;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.util.KsqlException;
import java.util.List;
import java.util.function.BiPredicate;
import java.util.function.Predicate;

final class ComparisonUtil {

  private static final List<Handler> HANDLERS = ImmutableList.<Handler>builder()
      .add(handler(SqlBaseType::isNumber, ComparisonUtil::handleNumber))
      .add(handler(SqlBaseType.STRING, ComparisonUtil::handleString))
      .add(handler(SqlBaseType.BOOLEAN, ComparisonUtil::handleBoolean))
      .build();

  private ComparisonUtil() {
  }

  static boolean isValidComparison(
      final SqlType left, final ComparisonExpression.Type operator, final SqlType right
  ) {
    if (left == null || right == null) {
      throw nullSchemaException(left, operator, right);
    }

    return HANDLERS.stream()
        .filter(h -> h.handles.test(left.baseType()))
        .findFirst()
        .map(h -> h.validator.test(operator, right))
        .orElse(false);
  }

  private static KsqlException nullSchemaException(
      final SqlType left,
      final Type operator,
      final SqlType right
  ) {
    final String leftType = left == null ? "NULL" : left.baseType().name();
    final String rightType = right == null ? "NULL" : right.baseType().name();

    return new KsqlException(
        "Comparison with NULL not supported: "
            + leftType + " " + operator.getValue() + " " + rightType
        + System.lineSeparator()
        + "Use 'IS NULL' or 'IS NOT NULL' instead."
    );
  }

  private static boolean handleNumber(final Type operator, final SqlType right) {
    return right.baseType().isNumber();
  }

  private static boolean handleString(final Type operator, final SqlType right) {
    return right.baseType() == SqlBaseType.STRING;
  }

  private static boolean handleBoolean(final Type operator, final SqlType right) {
    return right.baseType() == SqlBaseType.BOOLEAN
        && (operator == Type.EQUAL || operator == Type.NOT_EQUAL);
  }

  private static Handler handler(
      final SqlBaseType baseType,
      final BiPredicate<Type, SqlType> validator
  ) {
    return handler(t -> t == baseType, validator);
  }

  private static Handler handler(
      final Predicate<SqlBaseType> handles,
      final BiPredicate<Type, SqlType> validator
  ) {
    return new Handler(handles, validator);
  }

  private static final class Handler {

    final Predicate<SqlBaseType> handles;
    final BiPredicate<Type, SqlType> validator;

    private Handler(
        final Predicate<SqlBaseType> handles,
        final BiPredicate<Type, SqlType> validator
    ) {
      this.handles = requireNonNull(handles, "handles");
      this.validator = requireNonNull(validator, "validator");
    }
  }
}
