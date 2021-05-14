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

package io.confluent.ksql.execution.plan;

import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.execution.expression.formatter.ExpressionFormatter;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.schema.utils.FormatOptions;
import java.util.Objects;

/**
 * Pojo holding field name and expression of a select item.
 */
@Immutable
public final class SelectExpression {
  private static final String FMT = "%s AS %s";

  private final ColumnName alias;
  private final Expression expression;

  private SelectExpression(final ColumnName alias, final Expression expression) {
    this.alias = Objects.requireNonNull(alias, "alias");
    this.expression = Objects.requireNonNull(expression, "expression");
  }

  public static SelectExpression of(final ColumnName name, final Expression expression) {
    return new SelectExpression(name, expression);
  }

  public ColumnName getAlias() {
    return alias;
  }

  public Expression getExpression() {
    return expression;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final SelectExpression that = (SelectExpression) o;
    return Objects.equals(alias, that.alias)
        && Objects.equals(expression, that.expression);
  }

  @Override
  public int hashCode() {
    return Objects.hash(alias, expression);
  }

  @Override
  public String toString() {
    return format(FormatOptions.none());
  }

  public String format(final FormatOptions formatOptions) {
    return String.format(
        FMT,
        ExpressionFormatter.formatExpression(expression, formatOptions),
        alias.toString(formatOptions)
    );
  }
}
