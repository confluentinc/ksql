/**
 * Copyright 2017 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.confluent.ksql.parser.tree;

import static java.util.Objects.requireNonNull;

import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.SchemaUtil;
import java.util.Objects;
import java.util.Optional;

public class SingleColumn
    extends SelectItem {

  private final Optional<String> alias;
  private final Expression expression;

  public SingleColumn(final Expression expression) {
    this(Optional.empty(), expression, Optional.empty());
  }

  public SingleColumn(final Expression expression, final Optional<String> alias) {
    this(Optional.empty(), expression, alias);
  }

  public SingleColumn(final Expression expression, final String alias) {
    this(Optional.empty(), expression, Optional.of(alias));
  }

  public SingleColumn(
      final NodeLocation location, final Expression expression, final Optional<String> alias) {
    this(Optional.of(location), expression, alias);
  }

  private SingleColumn(final Optional<NodeLocation> location, final Expression expression,
                       final Optional<String> alias) {
    super(location);
    requireNonNull(expression, "expression is null");
    requireNonNull(alias, "alias is null");

    if (alias.isPresent()) {
      if (alias.get().equalsIgnoreCase(SchemaUtil.ROWTIME_NAME)) {
        final String expressionStr = expression.toString();
        if (!expressionStr.substring(expressionStr.indexOf(".") + 1)
            .equalsIgnoreCase(SchemaUtil.ROWTIME_NAME)) {
          throw new KsqlException(
              SchemaUtil.ROWTIME_NAME + " is a reserved token for implicit column."
                                  + " You cannot use it as an alias for a column.");
        }
      }
      if (alias.get().equalsIgnoreCase(SchemaUtil.ROWKEY_NAME)) {
        final String expressionStr = expression.toString();
        if (!expressionStr.substring(expressionStr.indexOf(".") + 1).equalsIgnoreCase(
            SchemaUtil.ROWKEY_NAME)) {
          throw new KsqlException(
              SchemaUtil.ROWKEY_NAME + " is a reserved token for implicit column."
                                  + " You cannot use it as an alias for a column.");
        }
      }
    }


    this.expression = expression;
    this.alias = alias;
  }

  public Optional<String> getAlias() {
    return alias;
  }

  public Expression getExpression() {
    return expression;
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    final SingleColumn other = (SingleColumn) obj;
    return Objects.equals(this.alias, other.alias) && Objects
        .equals(this.expression, other.expression);
  }

  @Override
  public int hashCode() {
    return Objects.hash(alias, expression);
  }

  @Override
  public String toString() {
    if (alias.isPresent()) {
      return expression.toString() + " " + alias.get();
    }

    return expression.toString();
  }

  @Override
  public <R, C> R accept(final AstVisitor<R, C> visitor, final C context) {
    return visitor.visitSingleColumn(this, context);
  }
}
