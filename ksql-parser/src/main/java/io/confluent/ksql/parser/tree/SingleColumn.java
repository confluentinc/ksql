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

package io.confluent.ksql.parser.tree;

import static java.util.Objects.requireNonNull;

import io.confluent.ksql.parser.exception.ParseFailedException;
import io.confluent.ksql.util.SchemaUtil;
import java.util.Objects;
import java.util.Optional;

public class SingleColumn
    extends SelectItem {

  private final Optional<AllColumns> allColumns;
  private final Optional<String> alias;
  private final Expression expression;

  public SingleColumn(final Expression expression) {
    this(Optional.empty(), expression, Optional.empty(), Optional.empty());
  }

  public SingleColumn(final Expression expression, final Optional<String> alias) {
    this(Optional.empty(), expression, alias, Optional.empty());
  }

  public SingleColumn(final Expression expression, final String alias) {
    this(Optional.empty(), expression, Optional.of(alias), Optional.empty());
  }

  public SingleColumn(
      final NodeLocation location, final Expression expression, final Optional<String> alias) {
    this(Optional.of(location), expression, alias, Optional.empty());
  }

  public SingleColumn(
      final Expression expression,
      final String alias,
      final AllColumns allColumns) {
    this(Optional.empty(), expression, Optional.of(alias), Optional.of(allColumns));
  }

  private SingleColumn(final SingleColumn other, final Expression expression) {
    this(other.getLocation(), expression, other.alias, other.allColumns);
  }

  private SingleColumn(
      final Optional<NodeLocation> location,
      final Expression expression,
      final Optional<String> alias,
      final Optional<AllColumns> allColumns) {
    super(location);
    requireNonNull(expression, "expression is null");
    requireNonNull(alias, "alias is null");
    requireNonNull(allColumns, "allColumns is null");

    alias.ifPresent(name -> {
      checkForReservedToken(expression, name, SchemaUtil.ROWTIME_NAME);
      checkForReservedToken(expression, name, SchemaUtil.ROWKEY_NAME);
    });

    this.expression = expression;
    this.alias = alias;
    this.allColumns = allColumns;
  }

  public SingleColumn copyWithExpression(final Expression expression) {
    return new SingleColumn(this, expression);
  }

  private void checkForReservedToken(
      final Expression expression, final String alias, final String reservedToken) {
    if (alias.equalsIgnoreCase(reservedToken)) {
      final String text = expression.toString();
      if (!text.substring(text.indexOf(".") + 1).equalsIgnoreCase(reservedToken)) {
        throw new ParseFailedException(reservedToken + " is a reserved token for implicit column. "
                        + "You cannot use it as an alias for a column.");
      }
    }
  }

  public Optional<String> getAlias() {
    return alias;
  }

  public Expression getExpression() {
    return expression;
  }

  /**
   * @return a reference to an {@code AllColumns} if this single column
   *         was expanded as part of a {@code SELECT *} Expression, otherwise
   *         returns an empty optional
   */
  public Optional<AllColumns> getAllColumns() {
    return allColumns;
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
    return Objects.equals(this.alias, other.alias)
        && Objects.equals(this.expression, other.expression)
        && Objects.equals(this.allColumns, other.allColumns);
  }

  @Override
  public int hashCode() {
    return Objects.hash(allColumns, alias, expression);
  }

  @Override
  public String toString() {
    return "SingleColumn{" + "allColumns=" + allColumns
        + ", alias=" + alias
        + ", expression=" + expression
        + '}';
  }

  @Override
  public <R, C> R accept(final AstVisitor<R, C> visitor, final C context) {
    return visitor.visitSingleColumn(this, context);
  }
}
