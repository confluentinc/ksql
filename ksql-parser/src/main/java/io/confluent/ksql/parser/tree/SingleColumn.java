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

import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.parser.exception.ParseFailedException;
import io.confluent.ksql.util.SchemaUtil;
import java.util.Objects;
import java.util.Optional;

@Immutable
public class SingleColumn extends SelectItem {

  private final Optional<AllColumns> source;
  private final Optional<String> alias;
  private final Expression expression;

  public SingleColumn(
      final Expression expression,
      final Optional<String> alias,
      final Optional<AllColumns> source
  ) {
    this(Optional.empty(), expression, alias, source);
  }

  public SingleColumn(
      final Optional<NodeLocation> location,
      final Expression expression,
      final Optional<String> alias,
      final Optional<AllColumns> source
  ) {
    super(location);

    alias.ifPresent(name -> {
      checkForReservedToken(expression, name, SchemaUtil.ROWTIME_NAME);
      checkForReservedToken(expression, name, SchemaUtil.ROWKEY_NAME);
    });

    this.expression = requireNonNull(expression, "expression");
    this.alias = requireNonNull(alias, "alias");
    this.source = requireNonNull(source, "source");
  }

  public SingleColumn copyWithExpression(final Expression expression) {
    return new SingleColumn(getLocation(), expression, alias, source);
  }

  private static void checkForReservedToken(
      final Expression expression,
      final String alias,
      final String reservedToken
  ) {
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
    return source;
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
        && Objects.equals(this.source, other.source);
  }

  @Override
  public int hashCode() {
    return Objects.hash(source, alias, expression);
  }

  @Override
  public String toString() {
    return "SingleColumn{" + "source=" + source
        + ", alias=" + alias
        + ", expression=" + expression
        + '}';
  }

  @Override
  public <R, C> R accept(final AstVisitor<R, C> visitor, final C context) {
    return visitor.visitSingleColumn(this, context);
  }
}
