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
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.parser.NodeLocation;
import java.util.Objects;
import java.util.Optional;

@Immutable
public class SingleColumn extends SelectItem {

  private final Optional<ColumnName> alias;
  private final Expression expression;

  public SingleColumn(
      final Expression expression,
      final Optional<ColumnName> alias
  ) {
    this(Optional.empty(), expression, alias);
  }

  public SingleColumn(
      final Optional<NodeLocation> location,
      final Expression expression,
      final Optional<ColumnName> alias
  ) {
    super(location);

    this.expression = requireNonNull(expression, "expression");
    this.alias = requireNonNull(alias, "alias");
  }

  public SingleColumn copyWithExpression(final Expression expression) {
    return new SingleColumn(getLocation(), expression, alias);
  }

  public Optional<ColumnName> getAlias() {
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
    return Objects.equals(this.alias, other.alias)
        && Objects.equals(this.expression, other.expression);
  }

  @Override
  public int hashCode() {
    return Objects.hash(alias, expression);
  }

  @Override
  public String toString() {
    return "SingleColumn{"
        + ", alias=" + alias
        + ", expression=" + expression
        + '}';
  }

  @Override
  public <R, C> R accept(final AstVisitor<R, C> visitor, final C context) {
    return visitor.visitSingleColumn(this, context);
  }
}
