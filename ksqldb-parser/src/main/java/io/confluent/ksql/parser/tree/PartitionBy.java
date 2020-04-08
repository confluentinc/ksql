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

package io.confluent.ksql.parser.tree;

import static java.util.Objects.requireNonNull;

import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.parser.NodeLocation;
import java.util.Objects;
import java.util.Optional;

@Immutable
public class PartitionBy extends AstNode {

  private final Expression expression;
  private final Optional<ColumnName> alias;

  public PartitionBy(
      final Optional<NodeLocation> location,
      final Expression partitionBy,
      final Optional<ColumnName> alias
  ) {
    super(location);
    this.expression = requireNonNull(partitionBy, "partitionBy");
    this.alias = requireNonNull(alias, "alias");
  }

  public Expression getExpression() {
    return expression;
  }

  public Optional<ColumnName> getAlias() {
    return alias;
  }

  @Override
  protected <R, C> R accept(final AstVisitor<R, C> visitor, final C context) {
    return visitor.visitPartitionBy(this, context);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final PartitionBy groupBy = (PartitionBy) o;
    return Objects.equals(expression, groupBy.expression)
        && Objects.equals(alias, groupBy.alias);
  }

  @Override
  public int hashCode() {
    return Objects.hash(expression, alias);
  }

  @Override
  public String toString() {
    return "PartitionBy{"
        + "expression=" + expression
        + ", alias=" + alias
        + '}';
  }
}
