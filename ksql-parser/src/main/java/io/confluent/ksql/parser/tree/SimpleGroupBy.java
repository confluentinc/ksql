/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.parser.tree;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.util.Objects.requireNonNull;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.confluent.ksql.parser.ExpressionFormatter;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

public class SimpleGroupBy
    extends GroupingElement {

  private final List<Expression> columns;

  public SimpleGroupBy(final List<Expression> simpleGroupByExpressions) {
    this(Optional.empty(), simpleGroupByExpressions);
  }

  public SimpleGroupBy(
      final NodeLocation location,
      final List<Expression> simpleGroupByExpressions) {
    this(Optional.of(location), simpleGroupByExpressions);
  }

  private SimpleGroupBy(
      final Optional<NodeLocation> location,
      final List<Expression> simpleGroupByExpressions) {
    super(location);
    this.columns = requireNonNull(simpleGroupByExpressions);
  }

  public List<Expression> getColumnExpressions() {
    return columns;
  }

  @Override
  public List<Set<Expression>> enumerateGroupingSets() {
    return ImmutableList.of(ImmutableSet.copyOf(columns));
  }

  @Override
  protected <R, C> R accept(final AstVisitor<R, C> visitor, final C context) {
    return visitor.visitSimpleGroupBy(this, context);
  }

  @Override
  public String format() {
    final Set<Expression>
        columns =
        ImmutableSet.copyOf(this.columns);
    if (columns.size() == 1) {
      return ExpressionFormatter.formatExpression(getOnlyElement(columns));
    }
    return ExpressionFormatter.formatGroupingSet(columns);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final SimpleGroupBy that = (SimpleGroupBy) o;
    return Objects.equals(columns, that.columns);
  }

  @Override
  public int hashCode() {
    return Objects.hash(columns);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("columns", columns)
        .toString();
  }
}
