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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import io.confluent.ksql.parser.ExpressionFormatter;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.util.Objects.requireNonNull;

public class SimpleGroupBy
    extends GroupingElement {

  private final List<Expression> columns;

  public SimpleGroupBy(NodeLocation location, List<Expression> simpleGroupByExpressions) {
    this(Optional.of(location), simpleGroupByExpressions);
  }

  private SimpleGroupBy(Optional<NodeLocation> location,
                        List<Expression> simpleGroupByExpressions) {
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
  protected <R, C> R accept(AstVisitor<R, C> visitor, C context) {
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
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SimpleGroupBy that = (SimpleGroupBy) o;
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
