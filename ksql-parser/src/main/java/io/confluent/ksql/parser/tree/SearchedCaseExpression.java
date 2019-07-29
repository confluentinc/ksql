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

import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.Immutable;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

@Immutable
public class SearchedCaseExpression extends Expression {

  private final ImmutableList<WhenClause> whenClauses;
  private final Optional<Expression> defaultValue;

  public SearchedCaseExpression(
      final List<WhenClause> whenClauses,
      final Optional<Expression> defaultValue
  ) {
    this(Optional.empty(), whenClauses, defaultValue);
  }

  public SearchedCaseExpression(
      final Optional<NodeLocation> location,
      final List<WhenClause> whenClauses,
      final Optional<Expression> defaultValue
  ) {
    super(location);
    this.whenClauses = ImmutableList.copyOf(requireNonNull(whenClauses, "whenClauses"));
    this.defaultValue = requireNonNull(defaultValue, "defaultValue");
  }

  public List<WhenClause> getWhenClauses() {
    return whenClauses;
  }

  public Optional<Expression> getDefaultValue() {
    return defaultValue;
  }

  @Override
  public <R, C> R accept(final ExpressionVisitor<R, C> visitor, final C context) {
    return visitor.visitSearchedCaseExpression(this, context);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final SearchedCaseExpression that = (SearchedCaseExpression) o;
    return Objects.equals(whenClauses, that.whenClauses)
           && Objects.equals(defaultValue, that.defaultValue);
  }

  @Override
  public int hashCode() {
    return Objects.hash(whenClauses, defaultValue);
  }
}
