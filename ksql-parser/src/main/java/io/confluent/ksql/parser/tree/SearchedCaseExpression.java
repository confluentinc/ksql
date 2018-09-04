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

import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public class SearchedCaseExpression
    extends Expression {

  private final List<WhenClause> whenClauses;
  private final Optional<Expression> defaultValue;

  public SearchedCaseExpression(
      final List<WhenClause> whenClauses,
      final Optional<Expression> defaultValue) {
    this(Optional.empty(), whenClauses, defaultValue);
  }

  public SearchedCaseExpression(
      final NodeLocation location,
      final List<WhenClause> whenClauses,
      final Optional<Expression> defaultValue) {
    this(Optional.of(location), whenClauses, defaultValue);
  }

  private SearchedCaseExpression(
      final Optional<NodeLocation> location,
      final List<WhenClause> whenClauses,
      final Optional<Expression> defaultValue) {
    super(location);
    requireNonNull(whenClauses, "whenClauses is null");
    requireNonNull(defaultValue, "defaultValue is null");
    this.whenClauses = ImmutableList.copyOf(whenClauses);
    this.defaultValue = defaultValue;
  }

  public List<WhenClause> getWhenClauses() {
    return whenClauses;
  }

  public Optional<Expression> getDefaultValue() {
    return defaultValue;
  }

  @Override
  public <R, C> R accept(final AstVisitor<R, C> visitor, final C context) {
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
