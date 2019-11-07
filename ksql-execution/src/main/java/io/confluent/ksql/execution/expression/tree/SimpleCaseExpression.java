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

package io.confluent.ksql.execution.expression.tree;

import static java.util.Objects.requireNonNull;

import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.parser.NodeLocation;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

@Immutable
public class SimpleCaseExpression extends Expression {

  private final Expression operand;
  private final ImmutableList<WhenClause> whenClauses;
  private final Optional<Expression> defaultValue;

  public SimpleCaseExpression(
      Expression operand, List<WhenClause> whenClauses, Optional<Expression> defaultValue
  ) {
    this(Optional.empty(), operand, whenClauses, defaultValue);
  }

  public SimpleCaseExpression(
      Optional<NodeLocation> location, Expression operand, List<WhenClause> whenClauses,
      Optional<Expression> defaultValue
  ) {
    super(location);
    this.operand = requireNonNull(operand, "operand");
    this.whenClauses = ImmutableList.copyOf(requireNonNull(whenClauses, "whenClauses"));
    this.defaultValue = requireNonNull(defaultValue);
  }

  public Expression getOperand() {
    return operand;
  }

  public List<WhenClause> getWhenClauses() {
    return whenClauses;
  }

  public Optional<Expression> getDefaultValue() {
    return defaultValue;
  }

  @Override
  public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
    return visitor.visitSimpleCaseExpression(this, context);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    SimpleCaseExpression that = (SimpleCaseExpression) o;
    return Objects.equals(operand, that.operand)
        && Objects.equals(whenClauses, that.whenClauses)
        && Objects.equals(defaultValue, that.defaultValue);
  }

  @Override
  public int hashCode() {
    return Objects.hash(operand, whenClauses, defaultValue);
  }
}
