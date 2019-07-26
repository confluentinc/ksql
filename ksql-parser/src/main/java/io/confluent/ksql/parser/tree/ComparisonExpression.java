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
import java.util.Objects;
import java.util.Optional;

@Immutable
public class ComparisonExpression extends Expression {

  public enum Type {
    EQUAL("="),
    NOT_EQUAL("<>"),
    LESS_THAN("<"),
    LESS_THAN_OR_EQUAL("<="),
    GREATER_THAN(">"),
    GREATER_THAN_OR_EQUAL(">="),
    IS_DISTINCT_FROM("IS DISTINCT FROM");

    private final String value;

    Type(final String value) {
      this.value = value;
    }

    public String getValue() {
      return value;
    }

    public Type flip() {
      switch (this) {
        case EQUAL:
          return EQUAL;
        case NOT_EQUAL:
          return NOT_EQUAL;
        case LESS_THAN:
          return GREATER_THAN;
        case LESS_THAN_OR_EQUAL:
          return GREATER_THAN_OR_EQUAL;
        case GREATER_THAN:
          return LESS_THAN;
        case GREATER_THAN_OR_EQUAL:
          return LESS_THAN_OR_EQUAL;
        case IS_DISTINCT_FROM:
          return IS_DISTINCT_FROM;
        default:
          throw new IllegalArgumentException("Unsupported comparison: " + this);
      }
    }

    public Type negate() {
      switch (this) {
        case EQUAL:
          return NOT_EQUAL;
        case NOT_EQUAL:
          return EQUAL;
        case LESS_THAN:
          return GREATER_THAN_OR_EQUAL;
        case LESS_THAN_OR_EQUAL:
          return GREATER_THAN;
        case GREATER_THAN:
          return LESS_THAN_OR_EQUAL;
        case GREATER_THAN_OR_EQUAL:
          return LESS_THAN;
        default:
          throw new IllegalArgumentException("Unsupported comparison: " + this);
      }
    }
  }

  private final Type type;
  private final Expression left;
  private final Expression right;

  public ComparisonExpression(
      final Type type,
      final Expression left,
      final Expression right
  ) {
    this(Optional.empty(), type, left, right);
  }

  public ComparisonExpression(
      final Optional<NodeLocation> location,
      final Type type,
      final Expression left,
      final Expression right
  ) {
    super(location);
    this.type = requireNonNull(type, "type");
    this.left = requireNonNull(left, "left");
    this.right = requireNonNull(right, "right");
  }

  public Type getType() {
    return type;
  }

  public Expression getLeft() {
    return left;
  }

  public Expression getRight() {
    return right;
  }

  @Override
  public <R, C> R accept(final ExpressionVisitor<R, C> visitor, final C context) {
    return visitor.visitComparisonExpression(this, context);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final ComparisonExpression that = (ComparisonExpression) o;
    return (type == that.type)
           && Objects.equals(left, that.left)
           && Objects.equals(right, that.right);
  }

  @Override
  public int hashCode() {
    return Objects.hash(type, left, right);
  }
}
