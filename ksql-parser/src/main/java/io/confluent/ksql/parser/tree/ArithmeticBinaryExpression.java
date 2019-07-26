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

import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.schema.Operator;
import java.util.Objects;
import java.util.Optional;

@Immutable
public class ArithmeticBinaryExpression extends Expression {

  private final Operator operator;
  private final Expression left;
  private final Expression right;

  public ArithmeticBinaryExpression(
      final Operator operator,
      final Expression left,
      final Expression right
  ) {
    this(Optional.empty(), operator, left, right);
  }

  public ArithmeticBinaryExpression(
      final Optional<NodeLocation> location,
      final Operator operator,
      final Expression left,
      final Expression right
  ) {
    super(location);
    this.operator = Objects.requireNonNull(operator, "operator");
    this.left = Objects.requireNonNull(left, "left");
    this.right = Objects.requireNonNull(right, "right");
  }

  public Operator getOperator() {
    return operator;
  }

  public Expression getLeft() {
    return left;
  }

  public Expression getRight() {
    return right;
  }

  @Override
  public <R, C> R accept(final ExpressionVisitor<R, C> visitor, final C context) {
    return visitor.visitArithmeticBinary(this, context);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final ArithmeticBinaryExpression that = (ArithmeticBinaryExpression) o;
    return (operator == that.operator)
           && Objects.equals(left, that.left)
           && Objects.equals(right, that.right);
  }

  @Override
  public int hashCode() {
    return Objects.hash(operator, left, right);
  }
}
