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

import java.util.Objects;
import java.util.Optional;

public class ArithmeticBinaryExpression
    extends Expression {

  public enum Type {
    ADD("+"),
    SUBTRACT("-"),
    MULTIPLY("*"),
    DIVIDE("/"),
    MODULUS("%");
    private final String value;

    Type(final String value) {
      this.value = value;
    }

    public String getValue() {
      return value;
    }
  }

  private final Type type;
  private final Expression left;
  private final Expression right;

  public ArithmeticBinaryExpression(
      final Type type,
      final Expression left,
      final Expression right) {
    this(Optional.empty(), type, left, right);
  }

  public ArithmeticBinaryExpression(
      final NodeLocation location,
      final Type type,
      final Expression left,
      final Expression right) {
    this(Optional.of(location), type, left, right);
  }

  private ArithmeticBinaryExpression(
      final Optional<NodeLocation> location,
      final Type type,
      final Expression left,
      final Expression right) {
    super(location);
    this.type = type;
    this.left = left;
    this.right = right;
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
  public <R, C> R accept(final AstVisitor<R, C> visitor, final C context) {
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
    return (type == that.type)
           && Objects.equals(left, that.left)
           && Objects.equals(right, that.right);
  }

  @Override
  public int hashCode() {
    return Objects.hash(type, left, right);
  }
}
