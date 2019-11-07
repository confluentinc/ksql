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

import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.parser.NodeLocation;
import java.util.Objects;
import java.util.Optional;

@Immutable
public class ArithmeticUnaryExpression extends Expression {

  public enum Sign {
    PLUS,
    MINUS
  }

  private final Expression value;
  private final Sign sign;

  public ArithmeticUnaryExpression(Optional<NodeLocation> location, Sign sign, Expression value) {
    super(location);
    this.value = requireNonNull(value, "value");
    this.sign = requireNonNull(sign, "sign");
  }

  public static ArithmeticUnaryExpression positive(
      Optional<NodeLocation> location, Expression value
  ) {
    return new ArithmeticUnaryExpression(location, Sign.PLUS, value);
  }

  public static ArithmeticUnaryExpression negative(
      Optional<NodeLocation> location, Expression value
  ) {
    return new ArithmeticUnaryExpression(location, Sign.MINUS, value);
  }

  public Expression getValue() {
    return value;
  }

  public Sign getSign() {
    return sign;
  }

  @Override
  public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
    return visitor.visitArithmeticUnary(this, context);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    ArithmeticUnaryExpression that = (ArithmeticUnaryExpression) o;
    return Objects.equals(value, that.value)
        && (sign == that.sign);
  }

  @Override
  public int hashCode() {
    return Objects.hash(value, sign);
  }
}
