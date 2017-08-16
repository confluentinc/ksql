/**
 * Copyright 2017 Confluent Inc.
 *
 **/

package io.confluent.ksql.parser.tree;

import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class ArithmeticUnaryExpression
    extends Expression {

  public enum Sign {
    PLUS,
    MINUS
  }

  private final Expression value;
  private final Sign sign;

  public ArithmeticUnaryExpression(Sign sign, Expression value) {
    this(Optional.empty(), sign, value);
  }

  public ArithmeticUnaryExpression(NodeLocation location, Sign sign, Expression value) {
    this(Optional.of(location), sign, value);
  }

  private ArithmeticUnaryExpression(Optional<NodeLocation> location, Sign sign, Expression value) {
    super(location);
    requireNonNull(value, "value is null");
    requireNonNull(sign, "sign is null");

    this.value = value;
    this.sign = sign;
  }

  public static ArithmeticUnaryExpression positive(NodeLocation location, Expression value) {
    return new ArithmeticUnaryExpression(Optional.of(location), Sign.PLUS, value);
  }

  public static ArithmeticUnaryExpression negative(NodeLocation location, Expression value) {
    return new ArithmeticUnaryExpression(Optional.of(location), Sign.MINUS, value);
  }

  public static ArithmeticUnaryExpression positive(Expression value) {
    return new ArithmeticUnaryExpression(Optional.empty(), Sign.PLUS, value);
  }

  public static ArithmeticUnaryExpression negative(Expression value) {
    return new ArithmeticUnaryExpression(Optional.empty(), Sign.MINUS, value);
  }

  public Expression getValue() {
    return value;
  }

  public Sign getSign() {
    return sign;
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
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
