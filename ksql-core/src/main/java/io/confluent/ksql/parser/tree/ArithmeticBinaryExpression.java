/**
 * Copyright 2017 Confluent Inc.
 *
 **/

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

    Type(String value) {
      this.value = value;
    }

    public String getValue() {
      return value;
    }
  }

  private final Type type;
  private final Expression left;
  private final Expression right;

  public ArithmeticBinaryExpression(Type type, Expression left, Expression right) {
    this(Optional.empty(), type, left, right);
  }

  public ArithmeticBinaryExpression(NodeLocation location, Type type, Expression left,
                                    Expression right) {
    this(Optional.of(location), type, left, right);
  }

  private ArithmeticBinaryExpression(Optional<NodeLocation> location, Type type, Expression left,
                                     Expression right) {
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
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitArithmeticBinary(this, context);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    ArithmeticBinaryExpression that = (ArithmeticBinaryExpression) o;
    return (type == that.type) &&
           Objects.equals(left, that.left) &&
           Objects.equals(right, that.right);
  }

  @Override
  public int hashCode() {
    return Objects.hash(type, left, right);
  }
}
