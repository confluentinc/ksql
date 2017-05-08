/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.ksql.parser.tree;

import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class NotExpression
    extends Expression {

  private final Expression value;

  public NotExpression(Expression value) {
    this(Optional.empty(), value);
  }

  public NotExpression(NodeLocation location, Expression value) {
    this(Optional.of(location), value);
  }

  private NotExpression(Optional<NodeLocation> location, Expression value) {
    super(location);
    requireNonNull(value, "value is null");
    this.value = value;
  }

  public Expression getValue() {
    return value;
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitNotExpression(this, context);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    NotExpression that = (NotExpression) o;
    return Objects.equals(value, that.value);
  }

  @Override
  public int hashCode() {
    return value.hashCode();
  }
}
