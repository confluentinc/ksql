/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.kql.parser.tree;

import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class SubscriptExpression
    extends Expression {

  private final Expression base;
  private final Expression index;

  public SubscriptExpression(Expression base, Expression index) {
    this(Optional.empty(), base, index);
  }

  public SubscriptExpression(NodeLocation location, Expression base, Expression index) {
    this(Optional.of(location), base, index);
  }

  private SubscriptExpression(Optional<NodeLocation> location, Expression base, Expression index) {
    super(location);
    this.base = requireNonNull(base, "base is null");
    this.index = requireNonNull(index, "index is null");
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitSubscriptExpression(this, context);
  }

  public Expression getBase() {
    return base;
  }

  public Expression getIndex() {
    return index;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    SubscriptExpression that = (SubscriptExpression) o;

    return Objects.equals(this.base, that.base) && Objects.equals(this.index, that.index);
  }

  @Override
  public int hashCode() {
    return Objects.hash(base, index);
  }
}
