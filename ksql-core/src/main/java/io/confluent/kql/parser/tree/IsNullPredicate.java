/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.kql.parser.tree;

import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class IsNullPredicate
    extends Expression {

  private final Expression value;

  public IsNullPredicate(Expression value) {
    this(Optional.empty(), value);
  }

  public IsNullPredicate(NodeLocation location, Expression value) {
    this(Optional.of(location), value);
  }

  private IsNullPredicate(Optional<NodeLocation> location, Expression value) {
    super(location);
    requireNonNull(value, "value is null");
    this.value = value;
  }

  public Expression getValue() {
    return value;
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitIsNullPredicate(this, context);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    IsNullPredicate that = (IsNullPredicate) o;
    return Objects.equals(value, that.value);
  }

  @Override
  public int hashCode() {
    return value.hashCode();
  }
}
