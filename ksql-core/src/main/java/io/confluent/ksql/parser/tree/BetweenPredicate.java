/**
 * Copyright 2017 Confluent Inc.
 *
 **/

package io.confluent.ksql.parser.tree;

import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class BetweenPredicate
    extends Expression {

  private final Expression value;
  private final Expression min;
  private final Expression max;

  public BetweenPredicate(Expression value, Expression min, Expression max) {
    this(Optional.empty(), value, min, max);
  }

  public BetweenPredicate(NodeLocation location, Expression value, Expression min, Expression max) {
    this(Optional.of(location), value, min, max);
  }

  private BetweenPredicate(Optional<NodeLocation> location, Expression value, Expression min,
                           Expression max) {
    super(location);
    requireNonNull(value, "value is null");
    requireNonNull(min, "min is null");
    requireNonNull(max, "max is null");

    this.value = value;
    this.min = min;
    this.max = max;
  }

  public Expression getValue() {
    return value;
  }

  public Expression getMin() {
    return min;
  }

  public Expression getMax() {
    return max;
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitBetweenPredicate(this, context);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    BetweenPredicate that = (BetweenPredicate) o;
    return Objects.equals(value, that.value) &&
           Objects.equals(min, that.min) &&
           Objects.equals(max, that.max);
  }

  @Override
  public int hashCode() {
    return Objects.hash(value, min, max);
  }
}
