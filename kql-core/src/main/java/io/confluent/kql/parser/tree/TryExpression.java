/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.kql.parser.tree;

import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class TryExpression
    extends Expression {

  private final Expression innerExpression;

  public TryExpression(Expression innerExpression) {
    this(Optional.empty(), innerExpression);
  }

  public TryExpression(NodeLocation location, Expression innerExpression) {
    this(Optional.of(location), innerExpression);
  }

  private TryExpression(Optional<NodeLocation> location, Expression innerExpression) {
    super(location);
    this.innerExpression = requireNonNull(innerExpression, "innerExpression is null");
  }

  public Expression getInnerExpression() {
    return innerExpression;
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitTryExpression(this, context);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if ((obj == null) || (getClass() != obj.getClass())) {
      return false;
    }
    TryExpression o = (TryExpression) obj;
    return Objects.equals(innerExpression, o.innerExpression);
  }

  @Override
  public int hashCode() {
    return Objects.hash(innerExpression);
  }
}
