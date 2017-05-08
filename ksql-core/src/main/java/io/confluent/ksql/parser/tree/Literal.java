/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.ksql.parser.tree;

import java.util.Optional;

public abstract class Literal
    extends Expression {

  protected Literal(Optional<NodeLocation> location) {
    super(location);
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitLiteral(this, context);
  }
}
