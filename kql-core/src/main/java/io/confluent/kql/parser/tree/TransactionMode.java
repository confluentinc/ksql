/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.kql.parser.tree;

import java.util.Optional;

public abstract class TransactionMode
    extends Node {

  public TransactionMode(Optional<NodeLocation> location) {
    super(location);
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitTransactionMode(this, context);
  }
}
