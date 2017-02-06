/**
 * Copyright 2017 Confluent Inc.
 *
 **/
package io.confluent.kql.parser.tree;

import java.util.Optional;

public final class Commit
    extends Statement {

  public Commit() {
    this(Optional.empty());
  }

  public Commit(NodeLocation location) {
    this(Optional.of(location));
  }

  private Commit(Optional<NodeLocation> location) {
    super(location);
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitCommit(this, context);
  }

  @Override
  public int hashCode() {
    return 0;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if ((obj == null) || (getClass() != obj.getClass())) {
      return false;
    }
    return true;
  }

  @Override
  public String toString() {
    return "COMMIT";
  }
}
