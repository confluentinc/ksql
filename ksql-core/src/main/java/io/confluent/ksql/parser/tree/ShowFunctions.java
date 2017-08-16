/**
 * Copyright 2017 Confluent Inc.
 **/

package io.confluent.ksql.parser.tree;

import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;

public class ShowFunctions
    extends Statement {

  public ShowFunctions() {
    this(Optional.empty());
  }

  public ShowFunctions(NodeLocation location) {
    this(Optional.of(location));
  }

  private ShowFunctions(Optional<NodeLocation> location) {
    super(location);
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitShowFunctions(this, context);
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
    return (obj != null) && (getClass() == obj.getClass());
  }

  @Override
  public String toString() {
    return toStringHelper(this).toString();
  }
}
