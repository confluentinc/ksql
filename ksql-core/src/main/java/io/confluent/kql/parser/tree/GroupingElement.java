/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.kql.parser.tree;

import java.util.List;
import java.util.Optional;
import java.util.Set;

public abstract class GroupingElement
    extends Node {

  public GroupingElement(Optional<NodeLocation> location) {
    super(location);
  }

  public abstract List<Set<Expression>> enumerateGroupingSets();

  @Override
  protected <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitGroupingElement(this, context);
  }
}
