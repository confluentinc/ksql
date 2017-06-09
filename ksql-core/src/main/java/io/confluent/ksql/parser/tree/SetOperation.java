/**
 * Copyright 2017 Confluent Inc.
 **/

package io.confluent.ksql.parser.tree;

import java.util.List;
import java.util.Optional;

public abstract class SetOperation
    extends QueryBody {

  private final boolean distinct;

  protected SetOperation(Optional<NodeLocation> location, boolean distinct) {
    super(location);
    this.distinct = distinct;
  }

  public boolean isDistinct() {
    return distinct;
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitSetOperation(this, context);
  }

  public abstract List<Relation> getRelations();
}
