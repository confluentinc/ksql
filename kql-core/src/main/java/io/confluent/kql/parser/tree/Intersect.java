/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.kql.parser.tree;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class Intersect
    extends SetOperation {

  private final List<Relation> relations;

  public Intersect(List<Relation> relations, boolean distinct) {
    this(Optional.empty(), relations, distinct);
  }

  public Intersect(NodeLocation location, List<Relation> relations, boolean distinct) {
    this(Optional.of(location), relations, distinct);
  }

  private Intersect(Optional<NodeLocation> location, List<Relation> relations, boolean distinct) {
    super(location, distinct);
    requireNonNull(relations, "relations is null");

    this.relations = ImmutableList.copyOf(relations);
  }

  public List<Relation> getRelations() {
    return relations;
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitIntersect(this, context);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("relations", relations)
        .add("distinct", isDistinct())
        .toString();
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if ((obj == null) || (getClass() != obj.getClass())) {
      return false;
    }
    Intersect o = (Intersect) obj;
    return Objects.equals(relations, o.relations) &&
           Objects.equals(isDistinct(), o.isDistinct());
  }

  @Override
  public int hashCode() {
    return Objects.hash(relations, isDistinct());
  }
}
