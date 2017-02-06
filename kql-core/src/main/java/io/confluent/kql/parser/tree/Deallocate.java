/**
 * Copyright 2017 Confluent Inc.
 *
 **/
package io.confluent.kql.parser.tree;

import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class Deallocate
    extends Statement {

  private final String name;

  public Deallocate(NodeLocation location, String name) {
    this(Optional.of(location), name);
  }

  public Deallocate(String name) {
    this(Optional.empty(), name);
  }

  private Deallocate(Optional<NodeLocation> location, String name) {
    super(location);
    this.name = requireNonNull(name, "name is null");
  }

  public String getName() {
    return name;
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitDeallocate(this, context);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if ((obj == null) || (getClass() != obj.getClass())) {
      return false;
    }
    Deallocate o = (Deallocate) obj;
    return Objects.equals(name, o.name);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("name", name)
        .toString();
  }
}
