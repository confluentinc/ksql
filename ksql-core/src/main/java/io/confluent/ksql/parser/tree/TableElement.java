/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.ksql.parser.tree;

import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public final class TableElement
    extends Node {

  private final String name;
  private final String type;

  public TableElement(String name, String type) {
    this(Optional.empty(), name, type);
  }

  public TableElement(NodeLocation location, String name, String type) {
    this(Optional.of(location), name, type);
  }

  private TableElement(Optional<NodeLocation> location, String name, String type) {
    super(location);
    this.name = requireNonNull(name, "name is null");
    this.type = requireNonNull(type, "type is null");
  }

  public String getName() {
    return name;
  }

  public String getType() {
    return type;
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitTableElement(this, context);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    TableElement o = (TableElement) obj;
    return Objects.equals(this.name, o.name) &&
           Objects.equals(this.type, o.type);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, type);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("name", name)
        .add("type", type)
        .toString();
  }
}
