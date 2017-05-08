/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.ksql.parser.tree;

import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class ShowCreate
    extends Statement {

  public enum Type {
    TABLE,
    VIEW
  }

  private final Type type;
  private final QualifiedName name;

  public ShowCreate(Type type, QualifiedName name) {
    this(Optional.empty(), type, name);
  }

  public ShowCreate(NodeLocation location, Type type, QualifiedName name) {
    this(Optional.of(location), type, name);
  }

  private ShowCreate(Optional<NodeLocation> location, Type type, QualifiedName name) {
    super(location);
    this.type = requireNonNull(type, "type is null");
    this.name = requireNonNull(name, "name is null");
  }

  public QualifiedName getName() {
    return name;
  }

  public Type getType() {
    return type;
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitShowCreate(this, context);
  }

  @Override
  public int hashCode() {
    return Objects.hash(type, name);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if ((obj == null) || (getClass() != obj.getClass())) {
      return false;
    }
    ShowCreate o = (ShowCreate) obj;
    return Objects.equals(name, o.name) && Objects.equals(type, o.type);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("type", type)
        .add("name", name)
        .toString();
  }
}
