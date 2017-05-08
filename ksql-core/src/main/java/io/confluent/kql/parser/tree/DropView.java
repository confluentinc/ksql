/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.kql.parser.tree;

import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;

public class DropView
    extends Statement {

  private final QualifiedName name;
  private final boolean exists;

  public DropView(QualifiedName name, boolean exists) {
    this(Optional.empty(), name, exists);
  }

  public DropView(NodeLocation location, QualifiedName name, boolean exists) {
    this(Optional.of(location), name, exists);
  }

  private DropView(Optional<NodeLocation> location, QualifiedName name, boolean exists) {
    super(location);
    this.name = name;
    this.exists = exists;
  }

  public QualifiedName getName() {
    return name;
  }

  public boolean isExists() {
    return exists;
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitDropView(this, context);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, exists);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if ((obj == null) || (getClass() != obj.getClass())) {
      return false;
    }
    DropView o = (DropView) obj;
    return Objects.equals(name, o.name)
           && (exists == o.exists);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("name", name)
        .add("exists", exists)
        .toString();
  }
}
