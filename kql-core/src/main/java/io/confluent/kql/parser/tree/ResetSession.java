/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.kql.parser.tree;

import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;

public class ResetSession
    extends Statement {

  private final QualifiedName name;

  public ResetSession(QualifiedName name) {
    this(Optional.empty(), name);
  }

  public ResetSession(NodeLocation location, QualifiedName name) {
    this(Optional.of(location), name);
  }

  private ResetSession(Optional<NodeLocation> location, QualifiedName name) {
    super(location);
    this.name = name;
  }

  public QualifiedName getName() {
    return name;
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitResetSession(this, context);
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
    ResetSession o = (ResetSession) obj;
    return Objects.equals(name, o.name);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("name", name)
        .toString();
  }
}
