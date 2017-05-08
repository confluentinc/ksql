/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.kql.parser.tree;

import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public final class RenameTable
    extends Statement {

  private final QualifiedName source;
  private final QualifiedName target;

  public RenameTable(QualifiedName source, QualifiedName target) {
    this(Optional.empty(), source, target);
  }

  public RenameTable(NodeLocation location, QualifiedName source, QualifiedName target) {
    this(Optional.of(location), source, target);
  }

  private RenameTable(Optional<NodeLocation> location, QualifiedName source, QualifiedName target) {
    super(location);
    this.source = requireNonNull(source, "source name is null");
    this.target = requireNonNull(target, "target name is null");
  }

  public QualifiedName getSource() {
    return source;
  }

  public QualifiedName getTarget() {
    return target;
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitRenameTable(this, context);
  }

  @Override
  public int hashCode() {
    return Objects.hash(source, target);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if ((obj == null) || (getClass() != obj.getClass())) {
      return false;
    }
    RenameTable o = (RenameTable) obj;
    return Objects.equals(source, o.source) &&
           Objects.equals(target, o.target);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("source", source)
        .add("target", target)
        .toString();
  }
}
