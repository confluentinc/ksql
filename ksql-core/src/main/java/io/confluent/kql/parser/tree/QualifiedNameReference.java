/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.kql.parser.tree;

import java.util.Objects;
import java.util.Optional;

public class QualifiedNameReference
    extends Expression {

  private final QualifiedName name;

  public QualifiedNameReference(QualifiedName name) {
    this(Optional.empty(), name);
  }

  public QualifiedNameReference(NodeLocation location, QualifiedName name) {
    this(Optional.of(location), name);
  }

  private QualifiedNameReference(Optional<NodeLocation> location, QualifiedName name) {
    super(location);
    this.name = name;
  }

  public QualifiedName getName() {
    return name;
  }

  public QualifiedName getSuffix() {
    return QualifiedName.of(name.getSuffix());
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitQualifiedNameReference(this, context);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    QualifiedNameReference that = (QualifiedNameReference) o;
    return Objects.equals(name, that.name);
  }

  @Override
  public int hashCode() {
    return name.hashCode();
  }
}
