/**
 * Copyright 2017 Confluent Inc.
 **/

package io.confluent.ksql.parser.tree;

import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;

public class SetSession
    extends Statement {

  private final QualifiedName name;
  private final Expression value;

  public SetSession(QualifiedName name, Expression value) {
    this(Optional.empty(), name, value);
  }

  public SetSession(NodeLocation location, QualifiedName name, Expression value) {
    this(Optional.of(location), name, value);
  }

  private SetSession(Optional<NodeLocation> location, QualifiedName name, Expression value) {
    super(location);
    this.name = name;
    this.value = value;
  }

  public QualifiedName getName() {
    return name;
  }

  public Expression getValue() {
    return value;
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitSetSession(this, context);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, value);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if ((obj == null) || (getClass() != obj.getClass())) {
      return false;
    }
    SetSession o = (SetSession) obj;
    return Objects.equals(name, o.name)
           && Objects.equals(value, o.value);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("name", name)
        .add("value", value)
        .toString();
  }
}
