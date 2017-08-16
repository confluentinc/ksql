/**
 * Copyright 2017 Confluent Inc.
 **/

package io.confluent.ksql.parser.tree;

import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class FrameBound
    extends Node {

  public enum Type {
    UNBOUNDED_PRECEDING,
    PRECEDING,
    CURRENT_ROW,
    FOLLOWING,
    UNBOUNDED_FOLLOWING
  }

  private final Type type;
  private final Optional<Expression> value;

  public FrameBound(Type type) {
    this(Optional.empty(), type);
  }

  public FrameBound(NodeLocation location, Type type) {
    this(Optional.of(location), type);
  }

  public FrameBound(Type type, Expression value) {
    this(Optional.empty(), type, value);
  }

  private FrameBound(Optional<NodeLocation> location, Type type) {
    this(location, type, null);
  }

  public FrameBound(NodeLocation location, Type type, Expression value) {
    this(Optional.of(location), type, value);
  }

  private FrameBound(Optional<NodeLocation> location, Type type, Expression value) {
    super(location);
    this.type = requireNonNull(type, "type is null");
    this.value = Optional.ofNullable(value);
  }

  public Type getType() {
    return type;
  }

  public Optional<Expression> getValue() {
    return value;
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitFrameBound(this, context);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if ((obj == null) || (getClass() != obj.getClass())) {
      return false;
    }
    FrameBound o = (FrameBound) obj;
    return Objects.equals(type, o.type)
           && Objects.equals(value, o.value);
  }

  @Override
  public int hashCode() {
    return Objects.hash(type, value);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("type", type)
        .add("value", value)
        .toString();
  }
}
