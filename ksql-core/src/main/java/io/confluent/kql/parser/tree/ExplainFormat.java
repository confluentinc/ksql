/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.kql.parser.tree;

import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class ExplainFormat
    extends ExplainOption {

  public enum Type {
    TEXT,
    GRAPHVIZ
  }

  private final Type type;

  public ExplainFormat(Type type) {
    this(Optional.empty(), type);
  }

  public ExplainFormat(NodeLocation location, Type type) {
    this(Optional.of(location), type);
  }

  private ExplainFormat(Optional<NodeLocation> location, Type type) {
    super(location);
    this.type = requireNonNull(type, "type is null");
  }

  public Type getType() {
    return type;
  }

  @Override
  public int hashCode() {
    return Objects.hash(type);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if ((obj == null) || (getClass() != obj.getClass())) {
      return false;
    }
    ExplainFormat o = (ExplainFormat) obj;
    return Objects.equals(type, o.type);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("type", type)
        .toString();
  }
}
