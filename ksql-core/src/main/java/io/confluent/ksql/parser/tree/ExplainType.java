/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.ksql.parser.tree;

import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class ExplainType
    extends ExplainOption {

  public enum Type {
    LOGICAL,
    DISTRIBUTED
  }

  private final Type type;

  public ExplainType(Type type) {
    this(Optional.empty(), type);
  }

  public ExplainType(NodeLocation location, Type type) {
    this(Optional.of(location), type);
  }

  private ExplainType(Optional<NodeLocation> location, Type type) {
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
    ExplainType o = (ExplainType) obj;
    return Objects.equals(type, o.type);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("type", type)
        .toString();
  }
}
