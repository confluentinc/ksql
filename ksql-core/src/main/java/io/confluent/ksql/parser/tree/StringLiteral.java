/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.ksql.parser.tree;

import io.airlift.slice.Slice;

import java.util.Objects;
import java.util.Optional;

import static io.airlift.slice.Slices.utf8Slice;
import static java.util.Objects.requireNonNull;

public class StringLiteral
    extends Literal {

  private final String value;
  private final Slice slice;

  public StringLiteral(String value) {
    this(Optional.empty(), value);
  }

  public StringLiteral(NodeLocation location, String value) {
    this(Optional.of(location), value);
  }

  private StringLiteral(Optional<NodeLocation> location, String value) {
    super(location);
    requireNonNull(value, "value is null");
    this.value = value;
    this.slice = utf8Slice(value);
  }

  public String getValue() {
    return value;
  }

  public Slice getSlice() {
    return slice;
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitStringLiteral(this, context);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    StringLiteral that = (StringLiteral) o;
    return Objects.equals(value, that.value);
  }

  @Override
  public int hashCode() {
    return value.hashCode();
  }
}
