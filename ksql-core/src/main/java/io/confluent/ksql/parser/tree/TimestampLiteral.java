/**
 * Copyright 2017 Confluent Inc.
 **/

package io.confluent.ksql.parser.tree;

import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class TimestampLiteral
    extends Literal {

  private final String value;

  public TimestampLiteral(String value) {
    this(Optional.empty(), value);
  }

  public TimestampLiteral(NodeLocation location, String value) {
    this(Optional.of(location), value);
  }

  private TimestampLiteral(Optional<NodeLocation> location, String value) {
    super(location);
    requireNonNull(value, "value is null");

    this.value = value;
  }

  public String getValue() {
    return value;
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitTimestampLiteral(this, context);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    TimestampLiteral that = (TimestampLiteral) o;
    return Objects.equals(value, that.value);
  }

  @Override
  public int hashCode() {
    return value.hashCode();
  }
}
