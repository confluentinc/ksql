/**
 * Copyright 2017 Confluent Inc.
 *
 **/

package io.confluent.ksql.parser.tree;

import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class DecimalLiteral
    extends Literal {

  private final String value;

  public DecimalLiteral(String value) {
    this(Optional.empty(), value);
  }

  public DecimalLiteral(NodeLocation location, String value) {
    this(Optional.of(location), value);
  }

  public DecimalLiteral(Optional<NodeLocation> location, String value) {
    super(location);
    this.value = requireNonNull(value, "value is null");
  }

  public String getValue() {
    return value;
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitDecimalLiteral(this, context);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    DecimalLiteral that = (DecimalLiteral) o;
    return Objects.equals(value, that.value);
  }

  @Override
  public int hashCode() {
    return Objects.hash(value);
  }
}
