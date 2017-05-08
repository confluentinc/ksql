/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.ksql.parser.tree;

import io.confluent.ksql.parser.ParsingException;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class LongLiteral
    extends Literal {

  private final long value;

  public LongLiteral(String value) {
    this(Optional.empty(), value);
  }

  public LongLiteral(NodeLocation location, String value) {
    this(Optional.of(location), value);
  }

  private LongLiteral(Optional<NodeLocation> location, String value) {
    super(location);
    requireNonNull(value, "value is null");
    try {
      this.value = Long.parseLong(value);
    } catch (NumberFormatException e) {
      throw new ParsingException("Invalid numeric literal: " + value);
    }
  }

  public long getValue() {
    return value;
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitLongLiteral(this, context);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    LongLiteral that = (LongLiteral) o;

    if (value != that.value) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    return (int) (value ^ (value >>> 32));
  }
}
