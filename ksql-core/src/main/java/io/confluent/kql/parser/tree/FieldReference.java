/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.kql.parser.tree;

import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;

public class FieldReference
    extends Expression {

  private final int fieldIndex;

  public FieldReference(int fieldIndex) {
    super(Optional.empty());
    checkArgument(fieldIndex >= 0, "fieldIndex must be >= 0");

    this.fieldIndex = fieldIndex;
  }

  public int getFieldIndex() {
    return fieldIndex;
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitFieldReference(this, context);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    FieldReference that = (FieldReference) o;

    return fieldIndex == that.fieldIndex;
  }

  @Override
  public int hashCode() {
    return fieldIndex;
  }
}
