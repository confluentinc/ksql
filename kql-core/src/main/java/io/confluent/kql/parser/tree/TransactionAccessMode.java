/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.kql.parser.tree;

import com.google.common.base.MoreObjects;

import java.util.Objects;
import java.util.Optional;

public final class TransactionAccessMode
    extends TransactionMode {

  private final boolean readOnly;

  public TransactionAccessMode(boolean readOnly) {
    this(Optional.empty(), readOnly);
  }

  public TransactionAccessMode(NodeLocation location, boolean readOnly) {
    this(Optional.of(location), readOnly);
  }

  private TransactionAccessMode(Optional<NodeLocation> location, boolean readOnly) {
    super(location);
    this.readOnly = readOnly;
  }

  public boolean isReadOnly() {
    return readOnly;
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitTransactionAccessMode(this, context);
  }

  @Override
  public int hashCode() {
    return Objects.hash(readOnly);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    final TransactionAccessMode other = (TransactionAccessMode) obj;
    return this.readOnly == other.readOnly;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("readOnly", readOnly)
        .toString();
  }
}
