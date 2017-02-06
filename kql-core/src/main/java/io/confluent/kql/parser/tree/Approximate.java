/**
 * Copyright 2017 Confluent Inc.
 *
 **/

package io.confluent.kql.parser.tree;

import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public final class Approximate
    extends Node {

  private final String confidence;

  public Approximate(NodeLocation location, String confidence) {
    super(Optional.of(location));
    this.confidence = requireNonNull(confidence, "confidence is null");
  }

  public String getConfidence() {
    return confidence;
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitApproximate(this, context);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if ((obj == null) || (getClass() != obj.getClass())) {
      return false;
    }
    Approximate o = (Approximate) obj;
    return Objects.equals(confidence, o.confidence);
  }

  @Override
  public int hashCode() {
    return confidence.hashCode();
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("confidence", confidence)
        .toString();
  }
}
