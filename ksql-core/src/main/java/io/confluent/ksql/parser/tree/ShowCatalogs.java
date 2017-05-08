/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.ksql.parser.tree;

import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public final class ShowCatalogs
    extends Statement {

  private final Optional<String> likePattern;

  public ShowCatalogs(Optional<String> likePattern) {
    this(Optional.empty(), likePattern);
  }

  public ShowCatalogs(NodeLocation location, Optional<String> likePattern) {
    this(Optional.of(location), likePattern);
  }

  public ShowCatalogs(Optional<NodeLocation> location, Optional<String> likePattern) {
    super(location);
    this.likePattern = requireNonNull(likePattern, "likePattern is null");
  }

  public Optional<String> getLikePattern() {
    return likePattern;
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitShowCatalogs(this, context);
  }

  @Override
  public int hashCode() {
    return 0;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    return (obj != null) && (getClass() == obj.getClass());
  }

  @Override
  public String toString() {
    return toStringHelper(this).toString();
  }
}
