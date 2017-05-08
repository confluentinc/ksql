/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.kql.parser.tree;

import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class ShowSchemas
    extends Statement {

  private final Optional<String> catalog;
  private final Optional<String> likePattern;

  public ShowSchemas(Optional<String> catalog, Optional<String> likePattern) {
    this(Optional.empty(), catalog, likePattern);
  }

  public ShowSchemas(NodeLocation location, Optional<String> catalog,
                     Optional<String> likePattern) {
    this(Optional.of(location), catalog, likePattern);
  }

  private ShowSchemas(Optional<NodeLocation> location, Optional<String> catalog,
                      Optional<String> likePattern) {
    super(location);
    this.catalog = requireNonNull(catalog, "catalog is null");
    this.likePattern = requireNonNull(likePattern, "likePattern is null");
  }

  public Optional<String> getCatalog() {
    return catalog;
  }

  public Optional<String> getLikePattern() {
    return likePattern;
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitShowSchemas(this, context);
  }

  @Override
  public int hashCode() {
    return catalog.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if ((obj == null) || (getClass() != obj.getClass())) {
      return false;
    }
    ShowSchemas o = (ShowSchemas) obj;
    return Objects.equals(catalog, o.catalog);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("catalog", catalog)
        .toString();
  }
}
