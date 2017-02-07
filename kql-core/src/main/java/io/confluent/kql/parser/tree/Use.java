/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.kql.parser.tree;

import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public final class Use
    extends Statement {

  private final Optional<String> catalog;
  private final String schema;

  public Use(Optional<String> catalog, String schema) {
    this(Optional.empty(), catalog, schema);
  }

  public Use(NodeLocation location, Optional<String> catalog, String schema) {
    this(Optional.of(location), catalog, schema);
  }

  private Use(Optional<NodeLocation> location, Optional<String> catalog, String schema) {
    super(location);
    requireNonNull(catalog, "catalog is null");
    requireNonNull(schema, "schema is null");
    this.catalog = catalog;
    this.schema = schema;
  }

  public Optional<String> getCatalog() {
    return catalog;
  }

  public String getSchema() {
    return schema;
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitUse(this, context);
  }

  @Override
  public int hashCode() {
    return Objects.hash(catalog, schema);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    Use use = (Use) o;

    if (!catalog.equals(use.catalog)) {
      return false;
    }
    if (!schema.equals(use.schema)) {
      return false;
    }

    return true;
  }

  @Override
  public String toString() {
    return toStringHelper(this).toString();
  }
}
