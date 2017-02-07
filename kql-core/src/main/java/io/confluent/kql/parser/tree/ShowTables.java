/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.kql.parser.tree;

import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class ShowTables
    extends Statement {

  private final Optional<QualifiedName> schema;
  private final Optional<String> likePattern;

  public ShowTables(Optional<QualifiedName> schema, Optional<String> likePattern) {
    this(Optional.empty(), schema, likePattern);
  }

  public ShowTables(NodeLocation location, Optional<QualifiedName> schema,
                    Optional<String> likePattern) {
    this(Optional.of(location), schema, likePattern);
  }

  private ShowTables(Optional<NodeLocation> location, Optional<QualifiedName> schema,
                     Optional<String> likePattern) {
    super(location);
    requireNonNull(schema, "schema is null");
    requireNonNull(likePattern, "likePattern is null");

    this.schema = schema;
    this.likePattern = likePattern;
  }

  public Optional<QualifiedName> getSchema() {
    return schema;
  }

  public Optional<String> getLikePattern() {
    return likePattern;
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitShowTables(this, context);
  }

  @Override
  public int hashCode() {
    return Objects.hash(schema, likePattern);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if ((obj == null) || (getClass() != obj.getClass())) {
      return false;
    }
    ShowTables o = (ShowTables) obj;
    return Objects.equals(schema, o.schema) &&
           Objects.equals(likePattern, o.likePattern);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("schema", schema)
        .add("likePattern", likePattern)
        .toString();
  }
}
