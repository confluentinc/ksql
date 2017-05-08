/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.ksql.parser.tree;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class With
    extends Node {

  private final boolean recursive;
  private final List<WithQuery> queries;

  public With(boolean recursive, List<WithQuery> queries) {
    this(Optional.empty(), recursive, queries);
  }

  public With(NodeLocation location, boolean recursive, List<WithQuery> queries) {
    this(Optional.of(location), recursive, queries);
  }

  private With(Optional<NodeLocation> location, boolean recursive, List<WithQuery> queries) {
    super(location);
    requireNonNull(queries, "queries is null");
    checkArgument(!queries.isEmpty(), "queries is empty");

    this.recursive = recursive;
    this.queries = ImmutableList.copyOf(queries);
  }

  public boolean isRecursive() {
    return recursive;
  }

  public List<WithQuery> getQueries() {
    return queries;
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitWith(this, context);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if ((obj == null) || (getClass() != obj.getClass())) {
      return false;
    }
    With o = (With) obj;
    return Objects.equals(recursive, o.recursive) &&
           Objects.equals(queries, o.queries);
  }

  @Override
  public int hashCode() {
    return Objects.hash(recursive, queries);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("recursive", recursive)
        .add("queries", queries)
        .toString();
  }
}
