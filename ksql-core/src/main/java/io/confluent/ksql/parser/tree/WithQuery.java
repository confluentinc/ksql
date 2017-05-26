/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.ksql.parser.tree;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class WithQuery
    extends Node {

  private final String name;
  private final Query query;
  private final Optional<List<String>> columnNames;

  public WithQuery(String name, Query query, Optional<List<String>> columnNames) {
    this(Optional.empty(), name, query, columnNames);
  }

  public WithQuery(NodeLocation location, String name, Query query,
                   Optional<List<String>> columnNames) {
    this(Optional.of(location), name, query, columnNames);
  }

  private WithQuery(Optional<NodeLocation> location, String name, Query query,
                    Optional<List<String>> columnNames) {
    super(location);
    this.name = QualifiedName.of(requireNonNull(name, "name is null")).getParts().get(0);
    this.query = requireNonNull(query, "query is null");
    this.columnNames = requireNonNull(columnNames, "columnNames is null");
  }

  public String getName() {
    return name;
  }

  public Query getQuery() {
    return query;
  }

  public Optional<List<String>> getColumnNames() {
    return columnNames;
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitWithQuery(this, context);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("name", name)
        .add("query", query)
        .add("columnNames", columnNames)
        .omitNullValues()
        .toString();
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, query, columnNames);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if ((obj == null) || (getClass() != obj.getClass())) {
      return false;
    }
    WithQuery o = (WithQuery) obj;
    return Objects.equals(name, o.name) &&
           Objects.equals(query, o.query) &&
           Objects.equals(columnNames, o.columnNames);
  }
}
