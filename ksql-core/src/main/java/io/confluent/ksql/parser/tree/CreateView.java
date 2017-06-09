/**
 * Copyright 2017 Confluent Inc.
 *
 **/

package io.confluent.ksql.parser.tree;

import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class CreateView
    extends Statement {

  private final QualifiedName name;
  private final Query query;
  private final boolean replace;

  public CreateView(QualifiedName name, Query query, boolean replace) {
    this(Optional.empty(), name, query, replace);
  }

  public CreateView(NodeLocation location, QualifiedName name, Query query, boolean replace) {
    this(Optional.of(location), name, query, replace);
  }

  private CreateView(Optional<NodeLocation> location, QualifiedName name, Query query,
                     boolean replace) {
    super(location);
    this.name = requireNonNull(name, "name is null");
    this.query = requireNonNull(query, "query is null");
    this.replace = replace;
  }

  public QualifiedName getName() {
    return name;
  }

  public Query getQuery() {
    return query;
  }

  public boolean isReplace() {
    return replace;
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitCreateView(this, context);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, query, replace);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if ((obj == null) || (getClass() != obj.getClass())) {
      return false;
    }
    CreateView o = (CreateView) obj;
    return Objects.equals(name, o.name)
           && Objects.equals(query, o.query)
           && Objects.equals(replace, o.replace);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("name", name)
        .add("query", query)
        .add("replace", replace)
        .toString();
  }
}
