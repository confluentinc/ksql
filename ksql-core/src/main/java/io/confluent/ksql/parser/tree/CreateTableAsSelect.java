/**
 * Copyright 2017 Confluent Inc.
 *
 **/
package io.confluent.ksql.parser.tree;

import com.google.common.collect.ImmutableMap;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class CreateTableAsSelect
    extends Statement {

  private final QualifiedName name;
  private final Query query;
  private final boolean notExists;
  private final Map<String, Expression> properties;

  public CreateTableAsSelect(QualifiedName name, Query query, boolean notExists,
                             Map<String, Expression> properties) {
    this(Optional.empty(), name, query, notExists, properties);
  }

  public CreateTableAsSelect(NodeLocation location, QualifiedName name, Query query,
                             boolean notExists, Map<String, Expression> properties
                             ) {
    this(Optional.of(location), name, query, notExists, properties);
  }

  private CreateTableAsSelect(Optional<NodeLocation> location, QualifiedName name, Query query,
                              boolean notExists, Map<String, Expression> properties
                              ) {
    super(location);
    this.name = requireNonNull(name, "name is null");
    this.query = requireNonNull(query, "query is null");
    this.notExists = notExists;
    this.properties = ImmutableMap.copyOf(requireNonNull(properties, "properties is null"));
  }

  public QualifiedName getName() {
    return name;
  }

  public Query getQuery() {
    return query;
  }

  public boolean isNotExists() {
    return notExists;
  }

  public Map<String, Expression> getProperties() {
    return properties;
  }


  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitCreateTableAsSelect(this, context);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, query, properties);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if ((obj == null) || (getClass() != obj.getClass())) {
      return false;
    }
    CreateTableAsSelect o = (CreateTableAsSelect) obj;
    return Objects.equals(name, o.name)
           && Objects.equals(query, o.query)
           && Objects.equals(notExists, o.notExists)
           && Objects.equals(properties, o.properties);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("name", name)
        .add("query", query)
        .add("notExists", notExists)
        .add("properties", properties)
        .toString();
  }
}
