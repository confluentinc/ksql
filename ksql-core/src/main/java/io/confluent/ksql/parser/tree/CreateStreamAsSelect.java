/**
 * Copyright 2017 Confluent Inc.
 **/

package io.confluent.ksql.parser.tree;

import com.google.common.collect.ImmutableMap;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class CreateStreamAsSelect extends Statement {

  private final QualifiedName name;
  private final Query query;
  private final boolean notExists;
  private final Map<String, Expression> properties;
  private final Optional<Expression> partitionByColumn;

  public CreateStreamAsSelect(QualifiedName name, Query query, boolean notExists,
                      Map<String, Expression> properties, Optional<Expression> partitionByColumn) {
    this(Optional.empty(), name, query, notExists, properties, partitionByColumn);
  }

  public CreateStreamAsSelect(NodeLocation location, QualifiedName name, Query query,
                      boolean notExists, Map<String, Expression> properties,
                              Optional<Expression> partitionByColumn) {
    this(Optional.of(location), name, query, notExists, properties, partitionByColumn);
  }

  private CreateStreamAsSelect(Optional<NodeLocation> location, QualifiedName name,
                               Query query, boolean notExists,
                       Map<String, Expression> properties, Optional<Expression> partitionByColumn) {
    super(location);
    this.name = requireNonNull(name, "stream is null");
    this.query = query;
    this.notExists = notExists;
    this.properties = ImmutableMap.copyOf(
        requireNonNull(properties, "properties is null"));
    this.partitionByColumn = partitionByColumn;
  }

  public QualifiedName getName() {
    return name;
  }

  public boolean isNotExists() {
    return notExists;
  }

  public Query getQuery() {
    return query;
  }

  public Map<String, Expression> getProperties() {
    return properties;
  }

  public Optional<Expression> getPartitionByColumn() {
    return partitionByColumn;
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitCreateStreamAsSelect(this, context);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, query, notExists, properties);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if ((obj == null) || (getClass() != obj.getClass())) {
      return false;
    }
    CreateStreamAsSelect o = (CreateStreamAsSelect) obj;
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
