/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.ksql.parser.tree;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;

public class Table
    extends QueryBody {

  public final boolean isSTDOut;
  Map<String, Expression> properties;
  private final QualifiedName name;

  public Table(QualifiedName name) {
    this(Optional.empty(), name, false);
  }

  public Table(QualifiedName name, boolean isSTDOut) {
    this(Optional.empty(), name, isSTDOut);
  }

  public Table(NodeLocation location, QualifiedName name) {
    this(Optional.of(location), name, false);
  }

  public Table(NodeLocation location, QualifiedName name, boolean isSTDOut) {
    this(Optional.of(location), name, isSTDOut);
  }

  private Table(Optional<NodeLocation> location, QualifiedName name, boolean isSTDOut) {
    super(location);
    this.name = name;
    this.isSTDOut = isSTDOut;
  }

  public QualifiedName getName() {
    return name;
  }

  public boolean isSTDOut() {
    return isSTDOut;
  }

  public Map<String, Expression> getProperties() {
    return properties;
  }

  public void setProperties(
      Map<String, Expression> properties) {
    this.properties = properties;
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitTable(this, context);
  }

  @Override
  public String toString() {
    return name.toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    Table table = (Table) o;
    return Objects.equals(name, table.name);
  }

  @Override
  public int hashCode() {
    return name.hashCode();
  }
}
