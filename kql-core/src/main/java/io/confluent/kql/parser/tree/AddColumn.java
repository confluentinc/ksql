/**
 * Copyright 2017 Confluent Inc.
 *
 **/

package io.confluent.kql.parser.tree;

import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class AddColumn
    extends Statement {

  private final QualifiedName name;
  private final TableElement column;

  public AddColumn(QualifiedName name, TableElement column) {
    this(Optional.empty(), name, column);
  }

  public AddColumn(NodeLocation location, QualifiedName name, TableElement column) {
    this(Optional.of(location), name, column);
  }

  private AddColumn(Optional<NodeLocation> location, QualifiedName name, TableElement column) {
    super(location);
    this.name = requireNonNull(name, "table is null");
    this.column = requireNonNull(column, "column is null");
  }

  public QualifiedName getName() {
    return name;
  }

  public TableElement getColumn() {
    return column;
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitAddColumn(this, context);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, column);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if ((obj == null) || (getClass() != obj.getClass())) {
      return false;
    }
    AddColumn o = (AddColumn) obj;
    return Objects.equals(name, o.name) &&
           Objects.equals(column, o.column);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("name", name)
        .add("column", column)
        .toString();
  }
}
