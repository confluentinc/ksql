/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.ksql.parser.tree;

import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class ShowColumns
    extends Statement {

  private final QualifiedName table;

  public ShowColumns(QualifiedName table) {
    this(Optional.empty(), table);
  }

  public ShowColumns(NodeLocation location, QualifiedName table) {
    this(Optional.of(location), table);
  }

  private ShowColumns(Optional<NodeLocation> location, QualifiedName table) {
    super(location);
    this.table = requireNonNull(table, "table is null");
  }

  public QualifiedName getTable() {
    return table;
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitShowColumns(this, context);
  }

  @Override
  public int hashCode() {
    return Objects.hash(table);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if ((obj == null) || (getClass() != obj.getClass())) {
      return false;
    }
    ShowColumns o = (ShowColumns) obj;
    return Objects.equals(table, o.table);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("table", table)
        .toString();
  }
}
