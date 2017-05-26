/**
 * Copyright 2017 Confluent Inc.
 *
 **/
package io.confluent.ksql.parser.tree;

import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class Delete
    extends Statement {

  private final Table table;
  private final Optional<Expression> where;

  public Delete(Table table, Optional<Expression> where) {
    this(Optional.empty(), table, where);
  }

  public Delete(NodeLocation location, Table table, Optional<Expression> where) {
    this(Optional.of(location), table, where);
  }

  private Delete(Optional<NodeLocation> location, Table table, Optional<Expression> where) {
    super(location);
    this.table = requireNonNull(table, "table is null");
    this.where = requireNonNull(where, "where is null");
  }

  public Table getTable() {
    return table;
  }

  public Optional<Expression> getWhere() {
    return where;
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitDelete(this, context);
  }

  @Override
  public int hashCode() {
    return Objects.hash(table, where);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if ((obj == null) || (getClass() != obj.getClass())) {
      return false;
    }
    Delete o = (Delete) obj;
    return Objects.equals(table, o.table) &&
           Objects.equals(where, o.where);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("table", table.getName())
        .add("where", where)
        .toString();
  }
}
