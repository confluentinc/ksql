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
  private final boolean isTopic;

  public ShowColumns(QualifiedName table, boolean isTopic) {
    this(Optional.empty(), table, isTopic);
  }

  public ShowColumns(NodeLocation location, QualifiedName table, boolean isTopic) {
    this(Optional.of(location), table, isTopic);
  }

  private ShowColumns(Optional<NodeLocation> location, QualifiedName table, boolean isTopic) {
    super(location);
    this.table = requireNonNull(table, "table is null");
    this.isTopic = isTopic;
  }

  public QualifiedName getTable() {
    return table;
  }

  public boolean isTopic() {
    return isTopic;
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
