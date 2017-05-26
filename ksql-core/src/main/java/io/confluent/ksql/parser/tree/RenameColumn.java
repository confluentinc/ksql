/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.ksql.parser.tree;

import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class RenameColumn
    extends Statement {

  private final QualifiedName table;
  private final String source;
  private final String target;

  public RenameColumn(QualifiedName table, String source, String target) {
    this(Optional.empty(), table, source, target);
  }

  public RenameColumn(NodeLocation location, QualifiedName table, String source, String target) {
    this(Optional.of(location), table, source, target);
  }

  private RenameColumn(Optional<NodeLocation> location, QualifiedName table, String source,
                       String target) {
    super(location);
    this.table = requireNonNull(table, "table is null");
    this.source = requireNonNull(source, "source is null");
    this.target = requireNonNull(target, "target is null");
  }

  public QualifiedName getTable() {
    return table;
  }

  public String getSource() {
    return source;
  }

  public String getTarget() {
    return target;
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitRenameColumn(this, context);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    RenameColumn that = (RenameColumn) o;
    return Objects.equals(table, that.table) &&
           Objects.equals(source, that.source) &&
           Objects.equals(target, that.target);
  }

  @Override
  public int hashCode() {
    return Objects.hash(table, source, target);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("table", table)
        .add("source", source)
        .add("target", target)
        .toString();
  }
}
