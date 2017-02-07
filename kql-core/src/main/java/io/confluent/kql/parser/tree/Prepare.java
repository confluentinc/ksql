/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.kql.parser.tree;

import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class Prepare
    extends Statement {

  private final String name;
  private final Statement statement;

  public Prepare(NodeLocation location, String name, Statement statement) {
    this(Optional.of(location), name, statement);
  }

  public Prepare(String name, Statement statement) {
    this(Optional.empty(), name, statement);
  }

  private Prepare(Optional<NodeLocation> location, String name, Statement statement) {
    super(location);
    this.name = requireNonNull(name, "name is null");
    this.statement = requireNonNull(statement, "statement is null");
  }

  public String getName() {
    return name;
  }

  public Statement getStatement() {
    return statement;
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitPrepare(this, context);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, statement);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if ((obj == null) || (getClass() != obj.getClass())) {
      return false;
    }
    Prepare o = (Prepare) obj;
    return Objects.equals(name, o.name) &&
           Objects.equals(statement, o.statement);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("name", name)
        .add("statement", statement)
        .toString();
  }
}
