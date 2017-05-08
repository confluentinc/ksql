/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.kql.parser.tree;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;

public class Statements extends Node {

  public List<Statement> statementList;

  public Statements(List<Statement> statementList) {
    super(Optional.empty());
    this.statementList = statementList;
  }

  protected Statements(Optional<NodeLocation> location, List<Statement> statementList) {
    super(location);
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {

    return visitor.visitStatements(this, context);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .toString();
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if ((obj == null) || (getClass() != obj.getClass())) {
      return false;
    }
    Statements o = (Statements) obj;
    return Objects.equals(statementList, o.statementList);
  }

  @Override
  public int hashCode() {
    return Objects.hash(statementList);
  }
}
