/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.ksql.parser.tree;

import java.util.Objects;
import java.util.Optional;

import io.confluent.ksql.util.KSQLException;
import io.confluent.ksql.util.SchemaUtil;

import static java.util.Objects.requireNonNull;

public class SingleColumn
    extends SelectItem {

  private final Optional<String> alias;
  private final Expression expression;

  public SingleColumn(Expression expression) {
    this(Optional.empty(), expression, Optional.empty());
  }

  public SingleColumn(Expression expression, Optional<String> alias) {
    this(Optional.empty(), expression, alias);
  }

  public SingleColumn(Expression expression, String alias) {
    this(Optional.empty(), expression, Optional.of(alias));
  }

  public SingleColumn(NodeLocation location, Expression expression, Optional<String> alias) {
    this(Optional.of(location), expression, alias);
  }

  private SingleColumn(Optional<NodeLocation> location, Expression expression,
                       Optional<String> alias) {
    super(location);
    requireNonNull(expression, "expression is null");
    requireNonNull(alias, "alias is null");

    if (alias.isPresent() && alias.get().equalsIgnoreCase(SchemaUtil.ROWKEY_NAME)) {
      String expressionStr = expression.toString();
      if (!expressionStr.substring(expressionStr.indexOf(".") + 1).equalsIgnoreCase(SchemaUtil
                                                                                        .ROWKEY_NAME)) {
        throw new KSQLException(SchemaUtil.ROWKEY_NAME + " is a reserved token for implicit column."
                                + " You cannot use it as an alias for a column.");
      }

    }

    this.expression = expression;
    this.alias = alias;
  }

  public Optional<String> getAlias() {
    return alias;
  }

  public Expression getExpression() {
    return expression;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    SingleColumn other = (SingleColumn) obj;
    return Objects.equals(this.alias, other.alias) && Objects
        .equals(this.expression, other.expression);
  }

  @Override
  public int hashCode() {
    return Objects.hash(alias, expression);
  }

  @Override
  public String toString() {
    if (alias.isPresent()) {
      return expression.toString() + " " + alias.get();
    }

    return expression.toString();
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitSingleColumn(this, context);
  }
}
