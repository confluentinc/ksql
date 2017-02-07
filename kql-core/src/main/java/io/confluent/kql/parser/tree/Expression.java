/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.kql.parser.tree;

import io.confluent.kql.parser.CodegenExpressionFormatter;
import io.confluent.kql.parser.ExpressionFormatter;

import org.apache.kafka.connect.data.Schema;

import java.util.Optional;

public abstract class Expression
    extends Node {

  protected Expression(Optional<NodeLocation> location) {
    super(location);
  }

  /**
   * Accessible for {@link AstVisitor}, use {@link AstVisitor#process(Node, Object)} instead.
   */
  @Override
  protected <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitExpression(this, context);
  }

  @Override
  public final String toString() {
    return ExpressionFormatter.formatExpression(this);
  }

  public final String getCodegenString(Schema schema) {
    return CodegenExpressionFormatter.formatExpression(this, schema);
  }
}
