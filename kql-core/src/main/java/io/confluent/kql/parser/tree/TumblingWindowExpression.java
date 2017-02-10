/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.kql.parser.tree;

public class TumblingWindowExpression extends HoppingWindowExpression {

  public TumblingWindowExpression(long size, WindowExpression.WindowUnit sizeUnit) {
    super(size, sizeUnit, size, sizeUnit);
  }

}
