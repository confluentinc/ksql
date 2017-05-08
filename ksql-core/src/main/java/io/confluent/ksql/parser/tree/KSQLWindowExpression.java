/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.ksql.parser.tree;

import java.util.Optional;

public abstract class KSQLWindowExpression extends Node {

  protected KSQLWindowExpression(Optional<NodeLocation> location) {
    super(location);
  }
}
