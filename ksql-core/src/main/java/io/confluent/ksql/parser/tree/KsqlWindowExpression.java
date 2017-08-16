/**
 * Copyright 2017 Confluent Inc.
 **/

package io.confluent.ksql.parser.tree;

import java.util.Optional;

public abstract class KsqlWindowExpression extends Node {

  protected KsqlWindowExpression(Optional<NodeLocation> location) {
    super(location);
  }
}
