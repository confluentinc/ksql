package io.confluent.kql.parser.tree;

import java.util.Optional;

public abstract class KQLWindowExpression extends Node {

  protected KQLWindowExpression(Optional<NodeLocation> location) {
    super(location);
  }
}
