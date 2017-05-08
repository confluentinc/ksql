/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.ksql.parser.tree;

import java.util.Optional;

public abstract class SelectItem
    extends Node {

  protected SelectItem(Optional<NodeLocation> location) {
    super(location);
  }
}
