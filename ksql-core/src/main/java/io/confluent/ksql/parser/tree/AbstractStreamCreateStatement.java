/**
 * Copyright 2017 Confluent Inc.
 **/

package io.confluent.ksql.parser.tree;

import java.util.List;
import java.util.Map;
import java.util.Optional;

public abstract class AbstractStreamCreateStatement extends Statement {
  public AbstractStreamCreateStatement(Optional<NodeLocation> location) {
    super(location);
  }

  public abstract Map<String,Expression> getProperties();

  public abstract QualifiedName getName();

  public abstract List<TableElement> getElements();
}
