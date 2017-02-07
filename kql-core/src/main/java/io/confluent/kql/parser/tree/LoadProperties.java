/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.kql.parser.tree;

import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;

public class LoadProperties extends Statement {

  public LoadProperties(Optional<NodeLocation> location) {
    super(location);
  }

  @Override
  public int hashCode() {
    return Objects.hash("LoadProperties");
  }

  @Override
  public boolean equals(Object obj) {
    return this == obj;
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .toString();
  }
}
