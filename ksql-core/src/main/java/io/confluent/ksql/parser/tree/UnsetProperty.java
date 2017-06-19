/**
 * Copyright 2017 Confluent Inc.
 **/

package io.confluent.ksql.parser.tree;

import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class UnsetProperty extends Statement {

  private final String propertyName;

  public UnsetProperty(Optional<NodeLocation> location, String propertyName) {
    super(location);
    requireNonNull(propertyName, "propertyName is null");
    this.propertyName = propertyName;
  }

  public String getPropertyName() {
    return propertyName;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof UnsetProperty)) {
      return false;
    }
    UnsetProperty that = (UnsetProperty) o;
    return Objects.equals(getPropertyName(), that.getPropertyName());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getPropertyName());
  }

  @Override
  public String toString() {
    return toStringHelper(this).toString();
  }
}
