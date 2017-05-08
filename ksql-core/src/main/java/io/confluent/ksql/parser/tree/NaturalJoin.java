/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.ksql.parser.tree;

import static com.google.common.base.MoreObjects.toStringHelper;

public class NaturalJoin
    extends JoinCriteria {

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    return (obj != null) && (getClass() == obj.getClass());
  }

  @Override
  public int hashCode() {
    return 0;
  }

  @Override
  public String toString() {
    return toStringHelper(this).toString();
  }
}
