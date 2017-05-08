/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.ksql.parser.tree;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class JoinUsing
    extends JoinCriteria {

  private final List<String> columns;

  public JoinUsing(List<String> columns) {
    requireNonNull(columns, "columns is null");
    checkArgument(!columns.isEmpty(), "columns is empty");
    this.columns = ImmutableList.copyOf(columns);
  }

  public List<String> getColumns() {
    return columns;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    JoinUsing o = (JoinUsing) obj;
    return Objects.equals(columns, o.columns);
  }

  @Override
  public int hashCode() {
    return Objects.hash(columns);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .addValue(columns)
        .toString();
  }
}
