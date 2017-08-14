/**
 * Copyright 2017 Confluent Inc.
 **/

package io.confluent.ksql.physical;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class GenericRow {

  private final List<Object> columns;

  public GenericRow() {
    columns = new ArrayList<>();
  }

  public GenericRow(List<Object> columns) {
    Objects.requireNonNull(columns);
    this.columns = columns;
  }

  @Override
  public String toString() {
    StringBuilder stringBuilder = new StringBuilder("[ ");
    int currentIndex = 0;
    for (int i = 0; i < columns.size(); i++) {
      Object obj = columns.get(i);
      if (obj == null) {
        stringBuilder.append("null");
      } else if (obj.getClass().isArray()) {
        stringBuilder.append(Arrays.toString((Object[]) obj));
      } else if (obj instanceof String) {
        stringBuilder.append("'" + obj + "'");
      } else {
        stringBuilder.append(obj);
      }

      currentIndex++;
      if (currentIndex < columns.size()) {
        stringBuilder.append(" | ");
      }
    }
    stringBuilder.append(" ]");
    return stringBuilder.toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    GenericRow that = (GenericRow) o;

    if (columns.size() != that.columns.size()) return false;

    // For now string matching is used to compare the rows as double comparision will cause issues
    return this.toString().equals(that.toString());
  }

  @Override
  public int hashCode() {
    return Objects.hash(columns);
  }

  public List<Object> getColumns() {
    return columns;
  }

}
