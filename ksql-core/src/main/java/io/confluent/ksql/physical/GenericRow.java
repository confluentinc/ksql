/**
 * Copyright 2017 Confluent Inc.
 **/

package io.confluent.ksql.physical;

import java.util.Arrays;
import java.util.List;

public class GenericRow {

  private List<Object> columns;

  public GenericRow() {
  }

  public GenericRow(List<Object> columns) {
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

  public boolean hasTheSameContent(Object other) {
    if (!(other instanceof GenericRow)) {
      return  false;
    }
    GenericRow otherGenericRow = (GenericRow) other;
    if (columns.size() != otherGenericRow.columns.size()) {
      return false;
    }

    // For now string matching is used to compare the rows.
    return this.toString().equals(otherGenericRow.toString());
  }

  public List<Object> getColumns() {
    return columns;
  }

  public void setColumns(List<Object> columns) {
    this.columns = columns;
  }

}
