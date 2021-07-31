/*
 * Copyright 2020 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.api.client.impl;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.api.client.ColumnType;
import io.confluent.ksql.api.client.KsqlArray;
import io.confluent.ksql.api.client.KsqlObject;
import io.confluent.ksql.api.client.Row;
import io.vertx.core.json.JsonArray;
import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class RowImpl implements Row {

  private final ImmutableList<String> columnNames;
  private final ImmutableList<ColumnType> columnTypes;
  private final KsqlArray values;
  private final ImmutableMap<String, Integer> columnNameToIndex;

  public RowImpl(
      final List<String> columnNames,
      final List<ColumnType> columnTypes,
      final JsonArray values,
      final Map<String, Integer> columnNameToIndex) {
    this.columnNames = ImmutableList.copyOf(Objects.requireNonNull(columnNames));
    this.columnTypes = ImmutableList.copyOf(Objects.requireNonNull(columnTypes));
    this.values = new KsqlArray(Objects.requireNonNull(values).getList());
    this.columnNameToIndex = ImmutableMap.copyOf(Objects.requireNonNull(columnNameToIndex));
  }

  @Override
  @SuppressFBWarnings(value = "EI_EXPOSE_REP", justification = "columnNames is ImmutableList")
  public List<String> columnNames() {
    return columnNames;
  }

  @Override
  @SuppressFBWarnings(value = "EI_EXPOSE_REP", justification = "columnTypes is ImmutableList")
  public List<ColumnType> columnTypes() {
    return columnTypes;
  }

  @Override
  public KsqlArray values() {
    return values.copy();
  }

  @Override
  public KsqlObject asObject() {
    return KsqlObject.fromArray(columnNames, values);
  }

  @Override
  public boolean isNull(final int columnIndex) {
    return getValue(columnIndex) == null;
  }

  @Override
  public boolean isNull(final String columnName) {
    return isNull(indexFromName(columnName));
  }

  @Override
  public Object getValue(final int columnIndex) {
    return values.getValue(columnIndex - 1);
  }

  @Override
  public Object getValue(final String columnName) {
    return getValue(indexFromName(columnName));
  }

  @Override
  public String getString(final int columnIndex) {
    return values.getString(columnIndex - 1);
  }

  @Override
  public String getString(final String columnName) {
    return getString(indexFromName(columnName));
  }

  @Override
  public Integer getInteger(final int columnIndex) {
    return values.getInteger(columnIndex - 1);
  }

  @Override
  public Integer getInteger(final String columnName) {
    return getInteger(indexFromName(columnName));
  }

  @Override
  public Long getLong(final int columnIndex) {
    return values.getLong(columnIndex - 1);
  }

  @Override
  public Long getLong(final String columnName) {
    return getLong(indexFromName(columnName));
  }

  @Override
  public Double getDouble(final int columnIndex) {
    return values.getDouble(columnIndex - 1);
  }

  @Override
  public Double getDouble(final String columnName) {
    return getDouble(indexFromName(columnName));
  }

  @Override
  public Boolean getBoolean(final int columnIndex) {
    return values.getBoolean(columnIndex - 1);
  }

  @Override
  public Boolean getBoolean(final String columnName) {
    return getBoolean(indexFromName(columnName));
  }

  @Override
  public BigDecimal getDecimal(final int columnIndex) {
    return values.getDecimal(columnIndex - 1);
  }

  @Override
  public BigDecimal getDecimal(final String columnName) {
    return getDecimal(indexFromName(columnName));
  }

  @Override
  public byte[] getBytes(final int columnIndex) {
    return values.getBytes(columnIndex - 1);
  }

  @Override
  public byte[] getBytes(final String columnName) {
    return getBytes(indexFromName(columnName));
  }

  @Override
  public KsqlObject getKsqlObject(final int columnIndex) {
    return values.getKsqlObject(columnIndex - 1);
  }

  @Override
  public KsqlObject getKsqlObject(final String columnName) {
    return getKsqlObject(indexFromName(columnName));
  }

  @Override
  public KsqlArray getKsqlArray(final int columnIndex) {
    return values.getKsqlArray(columnIndex - 1);
  }

  @Override
  public KsqlArray getKsqlArray(final String columnName) {
    return getKsqlArray(indexFromName(columnName));
  }

  private int indexFromName(final String columnName) {
    final Integer index = columnNameToIndex.get(columnName);
    if (index == null) {
      throw new IllegalArgumentException("No column exists with name: " + columnName);
    }
    return index;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final RowImpl row = (RowImpl) o;
    return columnNames.equals(row.columnNames)
        && columnTypes.equals(row.columnTypes)
        && values.equals(row.values);
  }

  @Override
  public int hashCode() {
    return Objects.hash(columnNames, columnTypes, values);
  }

  @Override
  public String toString() {
    return "Row{"
        + "columnNames=" + columnNames
        + ", columnTypes=" + columnTypes
        + ", values=" + values
        + '}';
  }
}