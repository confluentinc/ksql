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

import io.confluent.ksql.api.client.ColumnType;
import io.confluent.ksql.api.client.Row;
import io.vertx.core.json.JsonArray;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class RowImpl implements Row {

  private final List<String> columnNames;
  private final List<ColumnType> columnTypes;
  private final List<Object> values;
  private final Map<String, Integer> columnNameToIndex;

  @SuppressWarnings("unchecked")
  public RowImpl(
      final List<String> columnNames,
      final List<ColumnType> columnTypes,
      final JsonArray values,
      final Map<String, Integer> columnNameToIndex) {
    this.columnNames = Objects.requireNonNull(columnNames);
    this.columnTypes = Objects.requireNonNull(columnTypes);
    this.values = Objects.requireNonNull(values).getList();
    this.columnNameToIndex = Objects.requireNonNull(columnNameToIndex);
  }

  @Override
  public List<String> columnNames() {
    return columnNames;
  }

  @Override
  public List<ColumnType> columnTypes() {
    return columnTypes;
  }

  @Override
  public List<Object> values() {
    return values;
  }

  @Override
  public Object getObject(final int columnIndex) {
    return values.get(columnIndex - 1);
  }

  @Override
  public Object getObject(final String columnName) {
    return getObject(indexFromName(columnName));
  }

  @Override
  public String getString(final int columnIndex) {
    return (String)getObject(columnIndex);
  }

  @Override
  public String getString(final String columnName) {
    return getString(indexFromName(columnName));
  }

  @Override
  public Integer getInt(final int columnIndex) {
    final Number number = (Number)getObject(columnIndex);
    if (number == null) {
      return null;
    } else {
      return number instanceof Integer ? (Integer)number : number.intValue();
    }
  }

  @Override
  public Integer getInt(final String columnName) {
    return getInt(indexFromName(columnName));
  }

  @Override
  public Long getLong(final int columnIndex) {
    final Number number = (Number)getObject(columnIndex);
    if (number == null) {
      return null;
    } else {
      return number instanceof Long ? (Long)number : number.longValue();
    }
  }

  @Override
  public Long getLong(final String columnName) {
    return getLong(indexFromName(columnName));
  }

  @Override
  public Double getDouble(final int columnIndex) {
    final Number number = (Number)getObject(columnIndex);
    if (number == null) {
      return null;
    } else {
      return number instanceof Double ? (Double)number : number.doubleValue();
    }
  }

  @Override
  public Double getDouble(final String columnName) {
    return getDouble(indexFromName(columnName));
  }

  @Override
  public Boolean getBoolean(final int columnIndex) {
    return (Boolean)getObject(columnIndex);
  }

  @Override
  public Boolean getBoolean(final String columnName) {
    return getBoolean(indexFromName(columnName));
  }

  private int indexFromName(final String columnName) {
    final Integer index = columnNameToIndex.get(columnName);
    if (index == null) {
      throw new IllegalArgumentException("No column exists with name: " + columnName);
    }
    return index;
  }
}