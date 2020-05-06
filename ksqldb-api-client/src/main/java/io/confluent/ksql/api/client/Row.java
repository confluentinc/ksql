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

package io.confluent.ksql.api.client;

import java.util.List;

/**
 * A single record, returned as part of a query result.
 */
public interface Row {

  List<String> columnNames();

  List<ColumnType> columnTypes();

  KsqlArray values();

  /**
   * Whether the value for a particular column of the Row is null.
   *
   * @param columnIndex index of column (1-indexed).
   * @return whether the column value is null.
   */
  boolean isNull(int columnIndex);

  /**
   * Whether the value for a particular column of the Row is null.
   *
   * @param columnName name of column.
   * @return whether the column value is null.
   */
  boolean isNull(String columnName);

  /**
   * Get the value for a particular column of the Row as an Object.
   *
   * @param columnIndex index of column (1-indexed).
   * @return column value.
   */
  Object getValue(int columnIndex);

  /**
   * Get the value for a particular column of the Row as an Object.
   *
   * @param columnName name of column.
   * @return column value.
   */
  Object getValue(String columnName);

  /**
   * Get the value for a particular column of the Row as a string.
   *
   * @param columnIndex index of column (1-indexed).
   * @return column value.
   */
  String getString(int columnIndex);

  /**
   * Get the value for a particular column of the Row as a string.
   *
   * @param columnName name of column.
   * @return column value.
   */
  String getString(String columnName);

  /**
   * Get the value for a particular column of the Row as an integer.
   *
   * @param columnIndex index of column (1-indexed).
   * @return column value.
   */
  Integer getInt(int columnIndex);

  /**
   * Get the value for a particular column of the Row as an integer.
   *
   * @param columnName name of column.
   * @return column value.
   */
  Integer getInt(String columnName);

  /**
   * Get the value for a particular column of the Row as a long.
   *
   * @param columnIndex index of column (1-indexed).
   * @return column value.
   */
  Long getLong(int columnIndex);

  /**
   * Get the value for a particular column of the Row as a long.
   *
   * @param columnName name of column.
   * @return column value.
   */
  Long getLong(String columnName);

  /**
   * Get the value for a particular column of the Row as a double.
   *
   * @param columnIndex index of column (1-indexed).
   * @return column value.
   */
  Double getDouble(int columnIndex);

  /**
   * Get the value for a particular column of the Row as a double.
   *
   * @param columnName name of column.
   * @return column value.
   */
  Double getDouble(String columnName);

  /**
   * Get the value for a particular column of the Row as a boolean.
   *
   * @param columnIndex index of column (1-indexed).
   * @return column value.
   */
  Boolean getBoolean(int columnIndex);

  /**
   * Get the value for a particular column of the Row as a boolean.
   *
   * @param columnName name of column.
   * @return column value.
   */
  Boolean getBoolean(String columnName);

  /**
   * Get the value for a particular column of the Row as a KsqlObject.
   * Useful for MAP and STRUCT column types.
   *
   * @param columnIndex index of column (1-indexed).
   * @return column value.
   */
  KsqlObject getKsqlObject(int columnIndex);

  /**
   * Get the value for a particular column of the Row as a KsqlObject.
   * Useful for MAP and STRUCT column types.
   *
   * @param columnName name of column.
   * @return column value.
   */
  KsqlObject getKsqlObject(String columnName);

  /**
   * Get the value for a particular column of the Row as a KsqlArray. Useful for ARRAY column types.
   *
   * @param columnIndex index of column (1-indexed).
   * @return column value.
   */
  KsqlArray getKsqlArray(int columnIndex);

  /**
   * Get the value for a particular column of the Row as a KsqlArray. Useful for ARRAY column types.
   *
   * @param columnName name of column.
   * @return column value.
   */
  KsqlArray getKsqlArray(String columnName);
}