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

import java.math.BigDecimal;
import java.util.List;

/**
 * A single record, returned as part of a query result.
 */
public interface Row {

  /**
   * Returns column names for the data in this row.
   *
   * @return the column names
   */
  List<String> columnNames();

  /**
   * Returns column types for the data in this row.
   *
   * @return the column types
   */
  List<ColumnType> columnTypes();

  /**
   * Returns the values (data) in this row, represented as a {@link KsqlArray}.
   *
   * <p>Returned values are JSON types which means numeric columns are not necessarily typed in
   * accordance with {@link #columnTypes}. For example, a {@code BIGINT} field will be typed as an
   * integer rather than a long, if the numeric value fits into an integer.
   *
   * @return the values
   */
  KsqlArray values();

  /**
   * Returns the data in this row represented as a {@link KsqlObject} where keys are column names
   * and values are column values.
   *
   * @return the data
   */
  KsqlObject asObject();

  /**
   * Returns whether the value for a particular column of the {@code Row} is null.
   *
   * @param columnIndex index of column (1-indexed)
   * @return whether the column value is null
   * @throws IndexOutOfBoundsException if the index is invalid
   */
  boolean isNull(int columnIndex);

  /**
   * Returns whether the value for a particular column of the {@code Row} is null.
   *
   * @param columnName name of column
   * @return whether the column value is null
   * @throws IllegalArgumentException if the column name is invalid
   */
  boolean isNull(String columnName);

  /**
   * Returns the value for a particular column of the {@code Row} as an {@code Object}.
   *
   * @param columnIndex index of column (1-indexed)
   * @return column value
   * @throws IndexOutOfBoundsException if the index is invalid
   */
  Object getValue(int columnIndex);

  /**
   * Returns the value for a particular column of the {@code Row} as an {@code Object}.
   *
   * @param columnName name of column
   * @return column value
   * @throws IllegalArgumentException if the column name is invalid
   */
  Object getValue(String columnName);

  /**
   * Returns the value for a particular column of the {@code Row} as a string.
   *
   * @param columnIndex index of column (1-indexed)
   * @return column value
   * @throws ClassCastException if the column value is not a string
   * @throws IndexOutOfBoundsException if the index is invalid
   */
  String getString(int columnIndex);

  /**
   * Returns the value for a particular column of the {@code Row} as a string.
   *
   * @param columnName name of column
   * @return column value
   * @throws ClassCastException if the column value is not a string
   * @throws IllegalArgumentException if the column name is invalid
   */
  String getString(String columnName);

  /**
   * Returns the value for a particular column of the {@code Row} as an integer.
   *
   * @param columnIndex index of column (1-indexed)
   * @return column value
   * @throws ClassCastException if the column value is not a {@code Number}
   * @throws IndexOutOfBoundsException if the index is invalid
   */
  Integer getInteger(int columnIndex);

  /**
   * Returns the value for a particular column of the {@code Row} as an integer.
   *
   * @param columnName name of column
   * @return column value
   * @throws ClassCastException if the column value is not a {@code Number}
   * @throws IllegalArgumentException if the column name is invalid
   */
  Integer getInteger(String columnName);

  /**
   * Returns the value for a particular column of the {@code Row} as a long.
   *
   * @param columnIndex index of column (1-indexed)
   * @return column value
   * @throws ClassCastException if the column value is not a {@code Number}
   * @throws IndexOutOfBoundsException if the index is invalid
   */
  Long getLong(int columnIndex);

  /**
   * Returns the value for a particular column of the {@code Row} as a long.
   *
   * @param columnName name of column
   * @return column value
   * @throws ClassCastException if the column value is not a {@code Number}
   * @throws IllegalArgumentException if the column name is invalid
   */
  Long getLong(String columnName);

  /**
   * Returns the value for a particular column of the {@code Row} as a double.
   *
   * @param columnIndex index of column (1-indexed)
   * @return column value
   * @throws ClassCastException if the column value is not a {@code Number}
   * @throws IndexOutOfBoundsException if the index is invalid
   */
  Double getDouble(int columnIndex);

  /**
   * Returns the value for a particular column of the {@code Row} as a double.
   *
   * @param columnName name of column
   * @return column value
   * @throws ClassCastException if the column value is not a {@code Number}
   * @throws IllegalArgumentException if the column name is invalid
   */
  Double getDouble(String columnName);

  /**
   * Returns the value for a particular column of the {@code Row} as a boolean.
   *
   * @param columnIndex index of column (1-indexed)
   * @return column value
   * @throws ClassCastException if the column value is not a boolean
   * @throws IndexOutOfBoundsException if the index is invalid
   */
  Boolean getBoolean(int columnIndex);

  /**
   * Returns the value for a particular column of the {@code Row} as a boolean.
   *
   * @param columnName name of column
   * @return column value
   * @throws ClassCastException if the column value is not a boolean
   * @throws IllegalArgumentException if the column name is invalid
   */
  Boolean getBoolean(String columnName);

  /**
   * Returns the value for a particular column of the {@code Row} as a {@code BigDecimal}.
   *
   * @param columnIndex index of column (1-indexed)
   * @return column value
   * @throws ClassCastException if the column value is not a {@code Number}
   * @throws IndexOutOfBoundsException if the index is invalid
   */
  BigDecimal getDecimal(int columnIndex);

  /**
   * Returns the value for a particular column of the {@code Row} as a {@code BigDecimal}.
   *
   * @param columnName name of column
   * @return column value
   * @throws ClassCastException if the column value is not a {@code Number}
   * @throws IllegalArgumentException if the column name is invalid
   */
  BigDecimal getDecimal(String columnName);

  /**
   * Returns the value for a particular column of the {@code Row} as a byte array.
   *
   * @param columnIndex index of column (1-indexed)
   * @return column value
   * @throws ClassCastException if the column value is not a {@code String}
   * @throws IllegalArgumentException if the column value is not a base64 encoded string
   * @throws IndexOutOfBoundsException if the index is invalid
   */
  byte[] getBytes(int columnIndex);

  /**
   * Returns the value for a particular column of the {@code Row} as byte array.
   *
   * @param columnName name of column
   * @return column value
   * @throws ClassCastException if the column value is not a {@code String}
   * @throws IllegalArgumentException if the column name is invalid or the column value is not
   *                                  a base64 encoded string
   */
  byte[] getBytes(String columnName);

  /**
   * Returns the value for a particular column of the {@code Row} as a {@link KsqlObject}.
   * Useful for {@code MAP} and {@code STRUCT} column types.
   *
   * @param columnIndex index of column (1-indexed)
   * @return column value
   * @throws ClassCastException if the column value cannot be converted to a map
   * @throws IndexOutOfBoundsException if the index is invalid
   */
  KsqlObject getKsqlObject(int columnIndex);

  /**
   * Returns the value for a particular column of the {@code Row} as a {@link KsqlObject}.
   * Useful for {@code MAP} and {@code STRUCT} column types.
   *
   * @param columnName name of column
   * @return column value
   * @throws ClassCastException if the column value cannot be converted to a map
   * @throws IllegalArgumentException if the column name is invalid
   */
  KsqlObject getKsqlObject(String columnName);

  /**
   * Returns the value for a particular column of the {@code Row} as a {@link KsqlArray}.
   * Useful for {@code ARRAY} column types.
   *
   * @param columnIndex index of column (1-indexed)
   * @return column value
   * @throws ClassCastException if the column value cannot be converted to a list
   * @throws IndexOutOfBoundsException if the index is invalid
   */
  KsqlArray getKsqlArray(int columnIndex);

  /**
   * Returns the value for a particular column of the {@code Row} as a {@link KsqlArray}.
   * Useful for {@code ARRAY} column types.
   *
   * @param columnName name of column
   * @return column value
   * @throws ClassCastException if the column value cannot be converted to a list
   * @throws IllegalArgumentException if the column name is invalid
   */
  KsqlArray getKsqlArray(String columnName);
}