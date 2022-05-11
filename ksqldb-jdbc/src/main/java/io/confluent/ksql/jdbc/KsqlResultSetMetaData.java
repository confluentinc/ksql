/*
 * Copyright 2022 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, final software
 * distributed under the License is distributed on an "AS IS" BASIS, final WITHOUT
 * WARRANTIES OF ANY KIND, final either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.jdbc;

import io.confluent.ksql.api.client.ColumnType;
import io.confluent.ksql.api.client.ColumnType.Type;
import io.confluent.ksql.api.client.Row;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;

public class KsqlResultSetMetaData implements ResultSetMetaData {
  private final Row sampleRow;
  private final List<ColumnType> columnTypes;
  private final List<String> columnNames;

  public KsqlResultSetMetaData(final Row sampleRow) {
    this.sampleRow = sampleRow;
    this.columnTypes = sampleRow.columnTypes();
    this.columnNames = sampleRow.columnNames();
  }

  @Override
  public int getColumnCount() throws SQLException {
    return columnTypes.size();
  }

  @Override
  public boolean isAutoIncrement(final int column) throws SQLException {
    return false;
  }

  @Override
  public boolean isCaseSensitive(final int column) throws SQLException {
    return false;
  }

  @Override
  public boolean isSearchable(final int column) throws SQLException {
    return false;
  }

  @Override
  public boolean isCurrency(final int column) throws SQLException {
    return false;
  }

  @Override
  public int isNullable(final int column) throws SQLException {
    return 0;
  }

  @Override
  public boolean isSigned(final int column) throws SQLException {
    return false;
  }

  @Override
  public int getColumnDisplaySize(final int column) throws SQLException {
    return 0;
  }

  @Override
  public String getColumnLabel(final int column) throws SQLException {
    return columnNames.get(column - 1);
  }

  @Override
  public String getColumnName(final int column) throws SQLException {
    return columnNames.get(column - 1);
  }

  @Override
  public String getSchemaName(final int column) throws SQLException {
    return columnNames.get(column - 1);
  }

  @Override
  public int getPrecision(final int column) throws SQLException {
    return 0;
  }

  @Override
  public int getScale(final int column) throws SQLException {
    return 0;
  }

  @Override
  public String getTableName(final int column) throws SQLException {
    return null;
  }

  @Override
  public String getCatalogName(final int column) throws SQLException {
    return null;
  }

  // JNH Complete this.
  @Override
  public int getColumnType(final int column) throws SQLException {
    final ColumnType type = columnTypes.get(column - 1);
    if (type.getType() == Type.DATE) {
      return 91;
    } else if (type.getType() == Type.STRING) {
      return 12;
    } else if (type.getType() == Type.DOUBLE) {
      return 8;
    } else {
      return 0; // Default case is wrong.
    }
  }

  @Override
  public String getColumnTypeName(final int column) throws SQLException {
    return columnTypes.get(column - 1).toString();
  }

  @Override
  public boolean isReadOnly(final int column) throws SQLException {
    return false;
  }

  @Override
  public boolean isWritable(final int column) throws SQLException {
    return false;
  }

  @Override
  public boolean isDefinitelyWritable(final int column) throws SQLException {
    return false;
  }

  @Override
  public String getColumnClassName(final int column) throws SQLException {
    return null;
  }

  @Override
  public <T> T unwrap(final Class<T> iface) throws SQLException {
    return null;
  }

  @Override
  public boolean isWrapperFor(final Class<?> iface) throws SQLException {
    return false;
  }
}
