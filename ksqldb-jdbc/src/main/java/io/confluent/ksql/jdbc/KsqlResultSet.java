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

import io.confluent.ksql.api.client.Row;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.List;
import java.util.Map;

public class KsqlResultSet implements ResultSet {

  private final List<Row> rows;
  private Row currentRow = null;
  private int pointer = 0;

  public KsqlResultSet(final List<Row> rows) {
    this.rows = rows;
  }

  @Override
  public boolean next() throws SQLException {
    if (pointer < rows.size()) {
      currentRow = rows.get(pointer);
      pointer++;
      return true;
    } else {
      return false;
    }
  }

  @Override
  public void close() throws SQLException {
    // No-op
  }

  @Override
  public boolean wasNull() throws SQLException {
    return false;
  }

  @Override
  public String getString(final int columnIndex) throws SQLException {
    return currentRow.getString(columnIndex);
  }

  @Override
  public String getString(final String columnLabel) throws SQLException {
    return currentRow.getString(columnLabel);
  }

  @Override
  public boolean getBoolean(final int columnIndex) throws SQLException {
    return currentRow.getBoolean(columnIndex);
  }

  @Override
  public boolean getBoolean(final String columnLabel) throws SQLException {
    return currentRow.getBoolean(columnLabel);
  }

  @Override
  public byte getByte(final int columnIndex) throws SQLException {
    return currentRow.getBytes(columnIndex)[0]; // JNH: This may not work!
  }

  @Override
  public byte getByte(final String columnLabel) throws SQLException {
    return currentRow.getBytes(columnLabel)[0];
  }

  @Override
  public short getShort(final int columnIndex) throws SQLException {
    return 0; // JNH: FIX
  }

  @Override
  public short getShort(final String columnLabel) throws SQLException {
    return 0; // JNH: FIX
  }

  @Override
  public int getInt(final int columnIndex) throws SQLException {
    return currentRow.getInteger(columnIndex);
  }

  @Override
  public int getInt(final String columnLabel) throws SQLException {
    return currentRow.getInteger(columnLabel);
  }

  @Override
  public long getLong(final int columnIndex) throws SQLException {
    return currentRow.getLong(columnIndex);
  }

  @Override
  public long getLong(final String columnLabel) throws SQLException {
    return currentRow.getLong(columnLabel);
  }

  @Override
  public float getFloat(final int columnIndex) throws SQLException {
    return 0; // JNH: FIX
  }

  @Override
  public float getFloat(final String columnLabel) throws SQLException {
    return 0; // JNH: FIX
  }

  @Override
  public double getDouble(final int columnIndex) throws SQLException {
    return currentRow.getDouble(columnIndex);
  }

  @Override
  public double getDouble(final String columnLabel) throws SQLException {
    return currentRow.getDouble(columnLabel);
  }

  @Override
  public BigDecimal getBigDecimal(final int columnIndex) throws SQLException {
    return currentRow.getDecimal(columnIndex);
  }

  @Override
  public BigDecimal getBigDecimal(final String columnLabel) throws SQLException {
    return currentRow.getDecimal(columnLabel);
  }

  @Override
  public BigDecimal getBigDecimal(final int columnIndex, final int scale) throws SQLException {
    return null; // JNH: FIX
  }

  @Override
  public BigDecimal getBigDecimal(final String columnLabel, final int scale) throws SQLException {
    return null; // JNH: FIX
  }

  @Override
  public byte[] getBytes(final int columnIndex) throws SQLException {
    return currentRow.getBytes(columnIndex);
  }

  @Override
  public byte[] getBytes(final String columnLabel) throws SQLException {
    return currentRow.getBytes(columnLabel);
  }

  @Override
  public Date getDate(final int columnIndex) throws SQLException {
    return null; // JNH: FIX
  }

  @Override
  public Date getDate(final String columnLabel) throws SQLException {
    return null; // JNH: FIX
  }

  @Override
  public Date getDate(final int columnIndex, final Calendar cal) throws SQLException {
    return null; // JNH: FIX
  }

  @Override
  public Date getDate(final String columnLabel, final Calendar cal) throws SQLException {
    return null; // JNH: FIX
  }

  @Override
  public Time getTime(final int columnIndex) throws SQLException {
    return null; // JNH: FIX
  }

  @Override
  public Time getTime(final String columnLabel) throws SQLException {
    return null; // JNH: FIX
  }

  @Override
  public Time getTime(final int columnIndex, final Calendar cal) throws SQLException {
    return null; // JNH: FIX
  }

  @Override
  public Time getTime(final String columnLabel, final Calendar cal) throws SQLException {
    return null; // JNH: FIX
  }

  @Override
  public Timestamp getTimestamp(final int columnIndex) throws SQLException {
    return null; // JNH: FIX
  }

  @Override
  public Timestamp getTimestamp(final String columnLabel) throws SQLException {
    return null; // JNH: FIX
  }

  @Override
  public Timestamp getTimestamp(final int columnIndex, final Calendar cal) throws SQLException {
    return null; // JNH: FIX
  }

  @Override
  public Timestamp getTimestamp(final String columnLabel, final Calendar cal) throws SQLException {
    return null; // JNH: FIX
  }

  @Override
  public InputStream getAsciiStream(final int columnIndex) throws SQLException {
    return null; // JNH: FIX
  }

  @Override
  public InputStream getAsciiStream(final String columnLabel) throws SQLException {
    return null; // JNH: FIX
  }

  @Override
  public InputStream getUnicodeStream(final int columnIndex) throws SQLException {
    return null; // JNH: FIX
  }

  @Override
  public InputStream getUnicodeStream(final String columnLabel) throws SQLException {
    return null; // JNH: FIX
  }

  @Override
  public InputStream getBinaryStream(final int columnIndex) throws SQLException {
    return null; // JNH: FIX
  }

  @Override
  public InputStream getBinaryStream(final String columnLabel) throws SQLException {
    return null; // JNH: FIX
  }

  @Override
  public SQLWarning getWarnings() throws SQLException {
    return null;
  }

  @Override
  public void clearWarnings() throws SQLException {

  }

  @Override
  public String getCursorName() throws SQLException {
    return null;
  }

  @Override
  public ResultSetMetaData getMetaData() throws SQLException {
    if (rows.size() == 0) {
      return null;
    } else {
      return new KsqlResultSetMetaData(rows.get(0));
    }
  }

  @Override
  public Object getObject(final int columnIndex) throws SQLException {
    return null; // JNH: FIX
  }

  @Override
  public Object getObject(final String columnLabel) throws SQLException {
    return null; // JNH: FIX
  }

  @Override
  public Object getObject(final int columnIndex, final Map<String, Class<?>> map)
      throws SQLException {
    return null; // JNH: FIX
  }

  @Override
  public Object getObject(final String columnLabel, final Map<String, Class<?>> map)
      throws SQLException {
    return null;
  }

  @Override
  public <T> T getObject(final int columnIndex, final Class<T> type) throws SQLException {
    return null;
  }

  @Override
  public <T> T getObject(final String columnLabel, final Class<T> type) throws SQLException {
    return null;
  }

  @Override
  public int findColumn(final String columnLabel) throws SQLException {
    return 0; // JNH: FIX
  }

  @Override
  public Reader getCharacterStream(final int columnIndex) throws SQLException {
    return null;
  }

  @Override
  public Reader getCharacterStream(final String columnLabel) throws SQLException {
    return null;
  }

  @Override
  public boolean isBeforeFirst() throws SQLException {
    return false;
  }

  @Override
  public boolean isAfterLast() throws SQLException {
    return false;
  }

  @Override
  public boolean isFirst() throws SQLException {
    return false;
  }

  @Override
  public boolean isLast() throws SQLException {
    return false;
  }

  @Override
  public void beforeFirst() throws SQLException {

  }

  @Override
  public void afterLast() throws SQLException {

  }

  @Override
  public boolean first() throws SQLException {
    return false;
  }

  @Override
  public boolean last() throws SQLException {
    return false;
  }

  @Override
  public int getRow() throws SQLException {
    return 0;
  }

  @Override
  public boolean absolute(final int row) throws SQLException {
    return false;
  }

  @Override
  public boolean relative(final int rows) throws SQLException {
    return false;
  }

  @Override
  public boolean previous() throws SQLException {
    return false;
  }

  @Override
  public <T> T unwrap(final Class<T> iface) throws SQLException {
    return null;
  }

  @Override
  public boolean isWrapperFor(final Class<?> iface) throws SQLException {
    return false;
  }

  @Override
  public int getFetchDirection() throws SQLException {
    return 0;
  }

  @Override
  public void setFetchDirection(final int direction) throws SQLException {

  }

  @Override
  public int getFetchSize() throws SQLException {
    return 0;
  }

  @Override
  public void setFetchSize(final int rows) throws SQLException {

  }

  @Override
  public int getType() throws SQLException {
    return 0;
  }

  @Override
  public int getConcurrency() throws SQLException {
    return 0;
  }

  @Override
  public boolean rowUpdated() throws SQLException {
    return false;
  }

  @Override
  public boolean rowInserted() throws SQLException {
    return false;
  }

  @Override
  public boolean rowDeleted() throws SQLException {
    return false;
  }

  @Override
  public void updateNull(final int columnIndex) throws SQLException {

  }

  @Override
  public void updateNull(final String columnLabel) throws SQLException {

  }

  @Override
  public void updateBoolean(final int columnIndex, final boolean x) throws SQLException {

  }

  @Override
  public void updateBoolean(final String columnLabel, final boolean x) throws SQLException {

  }

  @Override
  public void updateByte(final int columnIndex, final byte x) throws SQLException {

  }

  @Override
  public void updateByte(final String columnLabel, final byte x) throws SQLException {

  }

  @Override
  public void updateShort(final int columnIndex, final short x) throws SQLException {

  }

  @Override
  public void updateShort(final String columnLabel, final short x) throws SQLException {

  }

  @Override
  public void updateInt(final int columnIndex, final int x) throws SQLException {

  }

  @Override
  public void updateInt(final String columnLabel, final int x) throws SQLException {

  }

  @Override
  public void updateLong(final int columnIndex, final long x) throws SQLException {

  }

  @Override
  public void updateLong(final String columnLabel, final long x) throws SQLException {

  }

  @Override
  public void updateFloat(final int columnIndex, final float x) throws SQLException {

  }

  @Override
  public void updateFloat(final String columnLabel, final float x) throws SQLException {

  }

  @Override
  public void updateDouble(final int columnIndex, final double x) throws SQLException {

  }

  @Override
  public void updateDouble(final String columnLabel, final double x) throws SQLException {

  }

  @Override
  public void updateBigDecimal(final int columnIndex, final BigDecimal x) throws SQLException {

  }

  @Override
  public void updateBigDecimal(final String columnLabel, final BigDecimal x) throws SQLException {

  }

  @Override
  public void updateString(final int columnIndex, final String x) throws SQLException {

  }

  @Override
  public void updateString(final String columnLabel, final String x) throws SQLException {

  }

  @Override
  public void updateBytes(final int columnIndex, final byte[] x) throws SQLException {

  }

  @Override
  public void updateBytes(final String columnLabel, final byte[] x) throws SQLException {

  }

  @Override
  public void updateDate(final int columnIndex, final Date x) throws SQLException {

  }

  @Override
  public void updateDate(final String columnLabel, final Date x) throws SQLException {

  }

  @Override
  public void updateTime(final int columnIndex, final Time x) throws SQLException {

  }

  @Override
  public void updateTime(final String columnLabel, final Time x) throws SQLException {

  }

  @Override
  public void updateTimestamp(final int columnIndex, final Timestamp x) throws SQLException {

  }

  @Override
  public void updateTimestamp(final String columnLabel, final Timestamp x) throws SQLException {

  }

  @Override
  public void updateAsciiStream(final int columnIndex, final InputStream x) throws SQLException {

  }

  @Override
  public void updateAsciiStream(final String columnLabel, final InputStream x) throws SQLException {

  }

  @Override
  public void updateAsciiStream(final int columnIndex, final InputStream x, final int length)
      throws SQLException {

  }

  @Override
  public void updateAsciiStream(final String columnLabel, final InputStream x, final int length)
      throws SQLException {

  }

  @Override
  public void updateAsciiStream(final int columnIndex, final InputStream x, final long length)
      throws SQLException {

  }

  @Override
  public void updateAsciiStream(final String columnLabel, final InputStream x, final long length)
      throws SQLException {

  }

  @Override
  public void updateBinaryStream(final int columnIndex, final InputStream x) throws SQLException {

  }

  @Override
  public void updateBinaryStream(final String columnLabel, final InputStream x)
      throws SQLException {

  }

  @Override
  public void updateBinaryStream(final int columnIndex, final InputStream x, final int length)
      throws SQLException {

  }

  @Override
  public void updateBinaryStream(final String columnLabel, final InputStream x, final int length)
      throws SQLException {

  }

  @Override
  public void updateBinaryStream(final int columnIndex, final InputStream x, final long length)
      throws SQLException {

  }

  @Override
  public void updateBinaryStream(final String columnLabel, final InputStream x, final long length)
      throws SQLException {

  }

  @Override
  public void updateCharacterStream(final int columnIndex, final Reader x) throws SQLException {

  }

  @Override
  public void updateCharacterStream(final String columnLabel, final Reader reader)
      throws SQLException {

  }

  @Override
  public void updateCharacterStream(final int columnIndex, final Reader x, final int length)
      throws SQLException {

  }

  @Override
  public void updateCharacterStream(final String columnLabel, final Reader reader, final int length)
      throws SQLException {

  }

  @Override
  public void updateCharacterStream(final int columnIndex, final Reader x, final long length)
      throws SQLException {

  }

  @Override
  public void updateCharacterStream(final String columnLabel, final Reader reader,
      final long length)
      throws SQLException {

  }

  @Override
  public void updateObject(final int columnIndex, final Object x) throws SQLException {

  }

  @Override
  public void updateObject(final String columnLabel, final Object x) throws SQLException {

  }

  @Override
  public void updateObject(final int columnIndex, final Object x, final int scaleOrLength)
      throws SQLException {

  }

  @Override
  public void updateObject(final String columnLabel, final Object x, final int scaleOrLength)
      throws SQLException {

  }

  @Override
  public void insertRow() throws SQLException {

  }

  @Override
  public void updateRow() throws SQLException {

  }

  @Override
  public void deleteRow() throws SQLException {

  }

  @Override
  public void refreshRow() throws SQLException {

  }

  @Override
  public void cancelRowUpdates() throws SQLException {

  }

  @Override
  public void moveToInsertRow() throws SQLException {

  }

  @Override
  public void moveToCurrentRow() throws SQLException {

  }

  @Override
  public Statement getStatement() throws SQLException {
    return null;
  }

  @Override
  public Ref getRef(final int columnIndex) throws SQLException {
    return null;
  }

  @Override
  public Ref getRef(final String columnLabel) throws SQLException {
    return null;
  }

  @Override
  public Blob getBlob(final int columnIndex) throws SQLException {
    return null;
  }

  @Override
  public Blob getBlob(final String columnLabel) throws SQLException {
    return null;
  }

  @Override
  public Clob getClob(final int columnIndex) throws SQLException {
    return null;
  }

  @Override
  public Clob getClob(final String columnLabel) throws SQLException {
    return null;
  }

  @Override
  public Array getArray(final int columnIndex) throws SQLException {
    return null;
  }

  @Override
  public Array getArray(final String columnLabel) throws SQLException {
    return null;
  }

  @Override
  public URL getURL(final int columnIndex) throws SQLException {
    return null;
  }

  @Override
  public URL getURL(final String columnLabel) throws SQLException {
    return null;
  }

  @Override
  public void updateRef(final int columnIndex, final Ref x) throws SQLException {

  }

  @Override
  public void updateRef(final String columnLabel, final Ref x) throws SQLException {

  }

  @Override
  public void updateBlob(final int columnIndex, final Blob x) throws SQLException {

  }

  @Override
  public void updateBlob(final String columnLabel, final Blob x) throws SQLException {

  }

  @Override
  public void updateBlob(final int columnIndex, final InputStream inputStream) throws SQLException {

  }

  @Override
  public void updateBlob(final String columnLabel, final InputStream inputStream)
      throws SQLException {

  }

  @Override
  public void updateBlob(final int columnIndex, final InputStream inputStream, final long length)
      throws SQLException {

  }

  @Override
  public void updateBlob(final String columnLabel, final InputStream inputStream, final long length)
      throws SQLException {

  }

  @Override
  public void updateClob(final int columnIndex, final Clob x) throws SQLException {

  }

  @Override
  public void updateClob(final String columnLabel, final Clob x) throws SQLException {

  }

  @Override
  public void updateClob(final int columnIndex, final Reader reader) throws SQLException {

  }

  @Override
  public void updateClob(final String columnLabel, final Reader reader) throws SQLException {

  }

  @Override
  public void updateClob(final int columnIndex, final Reader reader, final long length)
      throws SQLException {

  }

  @Override
  public void updateClob(final String columnLabel, final Reader reader, final long length)
      throws SQLException {

  }

  @Override
  public void updateArray(final int columnIndex, final Array x) throws SQLException {

  }

  @Override
  public void updateArray(final String columnLabel, final Array x) throws SQLException {

  }

  @Override
  public RowId getRowId(final int columnIndex) throws SQLException {
    return null;
  }

  @Override
  public RowId getRowId(final String columnLabel) throws SQLException {
    return null;
  }

  @Override
  public void updateRowId(final int columnIndex, final RowId x) throws SQLException {

  }

  @Override
  public void updateRowId(final String columnLabel, final RowId x) throws SQLException {

  }

  @Override
  public int getHoldability() throws SQLException {
    return 0;
  }

  @Override
  public boolean isClosed() throws SQLException {
    return false;
  }

  @Override
  public void updateNString(final int columnIndex, final String nstring) throws SQLException {

  }

  @Override
  public void updateNString(final String columnLabel, final String nstring) throws SQLException {

  }

  @Override
  public void updateNClob(final int columnIndex, final NClob nclob) throws SQLException {

  }

  @Override
  public void updateNClob(final String columnLabel, final NClob nclob) throws SQLException {

  }

  @Override
  public void updateNClob(final int columnIndex, final Reader reader) throws SQLException {

  }

  @Override
  public void updateNClob(final String columnLabel, final Reader reader) throws SQLException {

  }

  @Override
  public void updateNClob(final int columnIndex, final Reader reader, final long length)
      throws SQLException {

  }

  @Override
  public void updateNClob(final String columnLabel, final Reader reader, final long length)
      throws SQLException {

  }

  @Override
  public NClob getNClob(final int columnIndex) throws SQLException {
    return null;
  }

  @Override
  public NClob getNClob(final String columnLabel) throws SQLException {
    return null;
  }

  @Override
  public SQLXML getSQLXML(final int columnIndex) throws SQLException {
    return null;
  }

  @Override
  public SQLXML getSQLXML(final String columnLabel) throws SQLException {
    return null;
  }

  @Override
  public void updateSQLXML(final int columnIndex, final SQLXML xmlObject) throws SQLException {

  }

  @Override
  public void updateSQLXML(final String columnLabel, final SQLXML xmlObject) throws SQLException {

  }

  @Override
  public String getNString(final int columnIndex) throws SQLException {
    return null;
  }

  @Override
  public String getNString(final String columnLabel) throws SQLException {
    return null;
  }

  @Override
  public Reader getNCharacterStream(final int columnIndex) throws SQLException {
    return null;
  }

  @Override
  public Reader getNCharacterStream(final String columnLabel) throws SQLException {
    return null;
  }

  @Override
  public void updateNCharacterStream(final int columnIndex, final Reader x) throws SQLException {

  }

  @Override
  public void updateNCharacterStream(final String columnLabel, final Reader reader)
      throws SQLException {

  }

  @Override
  public void updateNCharacterStream(final int columnIndex, final Reader x, final long length)
      throws SQLException {

  }

  @Override
  public void updateNCharacterStream(final String columnLabel, final Reader reader,
      final long length)
      throws SQLException {

  }

}
