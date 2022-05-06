/*
 * Copyright 2022 Confluent Inc.
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

package io.confluent.ksql.jdbc;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;

public class KsqlDriver implements Driver {

  private static final String URI_PREFIX = "jdbc:ksqldb://";

  static {
    try {
      java.sql.DriverManager.registerDriver(new KsqlDriver());
    } catch (SQLException ex) {
      throw new RuntimeException("Cannot register ksqlDB JDBC driver.", ex);
    }
  }

  @Override
  public Connection connect(final String url, final Properties info) throws SQLException {

    return null;
  }

  @Override
  public boolean acceptsURL(final String url) throws SQLException {
    return true; // JNH fix this
  }

  @Override
  public DriverPropertyInfo[] getPropertyInfo(final String url, final Properties info)
      throws SQLException {
    // JNH: no clue what this is.
    return new DriverPropertyInfo[0];
  }

  @Override
  public int getMajorVersion() {
    return 0;
  }

  @Override
  public int getMinorVersion() {
    return 0;
  }

  @Override
  public boolean jdbcCompliant() {
    return false;
  }

  @Override
  public Logger getParentLogger() throws SQLFeatureNotSupportedException {
    return null;
  }
}
