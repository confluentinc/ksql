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

import io.confluent.ksql.api.client.Client;
import io.confluent.ksql.api.client.ClientOptions;
import io.confluent.ksql.api.client.StreamInfo;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
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
    // Build ksql api client here?
    System.out.println("In connect with " + url + " " + info);

    final ClientOptions options = ClientOptions.create();
    //.setHost("127.0.0.1")
    //.setPort(8088);
    final Client client = Client.create(options);

    /*
    System.out.println("Here");
    final BatchedQueryResult res = client.executeQuery("select * from riderLocations;");
    try {
      final List<Row> rows = res.get();
      rows.forEach(System.out::println);
    } catch (InterruptedException e) {
      e.printStackTrace();
    } catch (ExecutionException e) {
      e.printStackTrace();
    }
    */
    try {
      for (StreamInfo streamInfo : client.listStreams().get()) {
        System.out.println("Got stream: " + streamInfo);

      }
    } catch (InterruptedException e) {
      e.printStackTrace();
    } catch (ExecutionException e) {
      e.printStackTrace();
    }


    return new KsqlConnection(client);
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
