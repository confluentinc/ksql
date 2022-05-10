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

import java.util.Properties;
import org.junit.Test;
import java.sql.*;

public class KsqlJDBCTest {

  @Test
  public void testSomething() throws Exception {
    Class.forName("io.confluent.ksql.jdbc.KsqlDriver"); // JNH What do I need to do to skip this?

    String url = "jdbc:ksqldb://localhost/postgres";
    Properties props = new Properties();
    //props.setProperty("user","jhughes");
    // props.setProperty("password","secret");
    // props.setProperty("ssl","true");
    Connection conn = DriverManager.getConnection(url, props);


    Statement st = conn.createStatement();
    ResultSet rs = st.executeQuery("SELECT * FROM riderLocations;");
    while (rs.next())
    {
      System.out.print("Column 1 returned ");
      System.out.println(rs.getString(1) + " " + rs.getDouble(2) + " " + rs.getDouble(3));
    }
    rs.close();
    st.close();

    System.out.println("Catalog " + conn.getCatalog() + " metadata  " + conn.getMetaData());
  }

}
