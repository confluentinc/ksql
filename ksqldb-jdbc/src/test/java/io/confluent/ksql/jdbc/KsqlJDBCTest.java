package io.confluent.ksql.jdbc;

import java.util.Properties;
import org.junit.Test;
import java.sql.*;

public class KsqlJDBCTest {

  @Test
  public void testSomething() throws Exception {
    //Class.forName("org.postgresql.Driver");

    String url = "jdbc:postgresql://localhost/postgres";
    Properties props = new Properties();
    props.setProperty("user","jhughes");
    // props.setProperty("password","secret");
    // props.setProperty("ssl","true");
    Connection conn = DriverManager.getConnection(url, props);


    Statement st = conn.createStatement();
    ResultSet rs = st.executeQuery("SELECT * FROM actor");
    while (rs.next())
    {
      System.out.print("Column 1 returned ");
      System.out.println(rs.getString(1) + " " + rs.getString(2));
    }
    rs.close();
    st.close();

    System.out.println("Catalog " + conn.getCatalog() + " metadata  " + conn.getMetaData());
  }

}
