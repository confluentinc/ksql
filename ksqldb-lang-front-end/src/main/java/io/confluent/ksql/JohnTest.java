/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.confluent.ksql;

import com.google.common.collect.ImmutableList;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Random;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;

public class JohnTest {


  public static void main(String[] args) throws SQLException, RelConversionException,
      ValidationException, SqlParseException {

    final KsqlCatalog catalog = new KsqlCatalog();
    catalog.addSampleTables();

    final KsqlLogicalPlanner logicalPlanner = new KsqlLogicalPlanner(catalog);
    final KsqlSimpleExecutor simpleExecutor = new KsqlSimpleExecutor();

    {
      System.out.println();
      final String statement = "select * from sample.orders";
      System.out.println(statement);
      final KsqlLogicalPlanner.KsqlLogicalPlan logicalPlan = logicalPlanner.getLogicalPlan(statement);
      System.out.println(logicalPlan);
      final ResultSet resultSet = simpleExecutor.execute(logicalPlan);
      print(resultSet);
    }
    {
      System.out.println();
      final String statement = "select * from sample.products";
      System.out.println(statement);
      final KsqlLogicalPlanner.KsqlLogicalPlan logicalPlan = logicalPlanner.getLogicalPlan(statement);
      System.out.println(logicalPlan);
      final ResultSet resultSet = simpleExecutor.execute(logicalPlan);
      print(resultSet);
    }
    {
      System.out.println();
      final String statement = "select * from sample.orders o join sample.products p on o.product = p.id";
      System.out.println(statement);
      final KsqlLogicalPlanner.KsqlLogicalPlan logicalPlan = logicalPlanner.getLogicalPlan(statement);
      System.out.println(logicalPlan);
      final ResultSet resultSet = simpleExecutor.execute(logicalPlan);
      print(resultSet);
    }
    {
      System.out.println();
      final String statement = "(select rowtime, product from sample.orders) union (select rowtime, id from sample.products)";
      System.out.println(statement);
      final KsqlLogicalPlanner.KsqlLogicalPlan logicalPlan = logicalPlanner.getLogicalPlan(statement);
      System.out.println(logicalPlan);
      final ResultSet resultSet = simpleExecutor.execute(logicalPlan);
      print(resultSet);
    }
    {
      System.out.println();
      final String statement = "select distinct * from ((select rowtime, product from sample.orders) union (select rowtime, id from sample.products))";
      System.out.println(statement);
      final KsqlLogicalPlanner.KsqlLogicalPlan logicalPlan = logicalPlanner.getLogicalPlan(statement);
      System.out.println(logicalPlan);
      final ResultSet resultSet = simpleExecutor.execute(logicalPlan);
      print(resultSet);
    }
  }



  private static void print(final ResultSet resultSet) throws SQLException {
    final ResultSetMetaData metaData = resultSet.getMetaData();
    final int columnCount = metaData.getColumnCount();
    for (int i = 1; i <= columnCount; i++) {
      System.out.print(metaData.getColumnLabel(i) + " | ");
    }
    System.out.print("\n");
    while (resultSet.next()) {
      for (int i = 1; i <= columnCount; i++) {
        System.out.print(resultSet.getObject(i) + " | ");
      }
      System.out.print("\n");
    }
  }


}
