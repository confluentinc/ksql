/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.ksql;

import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.KsqlStatementException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.apache.calcite.interpreter.Bindables;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelHomogeneousShuttle;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.tools.RelRunner;

public class KsqlSimpleExecutor {
  public static class KsqlResultSet {
    private KsqlLogicalPlanner.KsqlLogicalPlan logicalPlan;
    private ResultSet resultSet;

    public KsqlResultSet(final KsqlLogicalPlanner.KsqlLogicalPlan logicalPlan, final ResultSet resultSet) {
      this.logicalPlan = logicalPlan;
      this.resultSet = resultSet;
    }

    public KsqlLogicalPlanner.KsqlLogicalPlan getLogicalPlan() {
      return logicalPlan;
    }

    public ResultSet getResultSet() {
      return resultSet;
    }
  }

  private final Connection connection;
  private final RelRunner runner;

  public KsqlSimpleExecutor() {
    try {
      connection = DriverManager.getConnection("jdbc:calcite:");
      runner = connection.unwrap(RelRunner.class);
    } catch (SQLException e) {
      throw new KsqlException(e);
    }
  }

  public KsqlResultSet execute(final KsqlLogicalPlanner.KsqlLogicalPlan logicalPlan) {
    final RelNode rel = logicalPlan.getRelNode();
    final RelShuttle shuttle = new RelHomogeneousShuttle() {
      @Override
      public RelNode visit(TableScan scan) {
        final RelOptTable table = scan.getTable();
        if (scan instanceof LogicalTableScan
            && Bindables.BindableTableScan.canHandle(table)) {
          // Always replace the LogicalTableScan with BindableTableScan
          // because it's implementation does not require a "schema" as context.
          return Bindables.BindableTableScan.create(scan.getCluster(), table);
        }
        return super.visit(scan);
      }
    };
    final RelNode boundRel = rel.accept(shuttle);
    try {
      @SuppressWarnings("resource") final PreparedStatement result =
          runner.prepareStatement(boundRel);
      return new KsqlResultSet(logicalPlan, result.executeQuery());
    } catch (SQLException e) {
      throw new KsqlStatementException(
          "Could not execute statement",
          logicalPlan.getOriginalStatement(),
          e
      );
    }
  }
}


