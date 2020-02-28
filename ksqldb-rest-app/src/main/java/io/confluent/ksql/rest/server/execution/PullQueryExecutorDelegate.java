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

package io.confluent.ksql.rest.server.execution;

import io.confluent.ksql.api.plugin.PullQueryApiExecutor;
import io.confluent.ksql.api.plugin.TableRows;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.rest.entity.TableRowsEntity;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.statement.ConfiguredStatement;
import java.util.List;
import java.util.Optional;

/**
 * Wrapper to break circular dependencies between modules. This is temporary until we migrate all
 * the rest app stuff to the new api
 */
public class PullQueryExecutorDelegate implements PullQueryApiExecutor {

  private final PullQueryExecutor pullQueryExecutor;

  public PullQueryExecutorDelegate(
      final PullQueryExecutor pullQueryExecutor) {
    this.pullQueryExecutor = pullQueryExecutor;
  }

  @Override
  public TableRows execute(final ConfiguredStatement<Query> statement,
      final ServiceContext serviceContext) {
    final TableRowsEntity tableRowsEntity = pullQueryExecutor.execute(
        statement, serviceContext, Optional.empty());
    return new TableRowsDelegate(tableRowsEntity);
  }

  private static class TableRowsDelegate implements TableRows {

    private final TableRowsEntity tableRowsEntity;

    TableRowsDelegate(final TableRowsEntity tableRowsEntity) {
      this.tableRowsEntity = tableRowsEntity;
    }

    @Override
    public LogicalSchema getSchema() {
      return tableRowsEntity.getSchema();
    }

    @Override
    public QueryId getQueryId() {
      return tableRowsEntity.getQueryId();
    }

    @Override
    public List<List<?>> getRows() {
      return tableRowsEntity.getRows();
    }
  }
}
