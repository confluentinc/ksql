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

package io.confluent.ksql.api.impl;

import io.confluent.ksql.api.server.PushQueryHandle;
import io.confluent.ksql.api.spi.QueryPublisher;
import io.confluent.ksql.engine.KsqlEngine;
import io.confluent.ksql.parser.KsqlParser.ParsedStatement;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.query.BlockingRowQueue;
import io.confluent.ksql.rest.entity.TableRowsEntity;
import io.confluent.ksql.rest.server.execution.PullQueryExecutor;
import io.confluent.ksql.schema.ksql.Column;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.utils.FormatOptions;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlStatementException;
import io.confluent.ksql.util.TransientQueryMetadata;
import io.confluent.ksql.util.VertxUtils;
import io.vertx.core.Context;
import io.vertx.core.WorkerExecutor;
import io.vertx.core.json.JsonObject;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

public class QueryEndpoint {

  private final KsqlEngine ksqlEngine;
  private final KsqlConfig ksqlConfig;
  private final PullQueryExecutor pullQueryExecutor;

  public QueryEndpoint(final KsqlEngine ksqlEngine, final KsqlConfig ksqlConfig,
      final PullQueryExecutor pullQueryExecutor) {
    this.ksqlEngine = ksqlEngine;
    this.ksqlConfig = ksqlConfig;
    this.pullQueryExecutor = pullQueryExecutor;
  }

  public QueryPublisher createQueryPublisher(
      final String sql, final JsonObject properties,
      final Context context,
      final WorkerExecutor workerExecutor,
      final ServiceContext serviceContext) {

    // Must be run on worker as all this stuff is slow
    VertxUtils.checkIsWorker();

    properties.put("auto.offset.reset", "earliest");

    final ConfiguredStatement<Query> statement = createStatement(sql, properties.getMap());

    if (statement.getStatement().isPullQuery()) {
      return createPullQueryPublisher(context, serviceContext, statement);
    } else {
      return createPushQueryPublisher(context, serviceContext, statement, workerExecutor);
    }
  }

  private QueryPublisher createPushQueryPublisher(
      final Context context,
      final ServiceContext serviceContext,
      final ConfiguredStatement<Query> statement,
      final WorkerExecutor workerExecutor
  ) {
    final BlockingQueryPublisher publisher = new BlockingQueryPublisher(context, workerExecutor);

    final TransientQueryMetadata queryMetadata = ksqlEngine.executeQuery(serviceContext, statement);

    publisher.setQueryHandle(new KsqlQueryHandle(queryMetadata));

    return publisher;
  }

  private QueryPublisher createPullQueryPublisher(final Context context,
      final ServiceContext serviceContext,
      final ConfiguredStatement<Query> statement) {
    final TableRowsEntity tableRows = pullQueryExecutor.execute(
        statement, serviceContext, Optional.empty());
    return new PullQueryPublisher(context, tableRows, colNamesFromSchema(tableRows.getSchema()),
        colTypesFromSchema(tableRows.getSchema()));
  }

  private ConfiguredStatement<Query> createStatement(final String queryString,
      final Map<String, Object> properties) {
    final List<ParsedStatement> statements = ksqlEngine.parse(queryString);
    if ((statements.size() != 1)) {
      throw new KsqlStatementException(
          String
              .format("Expected exactly one KSQL statement; found %d instead", statements.size()),
          queryString);
    }
    final PreparedStatement<?> ps = ksqlEngine.prepare(statements.get(0));
    final Statement statement = ps.getStatement();
    if (!(statement instanceof Query)) {
      throw new KsqlStatementException("Not a query", queryString);
    }
    @SuppressWarnings("unchecked") final PreparedStatement<Query> psq =
        (PreparedStatement<Query>) ps;
    return ConfiguredStatement.of(psq, properties, ksqlConfig);
  }

  private static List<String> colTypesFromSchema(final LogicalSchema logicalSchema) {
    final List<Column> key = logicalSchema.key();
    final List<Column> val = logicalSchema.value();
    final List<String> colTypes = new ArrayList<>(key.size() + val.size());
    for (Column col : key) {
      colTypes.add(col.type().toString(FormatOptions.none()));
    }
    for (Column col : val) {
      colTypes.add(col.type().toString(FormatOptions.none()));
    }
    return colTypes;
  }

  private static List<String> colNamesFromSchema(final LogicalSchema logicalSchema) {
    final List<Column> key = logicalSchema.key();
    final List<Column> val = logicalSchema.value();
    final List<String> colNames = new ArrayList<>(key.size() + val.size());
    for (Column col : key) {
      colNames.add(col.name().text());
    }
    for (Column col : val) {
      colNames.add(col.name().text());
    }
    return colNames;
  }

  private static class KsqlQueryHandle implements PushQueryHandle {

    private final TransientQueryMetadata queryMetadata;

    KsqlQueryHandle(final TransientQueryMetadata queryMetadata) {
      this.queryMetadata = Objects.requireNonNull(queryMetadata, "queryMetadata");
    }

    @Override
    public List<String> getColumnNames() {
      return colNamesFromSchema(queryMetadata.getLogicalSchema());
    }

    @Override
    public List<String> getColumnTypes() {
      return colTypesFromSchema(queryMetadata.getLogicalSchema());
    }

    @Override
    public void start() {
      queryMetadata.start();
    }

    @Override
    public void stop() {
      queryMetadata.close();
    }

    @Override
    public BlockingRowQueue getQueue() {
      return queryMetadata.getRowQueue();
    }
  }
}
