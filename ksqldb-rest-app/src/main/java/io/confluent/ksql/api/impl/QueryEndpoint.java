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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.KsqlExecutionContext;
import io.confluent.ksql.api.server.MetricsCallbackHolder;
import io.confluent.ksql.api.server.QueryHandle;
import io.confluent.ksql.api.spi.QueryPublisher;
import io.confluent.ksql.config.SessionConfig;
import io.confluent.ksql.execution.pull.PullQueryResult;
import io.confluent.ksql.internal.PullQueryExecutorMetrics;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.parser.KsqlParser.ParsedStatement;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.query.BlockingRowQueue;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.rest.server.query.QueryExecutor;
import io.confluent.ksql.rest.server.query.QueryMetadataHolder;
import io.confluent.ksql.schema.ksql.Column;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.utils.FormatOptions;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.util.ConsistencyOffsetVector;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlStatementException;
import io.confluent.ksql.util.PushQueryMetadata;
import io.confluent.ksql.util.PushQueryMetadata.ResultType;
import io.confluent.ksql.util.VertxUtils;
import io.vertx.core.Context;
import io.vertx.core.WorkerExecutor;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.stream.Collectors;

// CHECKSTYLE_RULES.OFF: ClassDataAbstractionCoupling
public class QueryEndpoint {
  // CHECKSTYLE_RULES.ON: ClassDataAbstractionCoupling

  private final KsqlExecutionContext ksqlEngine;
  private final KsqlConfig ksqlConfig;
  private final Optional<PullQueryExecutorMetrics> pullQueryMetrics;
  private final QueryExecutor queryExecutor;

  // CHECKSTYLE_RULES.OFF: ParameterNumberCheck
  @SuppressFBWarnings(value = "EI_EXPOSE_REP2")
  public QueryEndpoint(
      // CHECKSTYLE_RULES.OFF: ParameterNumberCheck
      final KsqlExecutionContext ksqlEngine,
      final KsqlConfig ksqlConfig,
      final Optional<PullQueryExecutorMetrics> pullQueryMetrics,
      final QueryExecutor queryExecutor

  ) {
    this.ksqlEngine = ksqlEngine;
    this.ksqlConfig = ksqlConfig;
    this.pullQueryMetrics = pullQueryMetrics;
    this.queryExecutor = queryExecutor;
  }

  public QueryPublisher createQueryPublisher(
      final String sql,
      final Map<String, Object> properties,
      final Map<String, Object> sessionVariables,
      final Map<String, Object> requestProperties,
      final Context context,
      final WorkerExecutor workerExecutor,
      final ServiceContext serviceContext,
      final MetricsCallbackHolder metricsCallbackHolder,
      final Optional<Boolean> isInternalRequest) {
    // Must be run on worker as all this stuff is slow
    VertxUtils.checkIsWorker();

    final ConfiguredStatement<Query> statement = createStatement(
        sql, properties, sessionVariables);

    final QueryMetadataHolder queryMetadataHolder = queryExecutor.handleStatement(
        serviceContext,
        properties,
        requestProperties,
        statement.getPreparedStatement(),
        isInternalRequest,
        metricsCallbackHolder,
        context,
        false
    );

    if (queryMetadataHolder.getPullQueryResult().isPresent()) {
      final PullQueryResult result = queryMetadataHolder.getPullQueryResult().get();
      final BlockingQueryPublisher publisher = new BlockingQueryPublisher(context, workerExecutor);

      publisher.setQueryHandle(new KsqlPullQueryHandle(result, pullQueryMetrics,
          statement.getPreparedStatement().getMaskedStatementText()), true, false);

      // Start from the worker thread so that errors can bubble up, and we can get a proper response
      // code rather than waiting until later after the header has been written and all we can do
      // is write an error message.
      publisher.startFromWorkerThread();
      return publisher;
    } else if (queryMetadataHolder.getPushQueryMetadata().isPresent()) {
      final PushQueryMetadata metadata = queryMetadataHolder.getPushQueryMetadata().get();
      final BlockingQueryPublisher publisher = new BlockingQueryPublisher(context, workerExecutor);

      publisher.setQueryHandle(
          new KsqlQueryHandle(metadata),
          false,
          queryMetadataHolder.getScalablePushQueryMetadata().isPresent()
      );
      return publisher;
    } else {
      throw new KsqlStatementException(
          "Unexpected metadata for query",
          statement.getMaskedStatementText()
      );
    }
  }

  private ConfiguredStatement<Query> createStatement(final String queryString,
      final Map<String, Object> properties, final Map<String, Object> sessionVariables) {
    final List<ParsedStatement> statements = ksqlEngine.parse(queryString);
    if ((statements.size() != 1)) {
      throw new KsqlStatementException(
          String
              .format("Expected exactly one KSQL statement; found %d instead", statements.size()),
          queryString);
    }
    final PreparedStatement<?> ps = ksqlEngine.prepare(
        statements.get(0),
        sessionVariables.entrySet()
            .stream()
            .collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue().toString()))
    );
    final Statement statement = ps.getStatement();
    if (!(statement instanceof Query)) {
      throw new KsqlStatementException("Not a query", queryString);
    }
    @SuppressWarnings("unchecked") final PreparedStatement<Query> psq =
        (PreparedStatement<Query>) ps;
    return ConfiguredStatement.of(psq, SessionConfig.of(ksqlConfig, properties));
  }

  private static List<String> colTypesFromSchema(final List<Column> columns) {
    return columns.stream()
        .map(Column::type)
        .map(type -> type.toString(FormatOptions.none()))
        .collect(Collectors.toList());
  }

  private static List<String> colNamesFromSchema(final List<Column> columns) {
    return columns.stream()
        .map(Column::name)
        .map(ColumnName::text)
        .collect(Collectors.toList());
  }

  private static class KsqlQueryHandle implements QueryHandle {

    private final PushQueryMetadata queryMetadata;

    KsqlQueryHandle(final PushQueryMetadata queryMetadata) {
      this.queryMetadata = Objects.requireNonNull(queryMetadata, "queryMetadata");
    }

    @Override
    public List<String> getColumnNames() {
      return colNamesFromSchema(queryMetadata.getLogicalSchema().value());
    }

    @Override
    public List<String> getColumnTypes() {
      return colTypesFromSchema(queryMetadata.getLogicalSchema().value());
    }

    @Override
    public LogicalSchema getLogicalSchema() {
      return queryMetadata.getLogicalSchema();
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

    @Override
    public void onException(final Consumer<Throwable> onException) {
      queryMetadata.setUncaughtExceptionHandler(throwable  -> {
        onException.accept(throwable);
        return null;
      });
    }

    @Override
    public QueryId getQueryId() {
      return queryMetadata.getQueryId();
    }

    @Override
    public Optional<ConsistencyOffsetVector> getConsistencyOffsetVector() {
      return Optional.empty();
    }

    @Override
    public Optional<ResultType> getResultType() {
      return Optional.of(queryMetadata.getResultType());
    }
  }

  private static class KsqlPullQueryHandle implements QueryHandle {

    private final PullQueryResult result;
    private final Optional<PullQueryExecutorMetrics>  pullQueryMetrics;
    private final String maskedStatementText;
    private final CompletableFuture<Void> future = new CompletableFuture<>();

    KsqlPullQueryHandle(final PullQueryResult result,
        final Optional<PullQueryExecutorMetrics> pullQueryMetrics,
        final String maskedStatementText
    ) {
      this.result = Objects.requireNonNull(result);
      this.pullQueryMetrics = Objects.requireNonNull(pullQueryMetrics);
      this.maskedStatementText = maskedStatementText;
    }

    @Override
    public List<String> getColumnNames() {
      return colNamesFromSchema(result.getSchema().columns());
    }

    @Override
    public List<String> getColumnTypes() {
      return colTypesFromSchema(result.getSchema().columns());
    }

    @Override
    public LogicalSchema getLogicalSchema() {
      return result.getSchema();
    }

    @Override
    public void start() {
      try {
        result.onException(future::completeExceptionally);
        result.onCompletion(future::complete);
        result.start();
      } catch (Exception e) {
        pullQueryMetrics.ifPresent(metrics -> metrics.recordErrorRate(1, result.getSourceType(),
            result.getPlanType(), result.getRoutingNodeType()));
        // Let this error bubble up since start is called from the worker thread and will fail the
        // query.
        throw new KsqlStatementException("Error starting pull query: " + e.getMessage(),
            maskedStatementText, e);
      }
    }

    @Override
    public void stop() {
      result.stop();
    }

    @Override
    public BlockingRowQueue getQueue() {
      return result.getPullQueryQueue();
    }

    @Override
    public void onException(final Consumer<Throwable> onException) {
      future.exceptionally(t -> {
        onException.accept(t);
        return null;
      });
    }

    @Override
    public QueryId getQueryId() {
      return result.getQueryId();
    }

    @Override
    public Optional<ConsistencyOffsetVector> getConsistencyOffsetVector() {
      return result.getConsistencyOffsetVector();
    }

    @Override
    public Optional<ResultType> getResultType() {
      return Optional.empty();
    }
  }
}
