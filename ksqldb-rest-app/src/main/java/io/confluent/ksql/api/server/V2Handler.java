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

package io.confluent.ksql.api.server;

import com.google.common.collect.ImmutableList;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.KsqlLang;
import io.confluent.ksql.KsqlLogicalPlanner;
import io.confluent.ksql.KsqlSimpleExecutor;
import io.confluent.ksql.Table;
import io.confluent.ksql.api.spi.Endpoints;
import io.confluent.ksql.api.util.ApiServerUtils;
import io.confluent.ksql.rest.entity.KsqlRequest;
import io.confluent.ksql.rest.entity.QueryStreamArgs;
import io.confluent.ksql.util.KsqlRateLimitException;
import io.confluent.ksql.util.KsqlStatementException;
import io.confluent.ksql.util.VertxCompletableFuture;
import io.vertx.core.Context;
import io.vertx.core.Handler;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.http.HttpVersion;
import io.vertx.ext.web.RoutingContext;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import javax.swing.tree.RowMapper;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.sql.SqlKind;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.confluent.ksql.rest.Errors.ERROR_CODE_BAD_REQUEST;
import static io.confluent.ksql.rest.Errors.ERROR_CODE_BAD_STATEMENT;
import static io.confluent.ksql.rest.Errors.ERROR_CODE_SERVER_ERROR;
import static io.confluent.ksql.rest.Errors.ERROR_CODE_TOO_MANY_REQUESTS;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static io.netty.handler.codec.http.HttpResponseStatus.TOO_MANY_REQUESTS;
import static org.apache.hc.core5.http.HeaderElements.CHUNKED_ENCODING;
import static org.apache.hc.core5.http.HttpHeaders.TRANSFER_ENCODING;

/**
 * Handles requests to the query-stream endpoint
 */
@SuppressWarnings({"ClassDataAbstractionCoupling"})
public class V2Handler implements Handler<RoutingContext> {

  private static final Logger log = LoggerFactory.getLogger(V2Handler.class);

  private final Endpoints endpoints;
  private final ConnectionQueryManager connectionQueryManager;
  private final Context context;
  private final Server server;
  private final boolean queryCompatibilityMode;
  private KsqlLang ksqlLang;
  private KsqlSimpleExecutor ksqlSimpleExecutor;

  @SuppressFBWarnings(value = "EI_EXPOSE_REP2")
  public V2Handler(final Endpoints endpoints,
                   final ConnectionQueryManager connectionQueryManager,
                   final Context context,
                   final Server server,
                   final boolean queryCompatibilityMode,
                   final KsqlLang ksqlLang,
                   final KsqlSimpleExecutor ksqlSimpleExecutor) {
    this.endpoints = Objects.requireNonNull(endpoints);
    this.connectionQueryManager = Objects.requireNonNull(connectionQueryManager);
    this.context = Objects.requireNonNull(context);
    this.server = Objects.requireNonNull(server);
    this.queryCompatibilityMode = queryCompatibilityMode;
    this.ksqlLang = ksqlLang;
    this.ksqlSimpleExecutor = ksqlSimpleExecutor;
  }

  @Override
  public void handle(final RoutingContext routingContext) {
    // We must set it to allow chunked encoding if we're using http1.1
    if (routingContext.request().version() == HttpVersion.HTTP_1_1) {
      routingContext.response().putHeader(TRANSFER_ENCODING, CHUNKED_ENCODING);
    } else if (routingContext.request().version() == HttpVersion.HTTP_2) {
      // Nothing required
    } else {
      routingContext.fail(BAD_REQUEST.code(),
          new KsqlApiException("This endpoint is only available when using HTTP1.1 or HTTP2",
              ERROR_CODE_BAD_REQUEST));
    }

    final CommonRequest request = getRequest(routingContext);
    if (request == null) {
      return;
    }

    final Optional<Boolean> internalRequest = ServerVerticle.isInternalRequest(routingContext);
    final MetricsCallbackHolder metricsCallbackHolder = new MetricsCallbackHolder();
    final long startTimeNanos = Time.SYSTEM.nanoseconds();

    final String sql = request.sql;

    final VertxCompletableFuture<KsqlLogicalPlanner.KsqlLogicalPlan> planFuture =
        new VertxCompletableFuture<>();
    server
        .getWorkerExecutor()
        .executeBlocking(
            promise -> promise.complete(ksqlLang.getLogicalPlan(sql)),
            false,
            planFuture
        );

    final CompletableFuture<KsqlSimpleExecutor.KsqlResultSet> resultFuture =
        planFuture.thenApply(logicalPlan -> {
          final SqlKind kind = logicalPlan.getRelRoot().kind;
          switch (kind) {
            case SELECT:
              return ksqlSimpleExecutor.execute(logicalPlan);
            default:
              throw new KsqlStatementException(
                  kind + " expressions are not yet supported by the new front-end.",
                  logicalPlan.getOriginalStatement()
              );
          }
        });

    resultFuture.thenAccept(resultSet -> {
      final HttpServerResponse response = routingContext.response();
      response.putHeader("statement", resultSet.getLogicalPlan().getOriginalStatement());
      response.putHeader("logicalPlan", resultSet.getLogicalPlan().getRelRoot().toString());
      try {
        print(resultSet.getResultSet(), response);
        response.end();
      } catch (SQLException e) {
        throw new KsqlStatementException(
            "Could not print result",
            resultSet.getLogicalPlan().getOriginalStatement(),
            e
        );
      }
    }).exceptionally(t -> {
      if (t instanceof CompletionException) {
        final Throwable actual = t.getCause();
        log.error("Failed to execute query", actual);
        if (actual instanceof KsqlStatementException) {
          routingContext.fail(BAD_REQUEST.code(),
              new KsqlApiException(actual.getMessage(), ERROR_CODE_BAD_STATEMENT,
                  ((KsqlStatementException) actual).getSqlStatement()));
          return null;
        } else if (actual instanceof KsqlRateLimitException) {
          routingContext.fail(TOO_MANY_REQUESTS.code(),
              new KsqlApiException(actual.getMessage(), ERROR_CODE_TOO_MANY_REQUESTS));
          return null;
        } else if (actual instanceof KsqlApiException) {
          routingContext.fail(BAD_REQUEST.code(), actual);
          return null;
        }
      } else {
        log.error("Failed to execute query", t);
      }
      // We don't expose internal error message via public API
      routingContext.fail(INTERNAL_SERVER_ERROR.code(), new KsqlApiException(
          "The server encountered an internal error when processing the query."
              + " Please consult the server logs for more information.",
          ERROR_CODE_SERVER_ERROR));
      return null;
    });
  }

  private static void print(final ResultSet resultSet, final HttpServerResponse response) throws SQLException {
    final Table.Builder builder = new Table.Builder();
    final ResultSetMetaData metaData = resultSet.getMetaData();
    final int columnCount = metaData.getColumnCount();
    final List<String> headers = new ArrayList<>(columnCount - 1);
    for (int i = 1; i <= columnCount; i++) {
      final String columnLabel = metaData.getColumnLabel(i);
      headers.add(i - 1, columnLabel);
    }
    builder.withColumnHeaders(headers);
    int rowCount = 0;
    while (resultSet.next()) {
      final List<String> row = new ArrayList<>(columnCount - 1);
      for (int i = 1; i <= columnCount; i++) {
        final String value = resultSet.getObject(i).toString();
        row.add(i - 1, value);
      }
      builder.withRow(row);
      rowCount++;
    }
    builder.withFooterLine(rowCount + " rows");
    builder.build().print(response);

  }

  private CommonRequest getRequest(final RoutingContext routingContext) {
    final String sql;
    final Map<String, Object> configOverrides;
    final Map<String, Object> sessionProperties;
    final Map<String, Object> requestProperties;
    if (queryCompatibilityMode) {
      final Optional<KsqlRequest> ksqlRequest = ServerUtils
          .deserialiseObject(routingContext.getBody(), routingContext, KsqlRequest.class);
      if (!ksqlRequest.isPresent()) {
        return null;
      }
      // Set masked sql statement if request is not from OldApiUtils.handleOldApiRequest
      ApiServerUtils.setMaskedSqlIfNeeded(ksqlRequest.get());
      sql = ksqlRequest.get().getUnmaskedKsql();
      configOverrides = ksqlRequest.get().getConfigOverrides();
      sessionProperties = ksqlRequest.get().getSessionVariables();
      requestProperties = ksqlRequest.get().getRequestProperties();
    } else {
      final Optional<QueryStreamArgs> queryStreamArgs = ServerUtils
          .deserialiseObject(routingContext.getBody(), routingContext, QueryStreamArgs.class);
      if (!queryStreamArgs.isPresent()) {
        return null;
      }
      sql = queryStreamArgs.get().sql;
      configOverrides = queryStreamArgs.get().properties;
      sessionProperties = queryStreamArgs.get().sessionVariables;
      requestProperties = queryStreamArgs.get().requestProperties;
    }
    return new CommonRequest(sql, configOverrides, sessionProperties, requestProperties);
  }

  private static class CommonRequest {
    final String sql;
    final Map<String, Object> configOverrides;
    final Map<String, Object> sessionProperties;
    final Map<String, Object> requestProperties;

    CommonRequest(
        final String sql,
        final Map<String, Object> configOverrides,
        final Map<String, Object> sessionProperties,
        final Map<String, Object> requestProperties
    ) {
      this.sql = sql;
      this.configOverrides = configOverrides;
      this.sessionProperties = sessionProperties;
      this.requestProperties = requestProperties;
    }
  }
}
