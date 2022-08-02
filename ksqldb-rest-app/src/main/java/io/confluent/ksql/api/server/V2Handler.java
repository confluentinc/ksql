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

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.KsqlLang;
import io.confluent.ksql.api.spi.Endpoints;
import io.confluent.ksql.api.util.ApiServerUtils;
import io.confluent.ksql.rest.entity.KsqlRequest;
import io.confluent.ksql.rest.entity.QueryStreamArgs;
import io.confluent.ksql.util.KsqlRateLimitException;
import io.confluent.ksql.util.KsqlStatementException;
import io.confluent.ksql.util.VertxCompletableFuture;
import io.vertx.core.Context;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.http.HttpVersion;
import io.vertx.ext.web.RoutingContext;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.AbstractList;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletionException;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.tools.RelRunners;
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

  @SuppressFBWarnings(value = "EI_EXPOSE_REP2")
  public V2Handler(final Endpoints endpoints,
                   final ConnectionQueryManager connectionQueryManager,
                   final Context context,
                   final Server server,
                   final boolean queryCompatibilityMode
  ) {
    this.endpoints = Objects.requireNonNull(endpoints);
    this.connectionQueryManager = Objects.requireNonNull(connectionQueryManager);
    this.context = Objects.requireNonNull(context);
    this.server = Objects.requireNonNull(server);
    this.queryCompatibilityMode = queryCompatibilityMode;
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

    final VertxCompletableFuture<RelRoot> vcf = new VertxCompletableFuture<>();
    final KsqlLang ksqlLang = new KsqlLang();
    server
        .getWorkerExecutor()
        .executeBlocking(
            promise -> promise.complete(ksqlLang.getLogicalPlan(sql)),
            false,
            vcf
        );

    vcf.thenAccept(logicalPlan -> {
      final HttpServerResponse response = routingContext.response();
      response.write(logicalPlan.toString() + "\n\n");
      try (final PreparedStatement run = RelRunners.run(logicalPlan.rel);
           final ResultSet resultSet = run.executeQuery()) {
        final int columnCount = resultSet.getMetaData().getColumnCount();
        for (int i = 0; i < columnCount; i++) {
          response.write(resultSet.getMetaData().getColumnName(i)).write(" | ");
        }
        response.write("\n");
        while (resultSet.next()) {
          for (int i = 0; i < columnCount; i++) {
            response.write(String.valueOf(resultSet.getObject(i))).write(" | ");
          }
          response.write("\n");
        }
      } catch (SQLException e) {
        throw new RuntimeException(e);
      }
      response.end();
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
