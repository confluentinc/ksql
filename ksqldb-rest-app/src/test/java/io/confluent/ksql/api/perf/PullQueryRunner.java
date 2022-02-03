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

package io.confluent.ksql.api.perf;

import static io.confluent.ksql.api.perf.RunnerUtils.DEFAULT_COLUMN_NAMES;
import static io.confluent.ksql.api.perf.RunnerUtils.DEFAULT_COLUMN_TYPES;
import static io.confluent.ksql.api.perf.RunnerUtils.DEFAULT_KEY;
import static io.confluent.ksql.api.perf.RunnerUtils.DEFAULT_ROW;
import static io.confluent.ksql.api.perf.RunnerUtils.SCHEMA;
import static io.confluent.ksql.util.KeyValue.keyValue;

import io.confluent.ksql.GenericRow;
import io.confluent.ksql.api.auth.ApiSecurityContext;
import io.confluent.ksql.api.server.InsertResult;
import io.confluent.ksql.api.server.InsertsStreamSubscriber;
import io.confluent.ksql.api.server.MetricsCallbackHolder;
import io.confluent.ksql.api.spi.Endpoints;
import io.confluent.ksql.api.spi.QueryPublisher;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.reactive.BufferedPublisher;
import io.confluent.ksql.rest.EndpointResponse;
import io.confluent.ksql.rest.entity.ClusterTerminateRequest;
import io.confluent.ksql.rest.entity.HeartbeatMessage;
import io.confluent.ksql.rest.entity.KsqlMediaType;
import io.confluent.ksql.rest.entity.KsqlRequest;
import io.confluent.ksql.rest.entity.LagReportingMessage;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.util.KeyValue;
import io.confluent.ksql.util.KeyValueMetadata;
import io.confluent.ksql.util.PushQueryMetadata.ResultType;
import io.confluent.ksql.util.VertxCompletableFuture;
import io.vertx.core.Context;
import io.vertx.core.MultiMap;
import io.vertx.core.WorkerExecutor;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.ServerWebSocket;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.HttpResponse;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import org.reactivestreams.Subscriber;

public class PullQueryRunner extends BasePerfRunner {

  public static void main(String[] args) {
    new PullQueryRunner().go();
  }

  private static final String DEFAULT_PULL_QUERY = "select * from foo where rowkey=123;";
  private static final JsonObject DEFAULT_PULL_QUERY_REQUEST_BODY = new JsonObject()
      .put("sql", DEFAULT_PULL_QUERY)
      .put("properties", new JsonObject().put("auto.offset.reset", "earliest"));
  private static final List<KeyValueMetadata<List<?>, GenericRow>> DEFAULT_ROWS = generateResults();
  private static final int MAX_CONCURRENT_REQUESTS = 100;

  private PullQueryEndpoints pullQueryEndpoints;

  @Override
  protected void configure() {
    this.pullQueryEndpoints = new PullQueryEndpoints();
    setNumWarmupRuns(5).setNumRuns(5).setRunMs(10000).setEndpoints(pullQueryEndpoints);
  }

  @Override
  protected void run(long runMs) throws Exception {
    Semaphore sem = new Semaphore(MAX_CONCURRENT_REQUESTS);

    long start = System.currentTimeMillis();

    do {

      sem.acquire();

      VertxCompletableFuture<HttpResponse<Buffer>> vcf = new VertxCompletableFuture<>();

      client.post(8089, "localhost", "/query-stream")
          .sendJsonObject(DEFAULT_PULL_QUERY_REQUEST_BODY, vcf);

      vcf.thenAccept(resp -> {
        count();
        sem.release();
      });

    } while (System.currentTimeMillis() - start < runMs);
  }

  @Override
  protected void endRun() throws Exception {
    pullQueryEndpoints.closePublishers();

    Thread.sleep(500);
  }

  private static List<KeyValueMetadata<List<?>, GenericRow>> generateResults() {
    final List<KeyValueMetadata<List<?>, GenericRow>> results = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      results.add(new KeyValueMetadata<>(keyValue(DEFAULT_KEY, DEFAULT_ROW)));
    }
    return results;
  }

  private static class PullQueryEndpoints implements Endpoints {

    private final Set<PullQueryPublisher> publishers = new HashSet<>();

    @Override
    public synchronized CompletableFuture<QueryPublisher> createQueryPublisher(final String sql,
        final Map<String, Object> properties,
        final Map<String, Object> sessionVariables,
        final Map<String, Object> requestProperties,
        final Context context,
        final WorkerExecutor workerExecutor,
        final ApiSecurityContext apiSecurityContext,
        final MetricsCallbackHolder metricsCallbackHolder,
        final Optional<Boolean> isInternalRequest) {
      PullQueryPublisher publisher = new PullQueryPublisher(context, DEFAULT_ROWS);
      publishers.add(publisher);
      return CompletableFuture.completedFuture(publisher);
    }

    @Override
    public CompletableFuture<InsertsStreamSubscriber> createInsertsSubscriber(final String target,
        final JsonObject properties,
        final Subscriber<InsertResult> acksSubscriber, final Context context,
        final WorkerExecutor workerExecutor,
        final ApiSecurityContext apiSecurityContext) {
      return null;
    }

    @Override
    public CompletableFuture<EndpointResponse> executeKsqlRequest(final KsqlRequest request,
        final WorkerExecutor workerExecutor, final ApiSecurityContext apiSecurityContext) {
      return null;
    }

    @Override
    public CompletableFuture<EndpointResponse> executeTerminate(
        final ClusterTerminateRequest request,
        final WorkerExecutor workerExecutor, final ApiSecurityContext apiSecurityContext) {
      return null;
    }

    @Override
    public CompletableFuture<EndpointResponse> executeQueryRequest(KsqlRequest request,
        WorkerExecutor workerExecutor, CompletableFuture<Void> connectionClosedFuture,
        ApiSecurityContext apiSecurityContext, Optional<Boolean> isInternalRequest,
        KsqlMediaType mediaType, final MetricsCallbackHolder metricsCallbackHolder,
        Context context) {
      return null;
    }

    @Override
    public CompletableFuture<EndpointResponse> executeInfo(ApiSecurityContext apiSecurityContext) {
      return null;
    }

    @Override
    public CompletableFuture<EndpointResponse> executeHeartbeat(HeartbeatMessage heartbeatMessage,
        ApiSecurityContext apiSecurityContext) {
      return null;
    }

    @Override
    public CompletableFuture<EndpointResponse> executeClusterStatus(
        ApiSecurityContext apiSecurityContext) {
      return null;
    }

    @Override
    public CompletableFuture<EndpointResponse> executeStatus(String type, String entity,
        String action, ApiSecurityContext apiSecurityContext) {
      return null;
    }

    @Override
    public CompletableFuture<EndpointResponse> executeIsValidProperty(String property,
        WorkerExecutor workerExecutor, ApiSecurityContext apiSecurityContext) {
      return null;
    }

    @Override
    public CompletableFuture<EndpointResponse> executeAllStatuses(
        ApiSecurityContext apiSecurityContext) {
      return null;
    }

    @Override
    public CompletableFuture<EndpointResponse> executeLagReport(
        LagReportingMessage lagReportingMessage, ApiSecurityContext apiSecurityContext) {
      return null;
    }

    @Override
    public CompletableFuture<EndpointResponse> executeCheckHealth(
        ApiSecurityContext apiSecurityContext) {
      return null;
    }

    @Override
    public CompletableFuture<EndpointResponse> executeServerMetadata(
        ApiSecurityContext apiSecurityContext) {
      return null;
    }

    @Override
    public CompletableFuture<EndpointResponse> executeServerMetadataClusterId(
        ApiSecurityContext apiSecurityContext) {
      return null;
    }

    @Override
    public void executeWebsocketStream(ServerWebSocket webSocket, MultiMap requstParams,
        WorkerExecutor workerExecutor, ApiSecurityContext apiSecurityContext, Context context) {

    }

    synchronized void closePublishers() {
      for (PullQueryPublisher publisher : publishers) {
        publisher.close();
      }
    }
  }

  private static class PullQueryPublisher
      extends BufferedPublisher<KeyValueMetadata<List<?>, GenericRow>>
      implements QueryPublisher {

    public PullQueryPublisher(final Context ctx, List<KeyValueMetadata<List<?>, GenericRow>> rows) {
      super(ctx, rows);
    }

    @Override
    public List<String> getColumnNames() {
      return DEFAULT_COLUMN_NAMES;
    }

    @Override
    public List<String> getColumnTypes() {
      return DEFAULT_COLUMN_TYPES;
    }

    @Override
    public LogicalSchema geLogicalSchema() {
      return SCHEMA;
    }

    @Override
    public boolean isPullQuery() {
      return true;
    }

    @Override
    public boolean isScalablePushQuery() {
      return false;
    }

    @Override
    public QueryId queryId() {
      return new QueryId("queryId");
    }

    @Override
    public boolean hitLimit() {
      return false;
    }

    @Override
    public Optional<ResultType> getResultType() {
      return Optional.empty();
    }
  }
}
