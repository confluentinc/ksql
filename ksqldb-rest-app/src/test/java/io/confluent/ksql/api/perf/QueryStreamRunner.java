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

import io.confluent.ksql.api.auth.ApiSecurityContext;
import io.confluent.ksql.api.impl.BlockingQueryPublisher;
import io.confluent.ksql.api.server.InsertResult;
import io.confluent.ksql.api.server.InsertsStreamSubscriber;
import io.confluent.ksql.api.server.MetricsCallbackHolder;
import io.confluent.ksql.api.server.QueryHandle;
import io.confluent.ksql.api.spi.Endpoints;
import io.confluent.ksql.api.spi.QueryPublisher;
import io.confluent.ksql.query.BlockingRowQueue;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.query.TransientQueryQueue;
import io.confluent.ksql.rest.EndpointResponse;
import io.confluent.ksql.rest.entity.ClusterTerminateRequest;
import io.confluent.ksql.rest.entity.HeartbeatMessage;
import io.confluent.ksql.rest.entity.KsqlMediaType;
import io.confluent.ksql.rest.entity.KsqlRequest;
import io.confluent.ksql.rest.entity.LagReportingMessage;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.util.ConsistencyOffsetVector;
import io.confluent.ksql.util.PushQueryMetadata.ResultType;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.MultiMap;
import io.vertx.core.WorkerExecutor;
import io.vertx.core.http.ServerWebSocket;
import io.vertx.core.json.JsonObject;
import io.vertx.core.parsetools.RecordParser;
import io.vertx.ext.web.codec.BodyCodec;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

public class QueryStreamRunner extends BasePerfRunner {

  private static final String DEFAULT_PUSH_QUERY = "select * from foo emit changes;";
  private static final JsonObject DEFAULT_PUSH_QUERY_REQUEST_BODY = new JsonObject()
      .put("sql", DEFAULT_PUSH_QUERY)
      .put("properties", new JsonObject().put("auto.offset.reset", "earliest"));

  public static void main(String[] args) {
    new QueryStreamRunner().go();
  }

  private QueryStreamEndpoints queryStreamEndpoints;

  @Override
  protected void configure() {
    this.queryStreamEndpoints = new QueryStreamEndpoints();
    setNumWarmupRuns(5).setNumRuns(5).setRunMs(10000).setEndpoints(queryStreamEndpoints);
  }

  @Override
  protected void run(long ms) throws Exception {

    RecordParser parser = RecordParser.newDelimited("\n").handler(row -> count());

    client.post(8089, "localhost", "/query-stream")
        .as(BodyCodec.pipe(new RunnerUtils.ReceiveStream(parser)))
        .sendJsonObject(DEFAULT_PUSH_QUERY_REQUEST_BODY, ar -> {
        });

    Thread.sleep(ms);
  }

  @Override
  protected void endRun() throws Exception {
    queryStreamEndpoints.closePublishers();

    Thread.sleep(500);
  }

  private class QueryStreamEndpoints implements Endpoints {

    private final Set<QueryStreamPublisher> publishers = new HashSet<>();

    @Override
    public synchronized CompletableFuture<Publisher<?>> createQueryPublisher(final String sql,
        final Map<String, Object> properties,
        final Map<String, Object> sessionVariables,
        final Map<String, Object> requestProperties,
        final Context context,
        final WorkerExecutor workerExecutor,
        final ApiSecurityContext apiSecurityContext,
        final MetricsCallbackHolder metricsCallbackHolder,
        final Optional<Boolean> isInternalRequest) {
      QueryStreamPublisher publisher = new QueryStreamPublisher(context,
          server.getWorkerExecutor());
      publisher.setQueryHandle(new TestQueryHandle(), false, false);
      publishers.add(publisher);
      publisher.start();
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
        final WorkerExecutor workerExecutor,
        final ApiSecurityContext apiSecurityContext) {
      return null;
    }

    @Override
    public CompletableFuture<EndpointResponse> executeQueryRequest(
        KsqlRequest request,
        WorkerExecutor workerExecutor,
        CompletableFuture<Void> connectionClosedFuture,
        ApiSecurityContext apiSecurityContext,
        Optional<Boolean> isInternalRequest,
        KsqlMediaType mediaType,
        final MetricsCallbackHolder metricsCallbackHolder,
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

    @Override
    public CompletableFuture<EndpointResponse> executeTest(String test,
        ApiSecurityContext apiSecurityContext) {
      return null;
    }

    synchronized void closePublishers() {
      for (QueryStreamPublisher publisher : publishers) {
        publisher.close();
      }
    }
  }

  private static class TestQueryHandle implements QueryHandle {

    private final TransientQueryQueue queue = new TransientQueryQueue(OptionalInt.empty());

    @Override
    public List<String> getColumnNames() {
      return DEFAULT_COLUMN_NAMES;
    }

    @Override
    public List<String> getColumnTypes() {
      return DEFAULT_COLUMN_TYPES;
    }

    @Override
    public LogicalSchema getLogicalSchema() {
      return SCHEMA;
    }

    @Override
    public BlockingRowQueue getQueue() {
      return queue;
    }

    @Override
    public void onException(Consumer<Throwable> onException) {
    }

    @Override
    public QueryId getQueryId() {
      return new QueryId("queryId");
    }

    @Override
    public Optional<ConsistencyOffsetVector> getConsistencyOffsetVector() {
      return Optional.empty();
    }

    @Override
    public Optional<ResultType> getResultType() {
      return Optional.empty();
    }

    @Override
    public void start() {
    }

    @Override
    public void stop() {
    }
  }

  private static class QueryStreamPublisher extends BlockingQueryPublisher implements Runnable {

    private static final int SEND_BATCH_SIZE = 200;
    private volatile boolean closed;
    private Thread thread;
    private TransientQueryQueue queue;

    public QueryStreamPublisher(final Context ctx, final WorkerExecutor workerExecutor) {
      super(ctx, workerExecutor);
    }

    public void start() {
      thread = new Thread(this);
      thread.start();
    }

    @Override
    public void setQueryHandle(final QueryHandle queryHandle, boolean isPullQuery,
        boolean isScalablePushQuery) {
      this.queue = (TransientQueryQueue) queryHandle.getQueue();
      super.setQueryHandle(queryHandle, isPullQuery, isScalablePushQuery);
    }

    @Override
    public Future<Void> close() {
      closed = true;
      try {
        thread.join();
      } catch (InterruptedException ignore) {
        // Ignore
      }
      return Future.succeededFuture();
    }

    public void run() {
      while (!closed) {
        for (int i = 0; i < SEND_BATCH_SIZE; i++) {
          queue.acceptRow(DEFAULT_KEY, DEFAULT_ROW);
        }
      }
    }
  }
}
