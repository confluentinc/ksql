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

import io.confluent.ksql.api.auth.ApiSecurityContext;
import io.confluent.ksql.api.server.InsertResult;
import io.confluent.ksql.api.server.InsertsStreamSubscriber;
import io.confluent.ksql.api.server.MetricsCallbackHolder;
import io.confluent.ksql.api.spi.Endpoints;
import io.confluent.ksql.reactive.BasePublisher;
import io.confluent.ksql.reactive.BaseSubscriber;
import io.confluent.ksql.reactive.BufferedPublisher;
import io.confluent.ksql.rest.EndpointResponse;
import io.confluent.ksql.rest.entity.ClusterTerminateRequest;
import io.confluent.ksql.rest.entity.HeartbeatMessage;
import io.confluent.ksql.rest.entity.KsqlMediaType;
import io.confluent.ksql.rest.entity.KsqlRequest;
import io.confluent.ksql.rest.entity.LagReportingMessage;
import io.vertx.codegen.annotations.Nullable;
import io.vertx.core.Context;
import io.vertx.core.Handler;
import io.vertx.core.MultiMap;
import io.vertx.core.Vertx;
import io.vertx.core.WorkerExecutor;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.ServerWebSocket;
import io.vertx.core.json.JsonObject;
import io.vertx.core.parsetools.RecordParser;
import io.vertx.core.streams.ReadStream;
import io.vertx.ext.web.codec.BodyCodec;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

public class InsertsStreamRunner extends BasePerfRunner {

  public static void main(String[] args) {
    new InsertsStreamRunner().go();
  }

  private SendStream sendStream;

  @Override
  public void configure() {
    setNumWarmupRuns(5).setNumRuns(5).setRunMs(10000).setEndpoints(new InsertsStreamEndpoints());
  }

  @Override
  public void run(long ms) throws Exception {

    RecordParser parser = RecordParser.newDelimited("\n").handler(row -> {
      count();
    });

    sendStream = new SendStream(vertx);

    client.post(8089, "localhost", "/inserts-stream")
        .as(BodyCodec.pipe(new RunnerUtils.ReceiveStream(parser)))
        .sendStream(sendStream, ar -> {
        });

    Thread.sleep(ms);

  }

  @Override
  protected void endRun() throws Exception {
    sendStream.pause();

    Thread.sleep(500);
  }

  // Just send as fast as we can
  private static class SendStream implements ReadStream<Buffer> {

    private static final int SEND_BATCH_SIZE = 200;

    private static final Buffer REQUEST_ARGS = new JsonObject().put("target", "whatever")
        .put("properties", new JsonObject()).toBuffer().appendString("\n");

    private static final Buffer ROW = new JsonObject().put("name", "tim").put("age", 105)
        .put("male", true).toBuffer()
        .appendString("\n");

    private final Vertx vertx;
    private Handler<Buffer> handler;
    private boolean paused;
    private boolean sentFirst;

    public SendStream(final Vertx vertx) {
      this.vertx = vertx;
    }

    private synchronized void checkSend() {
      Context context = vertx.getOrCreateContext();
      if (!paused && handler != null) {
        if (!sentFirst) {
          handler.handle(REQUEST_ARGS);
          sentFirst = true;
        } else {
          for (int i = 0; i < SEND_BATCH_SIZE; i++) {
            handler.handle(ROW);
          }
        }
        context.runOnContext(v -> checkSend());
      }
    }

    @Override
    public ReadStream<Buffer> exceptionHandler(final Handler<Throwable> handler) {
      return this;
    }

    @Override
    public synchronized ReadStream<Buffer> handler(@Nullable final Handler<Buffer> handler) {
      this.handler = handler;
      checkSend();
      return this;
    }

    @Override
    public synchronized ReadStream<Buffer> pause() {
      paused = true;
      return this;
    }

    @Override
    public synchronized ReadStream<Buffer> resume() {
      paused = false;
      checkSend();
      return this;
    }

    @Override
    public ReadStream<Buffer> fetch(final long amount) {
      return this;
    }

    @Override
    public ReadStream<Buffer> endHandler(@Nullable final Handler<Void> endHandler) {
      return this;
    }
  }

  private class InsertsStreamEndpoints implements Endpoints {

    @Override
    public CompletableFuture<Publisher<?>> createQueryPublisher(final String sql,
        final Map<String, Object> properties,
        final Map<String, Object> sessionVariables,
        final Map<String, Object> requestProperties,
        final Context context,
        final WorkerExecutor workerExecutor,
        final ApiSecurityContext apiSecurityContext,
        final MetricsCallbackHolder metricsCallbackHolder,
        final Optional<Boolean> isInternalRequest) {
      return null;
    }

    @Override
    public CompletableFuture<InsertsStreamSubscriber> createInsertsSubscriber(final String target,
        final JsonObject properties,
        final Subscriber<InsertResult> acksSubscriber, final Context context,
        final WorkerExecutor workerExecutor,
        final ApiSecurityContext apiSecurityContext) {
      return CompletableFuture.completedFuture(new InsertsSubscriber(context, acksSubscriber));
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

    @Override
    public CompletableFuture<EndpointResponse> executeTest(String test,
        ApiSecurityContext apiSecurityContext) {
      return null;
    }
  }

  private class InsertsSubscriber extends BaseSubscriber<JsonObject> implements
      InsertsStreamSubscriber {

    private static final int REQUEST_BATCH_SIZE = 200;
    private int tokens;
    private long seq;

    private BufferedPublisher<InsertResult> publisher;

    public InsertsSubscriber(final Context context, final Subscriber<InsertResult> acksSubscriber) {
      super(context);
      publisher = new BufferedPublisher<>(context);
      publisher.subscribe(acksSubscriber);
    }

    @Override
    protected void afterSubscribe(final Subscription subscription) {
      checkRequestTokens();
    }

    @Override
    protected void handleValue(final JsonObject row) {
      publisher.accept(InsertResult.succeededInsert(seq++));
      tokens--;
      checkRequestTokens();
    }

    @Override
    protected void handleError(final Throwable t) {
      errorOccurred(t);
    }

    private void checkRequestTokens() {
      if (tokens == 0) {
        tokens = REQUEST_BATCH_SIZE;
        makeRequest(REQUEST_BATCH_SIZE);
      }
    }

    @Override
    public void close() {
    }
  }


}
