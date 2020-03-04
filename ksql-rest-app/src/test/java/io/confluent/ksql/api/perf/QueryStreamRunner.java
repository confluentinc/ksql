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
import static io.confluent.ksql.api.perf.RunnerUtils.DEFAULT_ROW;

import io.confluent.ksql.api.auth.ApiSecurityContext;
import io.confluent.ksql.api.endpoints.BlockingQueryPublisher;
import io.confluent.ksql.api.server.InsertResult;
import io.confluent.ksql.api.server.InsertsStreamSubscriber;
import io.confluent.ksql.api.server.PushQueryHandle;
import io.confluent.ksql.api.spi.EndpointResponse;
import io.confluent.ksql.api.spi.Endpoints;
import io.confluent.ksql.api.spi.QueryPublisher;
import io.confluent.ksql.rest.entity.KsqlRequest;
import io.vertx.core.Context;
import io.vertx.core.WorkerExecutor;
import io.vertx.core.json.JsonObject;
import io.vertx.core.parsetools.RecordParser;
import io.vertx.ext.web.codec.BodyCodec;
import java.util.HashSet;
import java.util.List;
import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.reactivestreams.Subscriber;

public class QueryStreamRunner extends BasePerfRunner {

  private static final String DEFAULT_PUSH_QUERY = "select * from foo emit changes;";
  private static final JsonObject DEFAULT_PUSH_QUERY_REQUEST_BODY = new JsonObject()
      .put("sql", DEFAULT_PUSH_QUERY)
      .put("properties", new JsonObject());

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
    public synchronized CompletableFuture<QueryPublisher> createQueryPublisher(final String sql,
        final JsonObject properties,
        final Context context,
        final WorkerExecutor workerExecutor,
        final ApiSecurityContext apiSecurityContext) {
      QueryStreamPublisher publisher = new QueryStreamPublisher(context,
          server.getWorkerExecutor());
      publisher.setQueryHandle(new TestQueryHandle());
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

    synchronized void closePublishers() {
      for (QueryStreamPublisher publisher : publishers) {
        publisher.close();
      }
    }
  }

  private static class TestQueryHandle implements PushQueryHandle {

    @Override
    public List<String> getColumnNames() {
      return DEFAULT_COLUMN_NAMES;
    }

    @Override
    public List<String> getColumnTypes() {
      return DEFAULT_COLUMN_TYPES;
    }

    @Override
    public OptionalInt getLimit() {
      return OptionalInt.empty();
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

    public QueryStreamPublisher(final Context ctx, final WorkerExecutor workerExecutor) {
      super(ctx, workerExecutor);
    }

    public void start() {
      thread = new Thread(this);
      thread.start();
    }

    public void close() {
      closed = true;
      try {
        thread.join();
      } catch (InterruptedException ignore) {
        // Ignore
      }
    }

    public void run() {
      while (!closed) {
        for (int i = 0; i < SEND_BATCH_SIZE; i++) {
          accept(DEFAULT_ROW);
        }
      }
    }
  }

}
