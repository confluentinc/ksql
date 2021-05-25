/*
 * Copyright 2021 Confluent Inc.
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

package io.confluent.ksql.physical.scalablepush;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.physical.pull.HARouting;
import io.confluent.ksql.physical.scalablepush.locator.PushLocator.KsqlNode;
import io.confluent.ksql.query.TransientQueryQueue;
import io.confluent.ksql.reactive.BaseSubscriber;
import io.confluent.ksql.reactive.BufferedPublisher;
import io.confluent.ksql.rest.client.RestResponse;
import io.confluent.ksql.rest.entity.StreamedRow;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.KsqlRequestConfig;
import io.vertx.core.Context;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PushRouting implements AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(HARouting.class);
  private static final long TIMEOUT_CONNECT_MS = 3000;

  private final ExecutorService executorService;

  public PushRouting(
      final KsqlConfig ksqlConfig
  ) {
    // This thread pool is used to make connections to other servers, but not while sending rows
    // (which is async).
    this.executorService = Executors.newFixedThreadPool(
        ksqlConfig.getInt(KsqlConfig.KSQL_QUERY_PUSH_SCALABLE_THREAD_POOL_SIZE_CONFIG),
        new ThreadFactoryBuilder().setNameFormat("scalable-push-query-executor-%d").build());
  }

  @Override
  public void close() {
    executorService.shutdown();
  }

  public CompletableFuture<PushConnectionsHandle> handlePushQuery(
      final ServiceContext serviceContext,
      final PushPhysicalPlan pushPhysicalPlan,
      final ConfiguredStatement<Query> statement,
      final PushRoutingOptions pushRoutingOptions,
      final LogicalSchema outputSchema,
      final TransientQueryQueue transientQueryQueue
  ) {
    final List<KsqlNode> hosts = pushPhysicalPlan.getScalablePushRegistry()
        .getLocator()
        .locate()
        .stream()
        .filter(node -> !pushRoutingOptions.getIsSkipForwardRequest() || node.isLocal())
        .distinct()
        .collect(Collectors.toList());

    if (hosts.isEmpty()) {
      LOG.debug("Unable to execute push query: {}. No nodes executing persistent queries",
          statement.getStatementText());
      throw new KsqlException(String.format(
          "Unable to execute push query. No nodes executing persistent queries %s",
          statement.getStatementText()));
    }

    // At the moment, this isn't async, but we return a future anyway.
    final CompletableFuture<PushConnectionsHandle> completableFuture = new CompletableFuture<>();
    try {
      PushConnectionsHandle pushConnectionsHandle =
          connectToHosts(serviceContext, pushPhysicalPlan, statement, hosts, outputSchema,
              transientQueryQueue);
      completableFuture.complete(pushConnectionsHandle);
    } catch (Throwable t) {
      completableFuture.completeExceptionally(t);
    }

    return completableFuture;
  }

  private PushConnectionsHandle connectToHosts(
      final ServiceContext serviceContext,
      final PushPhysicalPlan pushPhysicalPlan,
      final ConfiguredStatement<Query> statement,
      final List<KsqlNode> hosts,
      final LogicalSchema outputSchema,
      final TransientQueryQueue transientQueryQueue
  ) throws InterruptedException {
    final List<Callable<RoutingResult>> callables = new ArrayList<>();
    final CompletableFuture<Void> errorCallback = new CompletableFuture<>();
    for (final KsqlNode node : hosts) {
      callables.add(() -> executeOrRouteQuery(
          node, statement, serviceContext, pushPhysicalPlan, outputSchema,
          transientQueryQueue, errorCallback::completeExceptionally));
    }
    List<Future<RoutingResult>> futureList = executorService.invokeAll(callables,
        TIMEOUT_CONNECT_MS, TimeUnit.MILLISECONDS);

    int i = 0;
    final PushConnectionsHandle pushConnectionsHandle = new PushConnectionsHandle(errorCallback);
    KsqlException exception = null;
    for (final KsqlNode node : hosts) {
      final Future<RoutingResult> future = futureList.get(i++);
      RoutingResult routingResult = null;
      try {
        routingResult = future.get();
        pushConnectionsHandle.add(node, routingResult);
      } catch (ExecutionException | CancellationException e) {
        LOG.warn("Error routing query {} to host {} at timestamp {} with exception {}",
            statement.getStatementText(), node, System.currentTimeMillis(), e.getCause());
        exception = new KsqlException(String.format(
            "Unable to execute push query \"%s\". %s",
            statement.getStatementText(), e.getCause().getMessage()));
      }
    }
    if (exception != null) {
      pushConnectionsHandle.close();
      pushConnectionsHandle.completeExceptionally(exception);
    }
    return pushConnectionsHandle;
  }

  @VisibleForTesting
  static RoutingResult executeOrRouteQuery(
      final KsqlNode node,
      final ConfiguredStatement<Query> statement,
      final ServiceContext serviceContext,
      final PushPhysicalPlan pushPhysicalPlan,
      final LogicalSchema outputSchema,
      final TransientQueryQueue transientQueryQueue,
      final Consumer<Throwable> errorCallback
  ) {
    if (node.isLocal()) {
      BufferedPublisher<List<?>> publisher = null;
      try {
        LOG.debug("Query {} executed locally at host {} at timestamp {}.",
            statement.getStatementText(), node.location(), System.currentTimeMillis());
        publisher = pushPhysicalPlan.execute();
        publisher.subscribe(new LocalQueryStreamSubscriber(publisher.getContext(),
            transientQueryQueue, errorCallback));
        return new RoutingResult(RoutingResultStatus.SUCCESS, pushPhysicalPlan::close);
      } catch (Exception e) {
        if (publisher != null) {
          publisher.close();
        }
        pushPhysicalPlan.close();
        LOG.error("Error executing query {} locally at node {}",
            statement.getStatementText(), node.location(), e.getCause());
        throw new KsqlException(
            String.format("Error executing query locally at node %s: %s", node.location(),
                e.getMessage()),
            e
        );
      }
    } else {
      BufferedPublisher<StreamedRow> publisher = null;
      try {
        LOG.debug("Query {} routed to host {} at timestamp {}.",
            statement.getStatementText(), node.location(), System.currentTimeMillis());
        publisher = forwardTo(node, statement, serviceContext, outputSchema);
        publisher.subscribe(new QueryStreamSubscriber(publisher.getContext(), transientQueryQueue,
            errorCallback));
        return new RoutingResult(RoutingResultStatus.SUCCESS, publisher::close);
      } catch (Exception e) {
        if (publisher != null) {
          publisher.close();
        }
        LOG.error("Error forwarding query {} to node {}",
            statement.getStatementText(), node, e.getCause());
        throw new KsqlException(
            String.format("Error forwarding query to node %s: %s", node.location(),
                e.getMessage()),
            e
        );
      }
    }
  }

  private static BufferedPublisher<StreamedRow> forwardTo(
      final KsqlNode owner,
      final ConfiguredStatement<Query> statement,
      final ServiceContext serviceContext,
      final LogicalSchema outputSchema
  ) {
    // Add skip forward flag to properties
    final Map<String, Object> requestProperties = ImmutableMap.of(
        KsqlRequestConfig.KSQL_REQUEST_QUERY_PULL_SKIP_FORWARDING, true,
        KsqlRequestConfig.KSQL_REQUEST_INTERNAL_REQUEST, true);

    final RestResponse<BufferedPublisher<StreamedRow>> response  = serviceContext
          .getKsqlClient()
          .makeQueryRequestStreamed(
              owner.location(),
              statement.getStatementText(),
              statement.getSessionConfig().getOverrides(),
              requestProperties
          );

    if (response.isErroneous()) {
      throw new KsqlException(String.format(
          "Forwarding pull query request [%s, %s] failed with error %s ",
          statement.getSessionConfig().getOverrides(), requestProperties,
          response.getErrorMessage()));
    }

    return response.getResponse();
  }

  public enum RoutingResultStatus {
    SUCCESS
  }

  public static class RoutingResult {
    private final RoutingResultStatus status;
    private final AutoCloseable closeable;

    public RoutingResult(final RoutingResultStatus status, final AutoCloseable closeable) {
      this.status = status;
      this.closeable = closeable;
    }

    public void close() {
      try {
        closeable.close();
      } catch (Exception e) {
        LOG.error("Error closing routing result: " + e.getMessage(), e);
      }
    }

    public RoutingResultStatus getStatus() {
      return status;
    }
  }

  private static class QueryStreamSubscriber extends BaseSubscriber<StreamedRow> {

    private final TransientQueryQueue transientQueryQueue;
    private final Consumer<Throwable> errorCallback;
    private boolean closed;

    QueryStreamSubscriber(final Context context,
        final TransientQueryQueue transientQueryQueue,
        final Consumer<Throwable> errorCallback) {
      super(context);
      this.transientQueryQueue = transientQueryQueue;
      this.errorCallback = errorCallback;
    }

    @Override
    protected void afterSubscribe(final Subscription subscription) {
      makeRequest(1);
    }

    @Override
    protected synchronized void handleValue(final StreamedRow row) {
      if (closed) {
        return;
      }
      if (row.isTerminal()) {
        close();
        return;
      }
      if (row.getRow().isPresent()) {
          if (!transientQueryQueue.acceptRowNonBlocking(null,
              GenericRow.fromList(row.getRow().get().getColumns()))) {
            errorCallback.accept(new KsqlException("Hit limit of request queue"));
            close();
            return;
          };
      }
      makeRequest(1);
    }

    @Override
    protected void handleComplete() {
    }

    @Override
    protected void handleError(final Throwable t) {
      errorCallback.accept(t);
    }

    synchronized void close() {
      closed = true;
      context.runOnContext(v -> cancel());
    }
  }

  private static class LocalQueryStreamSubscriber extends BaseSubscriber<List<?>> {

    private final TransientQueryQueue transientQueryQueue;
    private final Consumer<Throwable> errorCallback;
    private boolean closed;

    LocalQueryStreamSubscriber(
        final Context context,
        final TransientQueryQueue transientQueryQueue,
        final Consumer<Throwable> errorCallback
    ) {
      super(context);
      this.transientQueryQueue = transientQueryQueue;
      this.errorCallback = errorCallback;
    }

    @Override
    protected void afterSubscribe(final Subscription subscription) {
      makeRequest(1);
    }

    @Override
    protected synchronized void handleValue(final List<?> row) {
      if (closed) {
        return;
      }
      if (!transientQueryQueue.acceptRowNonBlocking(null, GenericRow.fromList(row))) {
        errorCallback.accept(new KsqlException("Hit limit of request queue"));
        close();
        return;
      }

      makeRequest(1);
    }

    @Override
    protected void handleComplete() {
    }

    @Override
    protected void handleError(final Throwable t) {
      errorCallback.accept(t);
    }

    synchronized void close() {
      closed = true;
      context.runOnContext(v -> cancel());
    }
  }

  public static class PushConnectionsHandle {
    private final Map<KsqlNode, RoutingResult> results = new LinkedHashMap<>();
    private final CompletableFuture<Void> errorCallback;

    public PushConnectionsHandle(final CompletableFuture<Void> errorCallback) {
      this.errorCallback = errorCallback;
    }

    public void add(final KsqlNode ksqlNode, RoutingResult result) {
      results.put(ksqlNode, result);
    }

    public void remove(final KsqlNode ksqlNode) {
      results.remove(ksqlNode);
    }

    public void close() {
      for (RoutingResult result : results.values()) {
        result.close();
      }
    }

    public void onException(final Consumer<Throwable> consumer) {
      errorCallback.exceptionally(t -> {
        consumer.accept(t);
        return null;
      });
    }

    public void completeExceptionally(final Throwable throwable) {
      if (!errorCallback.isDone()) {
        errorCallback.completeExceptionally(throwable);
      }
    }

    public Throwable getError() throws InterruptedException {
      try {
        errorCallback.get();
      } catch (ExecutionException e) {
        return e.getCause();
      }
      return null;
    }
  }
}
