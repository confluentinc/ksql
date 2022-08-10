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
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
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
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.KsqlRequestConfig;
import io.vertx.core.Context;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PushRouting implements AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(HARouting.class);

  public PushRouting() {
  }

  @Override
  public void close() {
  }

  /**
   * Does a scalable push query returning a handle which can be used to stop the connections.
   * @return A future for a PushConnectionsHandle, which can be used to terminate connections.
   */
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
      LOG.error("Unable to execute push query: {}. No nodes executing persistent queries",
          statement.getMaskedStatementText());
      throw new KsqlException(String.format(
          "Unable to execute push query. No nodes executing persistent queries %s",
          statement.getMaskedStatementText()));
    }

    return connectToHosts(serviceContext, pushPhysicalPlan, statement, hosts, outputSchema,
        transientQueryQueue);
  }

  /**
   * Connects to all of the hosts provided.
   * @return A future for a PushConnectionsHandle, which can be used to terminate connections.
   */
  private CompletableFuture<PushConnectionsHandle> connectToHosts(
      final ServiceContext serviceContext,
      final PushPhysicalPlan pushPhysicalPlan,
      final ConfiguredStatement<Query> statement,
      final List<KsqlNode> hosts,
      final LogicalSchema outputSchema,
      final TransientQueryQueue transientQueryQueue
  ) {
    final Map<KsqlNode, CompletableFuture<RoutingResult>> futureMap = new LinkedHashMap<>();
    final CompletableFuture<Void> errorCallback = new CompletableFuture<>();
    for (final KsqlNode node : hosts) {
      futureMap.put(node, executeOrRouteQuery(
          node, statement, serviceContext, pushPhysicalPlan, outputSchema,
          transientQueryQueue, errorCallback::completeExceptionally));
    }
    final PushConnectionsHandle pushConnectionsHandle = new PushConnectionsHandle(errorCallback);
    return CompletableFuture.allOf(futureMap.values().toArray(new CompletableFuture[0]))
        .thenApply(v -> {
          for (final KsqlNode node : hosts) {
            final CompletableFuture<RoutingResult> future = futureMap.get(node);
            final RoutingResult routingResult = future.join();
            pushConnectionsHandle.add(node, routingResult);
          }
          return pushConnectionsHandle;
        })
        .exceptionally(t -> {
          final KsqlNode node = futureMap.entrySet().stream()
              .filter(e -> e.getValue().isCompletedExceptionally())
              .map(Entry::getKey)
              .findFirst()
              .orElse(null);
          LOG.warn("Error routing query {} to host {} at timestamp {} with exception {}",
              statement.getMaskedStatementText(), node, System.currentTimeMillis(), t.getCause());

          pushConnectionsHandle.close();
          pushConnectionsHandle.completeExceptionally(
              new KsqlException(String.format(
                  "Unable to execute push query \"%s\". %s",
                  statement.getMaskedStatementText(), t.getCause().getMessage())));

          return pushConnectionsHandle;
        });
  }

  @VisibleForTesting
  static CompletableFuture<RoutingResult> executeOrRouteQuery(
      final KsqlNode node,
      final ConfiguredStatement<Query> statement,
      final ServiceContext serviceContext,
      final PushPhysicalPlan pushPhysicalPlan,
      final LogicalSchema outputSchema,
      final TransientQueryQueue transientQueryQueue,
      final Consumer<Throwable> errorCallback
  ) {
    if (node.isLocal()) {
      LOG.debug("Query {} executed locally at host {} at timestamp {}.",
          statement.getMaskedStatementText(), node.location(), System.currentTimeMillis());
      final AtomicReference<BufferedPublisher<List<?>>> publisherRef
          = new AtomicReference<>(null);
      return CompletableFuture.completedFuture(null)
          .thenApply(v -> pushPhysicalPlan.execute())
          .thenApply(publisher -> {
            publisherRef.set(publisher);
            publisher.subscribe(new LocalQueryStreamSubscriber(publisher.getContext(),
                transientQueryQueue, errorCallback));
            return new RoutingResult(RoutingResultStatus.SUCCESS, () -> {
              pushPhysicalPlan.close();
              publisher.close();
            });
          })
          .exceptionally(t -> {
            LOG.error("Error executing query {} locally at node {}",
                statement.getMaskedStatementText(), node.location(), t.getCause());
            final BufferedPublisher<List<?>> publisher = publisherRef.get();
            pushPhysicalPlan.close();
            if (publisher != null) {
              publisher.close();
            }
            throw new KsqlException(
                String.format("Error executing query locally at node %s: %s", node.location(),
                    t.getMessage()),
                t
            );
          });
    } else {
      LOG.debug("Query {} routed to host {} at timestamp {}.",
          statement.getMaskedStatementText(), node.location(), System.currentTimeMillis());
      final AtomicReference<BufferedPublisher<StreamedRow>> publisherRef
          = new AtomicReference<>(null);
      final CompletableFuture<BufferedPublisher<StreamedRow>> publisherFuture
          = forwardTo(node, statement, serviceContext, outputSchema);
      return publisherFuture.thenApply(publisher -> {
        publisherRef.set(publisher);
        publisher.subscribe(new RemoteStreamSubscriber(publisher.getContext(), transientQueryQueue,
            errorCallback));
        return new RoutingResult(RoutingResultStatus.SUCCESS, publisher::close);
      }).exceptionally(t -> {
        LOG.error("Error forwarding query {} to node {}",
            statement.getMaskedStatementText(), node, t.getCause());
        final BufferedPublisher<StreamedRow> publisher = publisherRef.get();
        if (publisher != null) {
          publisher.close();
        }
        throw new KsqlException(
            String.format("Error forwarding query to node %s: %s", node.location(),
                t.getMessage()),
            t
        );
      });
    }
  }

  private static CompletableFuture<BufferedPublisher<StreamedRow>> forwardTo(
      final KsqlNode owner,
      final ConfiguredStatement<Query> statement,
      final ServiceContext serviceContext,
      final LogicalSchema outputSchema
  ) {
    // Add skip forward flag to properties
    final Map<String, Object> requestProperties = ImmutableMap.of(
        KsqlRequestConfig.KSQL_REQUEST_QUERY_PUSH_SKIP_FORWARDING, true,
        KsqlRequestConfig.KSQL_REQUEST_INTERNAL_REQUEST, true);

    final CompletableFuture<RestResponse<BufferedPublisher<StreamedRow>>> future  = serviceContext
        .getKsqlClient()
        .makeQueryRequestStreamed(
            owner.location(),
            statement.getUnMaskedStatementText(),
            statement.getSessionConfig().getOverrides(),
            requestProperties
        );

    return future.thenApply(response -> {
      if (response.isErroneous()) {
        throw new KsqlException(String.format(
            "Forwarding pull query request [%s, %s] failed with error %s ",
            statement.getSessionConfig().getOverrides(), requestProperties,
            response.getErrorMessage()));
      }

      return response.getResponse();
    });
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

  /**
   * Subscriber for handling remote data.
   */
  private static class RemoteStreamSubscriber extends BaseSubscriber<StreamedRow> {

    private final TransientQueryQueue transientQueryQueue;
    private final Consumer<Throwable> errorCallback;
    private boolean closed;

    RemoteStreamSubscriber(final Context context,
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
        }
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

  /**
   * Subscriber for handling local data.
   */
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

  /**
   * An object containing references to all of the {@link RoutingResult} objects and their publisher
   * connections.
   */
  public static class PushConnectionsHandle {
    private final Map<KsqlNode, RoutingResult> results = new ConcurrentHashMap<>();
    private final CompletableFuture<Void> errorCallback;

    @SuppressFBWarnings(value = "EI_EXPOSE_REP")
    public PushConnectionsHandle(final CompletableFuture<Void> errorCallback) {
      this.errorCallback = errorCallback;

      // If anything calls the error callback. all results are closed.
      errorCallback.exceptionally(t -> {
        close();
        return null;
      });
    }

    public void add(final KsqlNode ksqlNode, final RoutingResult result) {
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
