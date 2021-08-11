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
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.physical.pull.HARouting;
import io.confluent.ksql.physical.scalablepush.locator.PushLocator.KsqlNode;
import io.confluent.ksql.query.TransientQueryQueue;
import io.confluent.ksql.reactive.BaseSubscriber;
import io.confluent.ksql.reactive.BufferedPublisher;
import io.confluent.ksql.rest.client.RestResponse;
import io.confluent.ksql.rest.entity.KsqlErrorMessage;
import io.confluent.ksql.rest.entity.StreamedRow;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.KsqlRequestConfig;
import io.confluent.ksql.util.VertxUtils;
import io.vertx.core.Context;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PushRouting implements AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(HARouting.class);

  private static final long CLUSTER_CHECK_INTERVAL = 2000;

  private final Function<ScalablePushRegistry, Set<KsqlNode>> registryToNodes;
  private final long clusterCheckInterval;

  public PushRouting() {
    this(createLoadingCache(), CLUSTER_CHECK_INTERVAL);
  }

  @VisibleForTesting
  public PushRouting(
      final Function<ScalablePushRegistry, Set<KsqlNode>> registryToNodes,
      final long clusterCheckInterval
      ) {
    this.registryToNodes = registryToNodes;
    this.clusterCheckInterval = clusterCheckInterval;
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
    final Set<KsqlNode> hosts = registryToNodes.apply(pushPhysicalPlan.getScalablePushRegistry())
        .stream()
        .filter(node -> !pushRoutingOptions.getIsSkipForwardRequest() || node.isLocal())
        .collect(Collectors.toSet());

    if (hosts.isEmpty()) {
      LOG.error("Unable to execute push query: {}. No nodes executing persistent queries",
          statement.getStatementText());
      throw new KsqlException(String.format(
          "Unable to execute push query. No nodes executing persistent queries %s",
          statement.getStatementText()));
    }

    final PushConnectionsHandle pushConnectionsHandle = new PushConnectionsHandle();
    // Returns a future with the handle once the initial connection is made
    CompletableFuture<PushConnectionsHandle> result = connectToHosts(
        serviceContext, pushPhysicalPlan, statement, hosts, outputSchema,
        transientQueryQueue, pushConnectionsHandle, false);
    // Only check for new nodes if this is the source node
    if (!pushRoutingOptions.getIsSkipForwardRequest()) {
      checkForNewHostsOnContext(serviceContext, pushPhysicalPlan, statement, hosts, outputSchema,
          transientQueryQueue, pushConnectionsHandle);
    }
    return result;
  }

  /**
   * Connects to all of the hosts provided.
   * @return A future for a PushConnectionsHandle, which can be used to terminate connections.
   */
  private CompletableFuture<PushConnectionsHandle> connectToHosts(
      final ServiceContext serviceContext,
      final PushPhysicalPlan pushPhysicalPlan,
      final ConfiguredStatement<Query> statement,
      final Collection<KsqlNode> hosts,
      final LogicalSchema outputSchema,
      final TransientQueryQueue transientQueryQueue,
      final PushConnectionsHandle pushConnectionsHandle,
      final boolean dynamicallyAddedNode
  ) {
    final Map<KsqlNode, CompletableFuture<RoutingResult>> futureMap = new LinkedHashMap<>();
    for (final KsqlNode node : hosts) {
      pushConnectionsHandle.add(
          node, new RoutingResult(RoutingResultStatus.IN_PROGRESS, () -> {}));
      CompletableFuture<Void> callback = new CompletableFuture<>();
      callback.handle((v, t) -> {
        if (t == null) {
          pushConnectionsHandle.get(node)
              .ifPresent(result -> {
                result.close();
                result.updateStatus(RoutingResultStatus.COMPLETE);
              });
          LOG.info("Host {} completed request.", node);
        } else {
          pushConnectionsHandle.completeExceptionally(t);
        }
        return null;
      });
      futureMap.put(node, executeOrRouteQuery(
          node, statement, serviceContext, pushPhysicalPlan, outputSchema,
          transientQueryQueue, callback, dynamicallyAddedNode));
    }
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
              statement.getStatementText(), node, System.currentTimeMillis(), t.getCause());

          pushConnectionsHandle.completeExceptionally(
              new KsqlException(String.format(
                  "Unable to execute push query \"%s\". %s",
                  statement.getStatementText(), t.getCause().getMessage())));

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
      final CompletableFuture<Void> callback,
      final boolean dynamicallyAddedNode
  ) {
    System.out.println("executeOrRouteQuery " + node);
    if (node.isLocal()) {
      LOG.debug("Query {} executed locally at host {} at timestamp {}.",
          statement.getStatementText(), node.location(), System.currentTimeMillis());
      System.out.println("Local execute " + node);
      final AtomicReference<BufferedPublisher<List<?>>> publisherRef
          = new AtomicReference<>(null);
      return CompletableFuture.completedFuture(null)
          .thenApply(v -> pushPhysicalPlan.execute())
          .thenApply(publisher -> {
            publisherRef.set(publisher);
            System.out.println("About to subscribe");
            publisher.subscribe(new LocalQueryStreamSubscriber(publisher.getContext(),
                transientQueryQueue, callback));
            return new RoutingResult(RoutingResultStatus.SUCCESS, () -> {
              pushPhysicalPlan.close();
              publisher.close();
            });
          })
          .exceptionally(t -> {
            LOG.error("Error executing query {} locally at node {}",
                statement.getStatementText(), node.location(), t.getCause());
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
          statement.getStatementText(), node.location(), System.currentTimeMillis());
      System.out.println("Starting request to node " + node);
      final AtomicReference<BufferedPublisher<StreamedRow>> publisherRef
          = new AtomicReference<>(null);
      final CompletableFuture<BufferedPublisher<StreamedRow>> publisherFuture
          = forwardTo(node, statement, serviceContext, outputSchema, dynamicallyAddedNode);
      return publisherFuture.thenApply(publisher -> {
        System.out.println("GOT publisher " + publisher);
        publisherRef.set(publisher);
        System.out.println("Creating new RemoteStreamSubscriber" + publisher);
        publisher.subscribe(new RemoteStreamSubscriber(publisher.getContext(), transientQueryQueue,
            callback));
        return new RoutingResult(RoutingResultStatus.SUCCESS, publisher::close);
      }).exceptionally(t -> {
        LOG.error("Error forwarding query {} to node {}",
            statement.getStatementText(), node, t.getCause());
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
      final LogicalSchema outputSchema,
      final boolean dynamicallyAddedNode
  ) {
    // Add skip forward flag to properties
    final Map<String, Object> requestProperties = ImmutableMap.of(
        KsqlRequestConfig.KSQL_REQUEST_QUERY_PUSH_SKIP_FORWARDING, true,
        KsqlRequestConfig.KSQL_REQUEST_INTERNAL_REQUEST, true,
        KsqlRequestConfig.KSQL_REQUEST_QUERY_PUSH_REGISTRY_START, dynamicallyAddedNode);

    final CompletableFuture<RestResponse<BufferedPublisher<StreamedRow>>> future  = serviceContext
        .getKsqlClient()
        .makeQueryRequestStreamed(
            owner.location(),
            statement.getStatementText(),
            statement.getSessionConfig().getOverrides(),
            requestProperties
        );

    System.out.println("Did Streamed future");

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

  private void checkForNewHostsOnContext(
      final ServiceContext serviceContext,
      final PushPhysicalPlan pushPhysicalPlan,
      final ConfiguredStatement<Query> statement,
      final Set<KsqlNode> hosts,
      final LogicalSchema outputSchema,
      final TransientQueryQueue transientQueryQueue,
      final PushConnectionsHandle pushConnectionsHandle
  ) {
    pushPhysicalPlan.getContext().runOnContext(v ->
        checkForNewHosts(serviceContext, pushPhysicalPlan, statement, outputSchema,
            transientQueryQueue, pushConnectionsHandle));
  }

  private void checkForNewHosts(
      final ServiceContext serviceContext,
      final PushPhysicalPlan pushPhysicalPlan,
      final ConfiguredStatement<Query> statement,
      final LogicalSchema outputSchema,
      final TransientQueryQueue transientQueryQueue,
      final PushConnectionsHandle pushConnectionsHandle
  ) {
    VertxUtils.checkContext(pushPhysicalPlan.getContext());
    if (pushConnectionsHandle.isClosed()) {
      return;
    }
    Set<KsqlNode> updatedHosts = registryToNodes.apply(pushPhysicalPlan.getScalablePushRegistry());
    Set<KsqlNode> hosts = pushConnectionsHandle.getAllHosts();
    Set<KsqlNode> newHosts = Sets.difference(updatedHosts, hosts).stream()
        .filter(node -> !node.isLocal())
        .collect(Collectors.toSet());
    Set<KsqlNode> removedHosts = Sets.difference(hosts, updatedHosts);
    if (newHosts.size() > 0) {
      System.out.println("NEW HOSTS " + newHosts);
      connectToHosts(serviceContext, pushPhysicalPlan, statement, newHosts, outputSchema,
          transientQueryQueue, pushConnectionsHandle, true);
    }
    if (removedHosts.size() > 0) {
      System.out.println("REMOVING HOSTS " + removedHosts);
      for (final KsqlNode node : removedHosts) {
        RoutingResult result = pushConnectionsHandle.remove(node);
        result.close();
        result.updateStatus(RoutingResultStatus.REMOVED);
      }
    }
    System.out.println("UPDATED HOSTS " + updatedHosts);
    pushPhysicalPlan.getContext().owner().setTimer(clusterCheckInterval, timerId ->
        checkForNewHosts(serviceContext, pushPhysicalPlan, statement, outputSchema,
            transientQueryQueue, pushConnectionsHandle));
  }

  private static Set<KsqlNode> loadCurrentHosts(final ScalablePushRegistry scalablePushRegistry) {
    return new HashSet<>(scalablePushRegistry
        .getLocator()
        .locate());
  }

  private static Function<ScalablePushRegistry, Set<KsqlNode>> createLoadingCache() {
    LoadingCache<ScalablePushRegistry, Set<KsqlNode>> cache = CacheBuilder.newBuilder()
        .maximumSize(40)
        .expireAfterWrite(2000, TimeUnit.MILLISECONDS)
        .build(new CacheLoader<ScalablePushRegistry, Set<KsqlNode>>() {
          @Override
          public Set<KsqlNode> load(ScalablePushRegistry scalablePushRegistry) {
            return loadCurrentHosts(scalablePushRegistry);
          }
        });
    return cache::getUnchecked;
  }

  public enum RoutingResultStatus {
    IN_PROGRESS,
    SUCCESS,
    COMPLETE,
    REMOVED
  }

  public static class RoutingResult {
    private final AutoCloseable closeable;
    private volatile RoutingResultStatus status;

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

    public void updateStatus(final RoutingResultStatus status) {
      this.status = status;
    }
  }

  /**
   * Subscriber for handling remote data.
   */
  private static class RemoteStreamSubscriber extends BaseSubscriber<StreamedRow> {

    private final TransientQueryQueue transientQueryQueue;
    private final CompletableFuture<Void> callback;
    private boolean closed;

    RemoteStreamSubscriber(final Context context,
        final TransientQueryQueue transientQueryQueue,
        final CompletableFuture<Void> callback) {
      super(context);
      this.transientQueryQueue = transientQueryQueue;
      this.callback = callback;
    }

    @Override
    protected void afterSubscribe(final Subscription subscription) {
      makeRequest(1);
    }

    @Override
    protected synchronized void handleValue(final StreamedRow row) {
      System.out.println("Got a row");
      if (closed) {
        System.out.println("Got a row but closed");
        return;
      }
      if (row.getFinalMessage().isPresent()) {
        System.out.println("Got a row: TERMINAL");
        close();
        return;
      }
      if (row.getRow().isPresent()) {
        System.out.println("Got a row: ROW");
        if (!transientQueryQueue.acceptRowNonBlocking(null,
            GenericRow.fromList(row.getRow().get().getColumns()))) {
          callback.completeExceptionally(new KsqlException("Hit limit of request queue"));
          close();
          return;
        }
      }
      if (row.getErrorMessage().isPresent()) {
        System.out.println("Got a row: ERROR MESSAGE");
        final KsqlErrorMessage errorMessage = row.getErrorMessage().get();
        callback.completeExceptionally(new KsqlException("Remote server had an error: "
            + errorMessage.getErrorCode() + " - " + errorMessage.getMessage()));
        close();
        return;
      }
      makeRequest(1);
    }

    @Override
    protected void handleComplete() {
      close();
    }

    @Override
    protected void handleError(final Throwable t) {
      System.out.println("Handle error");
      callback.completeExceptionally(t);
    }

    synchronized void close() {
      closed = true;
      // Is a noop if already completed in some manner
      callback.complete(null);
      context.runOnContext(v -> cancel());
    }
  }

  /**
   * Subscriber for handling local data.
   */
  private static class LocalQueryStreamSubscriber extends BaseSubscriber<List<?>> {

    private final TransientQueryQueue transientQueryQueue;
    private final CompletableFuture<Void> callback;
    private boolean closed;

    LocalQueryStreamSubscriber(
        final Context context,
        final TransientQueryQueue transientQueryQueue,
        final CompletableFuture<Void> callback
    ) {
      super(context);
      this.transientQueryQueue = transientQueryQueue;
      this.callback = callback;
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
      System.out.println("Writing local row " + row);
      if (!transientQueryQueue.acceptRowNonBlocking(null, GenericRow.fromList(row))) {
        callback.completeExceptionally(new KsqlException("Hit limit of request queue"));
        close();
        return;
      }

      makeRequest(1);
    }

    @Override
    protected void handleComplete() {
      close();
    }

    @Override
    protected void handleError(final Throwable t) {
      System.out.println("GOT ERROR IN LOCAL SUB");
      callback.completeExceptionally(t);
    }

    synchronized void close() {
      closed = true;
      // Is a noop if already completed in some manner
      callback.complete(null);
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
    public PushConnectionsHandle() {
      this.errorCallback = new CompletableFuture<>();

      // If anything calls the error callback. all results are closed.
      errorCallback.exceptionally(t -> {
        close();
        return null;
      });
    }

    public void add(final KsqlNode ksqlNode, final RoutingResult result) {
      results.put(ksqlNode, result);
    }

    public RoutingResult remove(final KsqlNode ksqlNode) {
      return results.remove(ksqlNode);
    }

    public Optional<RoutingResult> get(final KsqlNode ksqlNode) {
      return Optional.ofNullable(results.getOrDefault(ksqlNode, null));
    }

    public void close() {
      for (RoutingResult result : results.values()) {
        result.close();
      }
    }

    public Set<KsqlNode> getAllHosts() {
      return ImmutableSet.copyOf(results.keySet());
    }

    public boolean isClosed() {
      return errorCallback.isDone();
    }

    public void onException(final Consumer<Throwable> consumer) {
      errorCallback.exceptionally(t -> {
        consumer.accept(t);
        return null;
      });
    }

    public void completeExceptionally(final Throwable throwable) {
      System.out.println("PushConnectionsHandle Completed with error");
      if (!errorCallback.isDone()) {
        System.out.println("PushConnectionsHandle Calling callback");
        close();
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
