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
import io.confluent.ksql.physical.scalablepush.locator.PushLocator.KsqlNode;
import io.confluent.ksql.query.QueryId;
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

  private static final Logger LOG = LoggerFactory.getLogger(PushRouting.class);

  private static final long CLUSTER_CHECK_INTERVAL_MS = 1000;
  private static final long HOST_CACHE_EXPIRATION_MS = 1000;

  private final Function<ScalablePushRegistry, Set<KsqlNode>> registryToNodes;
  private final long clusterCheckInterval;
  private final boolean backgroundRetries;

  public PushRouting() {
    this(createLoadingCache(), CLUSTER_CHECK_INTERVAL_MS, true);
  }

  @VisibleForTesting
  public PushRouting(
      final Function<ScalablePushRegistry, Set<KsqlNode>> registryToNodes,
      final long clusterCheckInterval,
      final boolean backgroundRetries
  ) {
    this.registryToNodes = registryToNodes;
    this.clusterCheckInterval = clusterCheckInterval;
    this.backgroundRetries = backgroundRetries;
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
    final Set<KsqlNode> hosts = getInitialHosts(pushPhysicalPlan, statement, pushRoutingOptions);

    final PushConnectionsHandle pushConnectionsHandle = new PushConnectionsHandle();
    // Returns a future with the handle once the initial connection is made
    final CompletableFuture<PushConnectionsHandle> result = connectToHosts(
        serviceContext, pushPhysicalPlan, statement, hosts, outputSchema,
        transientQueryQueue, pushConnectionsHandle, false);
    // Only check for new nodes if this is the source node
    if (backgroundRetries && !pushRoutingOptions.getHasBeenForwarded()) {
      checkForNewHostsOnContext(serviceContext, pushPhysicalPlan, statement, hosts, outputSchema,
          transientQueryQueue, pushConnectionsHandle);
    }
    return result;
  }

  public void preparePushQuery(
      final PushPhysicalPlan pushPhysicalPlan,
      final ConfiguredStatement<Query> statement,
      final PushRoutingOptions pushRoutingOptions
  ) {
    // Ensure that we have the expected hosts and the below doesn't throw an exception.
    getInitialHosts(pushPhysicalPlan, statement, pushRoutingOptions);
  }

  private Set<KsqlNode> getInitialHosts(
      final PushPhysicalPlan pushPhysicalPlan,
      final ConfiguredStatement<Query> statement,
      final PushRoutingOptions pushRoutingOptions
  ) {
    final Set<KsqlNode> hosts = registryToNodes.apply(pushPhysicalPlan.getScalablePushRegistry())
        .stream()
        .filter(node -> !pushRoutingOptions.getHasBeenForwarded() || node.isLocal())
        .collect(Collectors.toSet());

    if (hosts.isEmpty()) {
      LOG.error("Unable to execute push query: {}. No nodes executing persistent queries",
          statement.getStatementText());
      throw new KsqlException(String.format(
          "Unable to execute push query. No nodes executing persistent queries %s",
          statement.getStatementText()));
    }
    return hosts;
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
          node, new RoutingResult(RoutingResultStatus.IN_PROGRESS, () -> { }));
      final CompletableFuture<Void> callback = new CompletableFuture<>();
      callback.handle((v, t) -> {
        if (t == null) {
          pushConnectionsHandle.get(node)
              .ifPresent(result -> {
                result.close();
                result.updateStatus(RoutingResultStatus.COMPLETE);
              });
          LOG.info("Host {} completed request {}.", node, pushPhysicalPlan.getQueryId());
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
          LOG.warn("Error routing query {} id {} to host {} at timestamp {} with exception {}",
              statement.getStatementText(), pushPhysicalPlan.getQueryId(), node,
              System.currentTimeMillis(), t.getCause());

          // We only fail the whole thing if this is not a new dynamically added node. We allow
          // retries in that case and don't fail the original request.
          pushConnectionsHandle.get(node)
              .ifPresent(result -> result.updateStatus(RoutingResultStatus.FAILED));
          if (!dynamicallyAddedNode) {
            pushConnectionsHandle.completeExceptionally(
                new KsqlException(String.format(
                    "Unable to execute push query \"%s\". %s",
                    statement.getStatementText(), t.getCause().getMessage())));
          }
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
    if (node.isLocal()) {
      LOG.debug("Query {} id {} executed locally at host {} at timestamp {}.",
          statement.getStatementText(), pushPhysicalPlan.getQueryId(), node.location(),
          System.currentTimeMillis());
      final AtomicReference<BufferedPublisher<List<?>>> publisherRef
          = new AtomicReference<>(null);
      return CompletableFuture.completedFuture(null)
          .thenApply(v -> pushPhysicalPlan.execute())
          .thenApply(publisher -> {
            publisherRef.set(publisher);
            publisher.subscribe(new LocalQueryStreamSubscriber(publisher.getContext(),
                transientQueryQueue, callback, node, pushPhysicalPlan.getQueryId()));
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
      final AtomicReference<BufferedPublisher<StreamedRow>> publisherRef
          = new AtomicReference<>(null);
      final CompletableFuture<BufferedPublisher<StreamedRow>> publisherFuture
          = forwardTo(node, statement, serviceContext, outputSchema, dynamicallyAddedNode);
      return publisherFuture.thenApply(publisher -> {
        publisherRef.set(publisher);
        publisher.subscribe(new RemoteStreamSubscriber(publisher.getContext(), transientQueryQueue,
            callback, node, pushPhysicalPlan.getQueryId()));
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
    final Set<KsqlNode> updatedHosts = registryToNodes.apply(
        pushPhysicalPlan.getScalablePushRegistry());
    final Set<KsqlNode> hosts = pushConnectionsHandle.getActiveHosts();
    final Set<KsqlNode> newHosts = Sets.difference(updatedHosts, hosts).stream()
        .filter(node -> !node.isLocal())
        .filter(node ->
            pushConnectionsHandle.get(node)
              .map(routingResult -> routingResult.getStatus() != RoutingResultStatus.IN_PROGRESS)
              .orElse(true))
        .collect(Collectors.toSet());
    final Set<KsqlNode> removedHosts = Sets.difference(hosts, updatedHosts);
    if (newHosts.size() > 0) {
      LOG.info("Dynamically adding new hosts {} for {}", newHosts, pushPhysicalPlan.getQueryId());
      connectToHosts(serviceContext, pushPhysicalPlan, statement, newHosts, outputSchema,
          transientQueryQueue, pushConnectionsHandle, true);
    }
    if (removedHosts.size() > 0) {
      LOG.info("Dynamically removing hosts {} for {}", removedHosts, pushPhysicalPlan.getQueryId());
      for (final KsqlNode node : removedHosts) {
        final RoutingResult result = pushConnectionsHandle.remove(node);
        result.close();
        result.updateStatus(RoutingResultStatus.REMOVED);
      }
    }
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
    final LoadingCache<ScalablePushRegistry, Set<KsqlNode>> cache = CacheBuilder.newBuilder()
        .maximumSize(40)
        .expireAfterWrite(HOST_CACHE_EXPIRATION_MS, TimeUnit.MILLISECONDS)
        .build(new CacheLoader<ScalablePushRegistry, Set<KsqlNode>>() {
          @Override
          public Set<KsqlNode> load(final ScalablePushRegistry scalablePushRegistry) {
            return loadCurrentHosts(scalablePushRegistry);
          }
        });
    return cache::getUnchecked;
  }

  /**
   * The status for a connection
   */
  public enum RoutingResultStatus {
    // The host connection is being set up but is not connected yet.
    IN_PROGRESS,
    // The host connection was successful and in use
    SUCCESS,
    // The host connection was closed and isn't active, but hasn't yet been removed from the map
    // since they're part of the known set of hosts.
    COMPLETE,
    // The connection has been removed since the host is no longer running the persistent query.
    REMOVED,
    // The request to the other host failed on the last try
    FAILED;

    static boolean isHostActive(final RoutingResultStatus status) {
      switch (status) {
        case IN_PROGRESS:
        case SUCCESS:
          return true;
        default:
          return false;
      }
    }
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
    private final KsqlNode node;
    private final QueryId queryId;
    private boolean closed;

    RemoteStreamSubscriber(final Context context,
        final TransientQueryQueue transientQueryQueue,
        final CompletableFuture<Void> callback,
        final KsqlNode node,
        final QueryId queryId) {
      super(context);
      this.transientQueryQueue = transientQueryQueue;
      this.callback = callback;
      this.node = node;
      this.queryId = queryId;
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
      if (row.getFinalMessage().isPresent()) {
        close();
        return;
      }
      if (row.getRow().isPresent()) {
        if (!transientQueryQueue.acceptRowNonBlocking(null,
            GenericRow.fromList(row.getRow().get().getColumns()))) {
          callback.completeExceptionally(new KsqlException("Hit limit of request queue"));
          close();
          return;
        }
      }
      if (row.getErrorMessage().isPresent()) {
        final KsqlErrorMessage errorMessage = row.getErrorMessage().get();
        LOG.error("Received error from remote node {} and id {}: {}", node, queryId, errorMessage);
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
      LOG.error("Received error from remote node {} for id {}: {}", node, queryId, t.getMessage(),
          t);
      callback.completeExceptionally(t);
      close();
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
    private final KsqlNode localNode;
    private final QueryId queryId;
    private boolean closed;

    LocalQueryStreamSubscriber(
        final Context context,
        final TransientQueryQueue transientQueryQueue,
        final CompletableFuture<Void> callback,
        final KsqlNode localNode,
        final QueryId queryId
    ) {
      super(context);
      this.transientQueryQueue = transientQueryQueue;
      this.callback = callback;
      this.localNode = localNode;
      this.queryId = queryId;
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
      LOG.error("Received error from local node {} for id {}: {}", localNode, queryId,
          t.getMessage(), t);
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
    private volatile boolean closed = false;

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
      closed = true;
      for (RoutingResult result : results.values()) {
        result.close();
      }
    }

    public Set<KsqlNode> getAllHosts() {
      return ImmutableSet.copyOf(results.keySet());
    }

    public Set<KsqlNode> getActiveHosts() {
      return results.entrySet().stream()
          .filter(e -> RoutingResultStatus.isHostActive(e.getValue().getStatus()))
          .map(Entry::getKey)
          .collect(ImmutableSet.toImmutableSet());
    }

    public boolean isClosed() {
      return closed || errorCallback.isDone();
    }

    public void onException(final Consumer<Throwable> consumer) {
      errorCallback.exceptionally(t -> {
        consumer.accept(t);
        return null;
      });
    }

    public void completeExceptionally(final Throwable throwable) {
      if (!errorCallback.isDone()) {
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
