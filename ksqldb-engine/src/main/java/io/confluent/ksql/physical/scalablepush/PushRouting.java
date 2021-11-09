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
import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.internal.ScalablePushQueryMetrics;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.physical.common.QueryRow;
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
import io.confluent.ksql.util.KeyValue;
import io.confluent.ksql.util.KeyValueMetadata;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.KsqlRequestConfig;
import io.confluent.ksql.util.OffsetVector;
import io.confluent.ksql.util.PushOffsetRange;
import io.confluent.ksql.util.PushOffsetVector;
import io.confluent.ksql.util.RowMetadata;
import io.confluent.ksql.util.VertxUtils;
import io.vertx.core.Context;
import java.util.Collection;
import java.util.Collections;
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
import java.util.function.BiConsumer;
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
      final TransientQueryQueue transientQueryQueue,
      final Optional<ScalablePushQueryMetrics> scalablePushQueryMetrics,
      final PushPhysicalPlanCreator pushPhysicalPlanCreator,
      final Optional<PushOffsetRange> offsetRange
  ) {
    final Set<KsqlNode> hosts = getInitialHosts(pushPhysicalPlan, statement, pushRoutingOptions);

    final PushConnectionsHandle pushConnectionsHandle = new PushConnectionsHandle();
    pushConnectionsHandle.onCompletionOrException((v, t) -> {
      pushPhysicalPlan.getScalablePushRegistry()
          .cleanupCatchupConsumer(pushPhysicalPlan.getCatchupConsumerGroupId());
    });
    final AtomicReference<PushPhysicalPlan> pushPhysicalPlanReference
        = new AtomicReference<>(pushPhysicalPlan);
    Set<KsqlNode> catchupHosts = Collections.emptySet();
    if (offsetRange.isPresent()) {
      pushConnectionsHandle.getOffsetsTracker().updateFromToken(offsetRange.get().getEndOffsets());
      if (!pushRoutingOptions.getHasBeenForwarded()) {
        catchupHosts = hosts;
      }
    }
    // Returns a future with the handle once the initial connection is made
    final CompletableFuture<PushConnectionsHandle> result = connectToHosts(
        serviceContext, pushPhysicalPlanReference, statement, hosts, outputSchema,
        transientQueryQueue, pushConnectionsHandle, false, scalablePushQueryMetrics,
        catchupHosts, pushPhysicalPlanCreator, pushRoutingOptions);
    // Only check for new nodes if this is the source node
    if (backgroundRetries && !pushRoutingOptions.getHasBeenForwarded()) {
      checkForNewHostsOnContext(serviceContext, pushPhysicalPlanReference, statement, hosts,
          outputSchema, transientQueryQueue, pushConnectionsHandle, scalablePushQueryMetrics,
          pushPhysicalPlanCreator, pushRoutingOptions);
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
      final AtomicReference<PushPhysicalPlan> pushPhysicalPlan,
      final ConfiguredStatement<Query> statement,
      final Collection<KsqlNode> hosts,
      final LogicalSchema outputSchema,
      final TransientQueryQueue transientQueryQueue,
      final PushConnectionsHandle pushConnectionsHandle,
      final boolean dynamicallyAddedNode,
      final Optional<ScalablePushQueryMetrics> scalablePushQueryMetrics,
      final Set<KsqlNode> catchupHosts,
      final PushPhysicalPlanCreator pushPhysicalPlanCreator,
      final PushRoutingOptions pushRoutingOptions
  ) {
    System.out.println("connectToHosts " + hosts);
    final Map<KsqlNode, CompletableFuture<RoutingResult>> futureMap = new LinkedHashMap<>();
    for (final KsqlNode node : hosts) {
      pushConnectionsHandle.add(
          node, new RoutingResult(RoutingResultStatus.IN_PROGRESS, () -> { }));
      final CompletableFuture<Void> callback = new CompletableFuture<>();
      callback.handle((v, t) -> {
        if (t == null) {
          pushConnectionsHandle.get(node)
              .ifPresent(result -> {
                System.out.println("CLOSING");
                result.close();
                System.out.println("SETTING TO COMPLETE " + node + " result " + result);
                result.updateStatus(RoutingResultStatus.COMPLETE);
                System.out.println("AFTER SETTING TO COMPLETE " + node + " result " + result);
              });
          LOG.info("Host {} completed request {}.", node, pushPhysicalPlan.get().getQueryId());
        } else if (t instanceof GapFoundException) {
          pushConnectionsHandle.get(node)
              .ifPresent(result -> {
                result.close();
                result.updateStatus(RoutingResultStatus.OFFSET_GAP_FOUND);
              });
        } else {
          pushConnectionsHandle.completeExceptionally(t);
        }
        return null;
      });
      futureMap.put(node, executeOrRouteQuery(
          node, statement, serviceContext, pushPhysicalPlan, outputSchema,
          transientQueryQueue, callback, scalablePushQueryMetrics,
          pushConnectionsHandle.getOffsetsTracker(), catchupHosts.contains(node),
          pushPhysicalPlanCreator, pushRoutingOptions));
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
          for (KsqlNode n : hosts) {
            final CompletableFuture<RoutingResult> future = futureMap.get(n);
            // Take whatever completed exceptionally and mark it as failed
            if (future.isCompletedExceptionally()) {
              pushConnectionsHandle.get(n)
                  .ifPresent(result -> result.updateStatus(RoutingResultStatus.FAILED));
            } else {
              final RoutingResult routingResult = future.join();
              pushConnectionsHandle.add(node, routingResult);
            }
          }
          LOG.warn("Error routing query {} id {} to host {} at timestamp {} with exception {}",
              statement.getStatementText(), pushPhysicalPlan.get().getQueryId(), node,
              System.currentTimeMillis(), t.getCause());

          // We only fail the whole thing if this is not a new dynamically added node. We allow
          // retries in that case and don't fail the original request.
          if (!dynamicallyAddedNode) {
            pushConnectionsHandle.completeExceptionally(
                new KsqlException(String.format(
                    "Unable to execute push query \"%s\". %s",
                    statement.getStatementText(), t.getCause().getMessage())));
          }
          return pushConnectionsHandle;
        })
        .exceptionally(t -> {
          LOG.error("Unexpected error handing exception", t);
          return pushConnectionsHandle;
        });
  }

  @VisibleForTesting
  static CompletableFuture<RoutingResult> executeOrRouteQuery(
      final KsqlNode node,
      final ConfiguredStatement<Query> statement,
      final ServiceContext serviceContext,
      final AtomicReference<PushPhysicalPlan> pushPhysicalPlan,
      final LogicalSchema outputSchema,
      final TransientQueryQueue transientQueryQueue,
      final CompletableFuture<Void> callback,
      final Optional<ScalablePushQueryMetrics> scalablePushQueryMetrics,
      final OffsetsTracker offsetsTracker,
      final boolean shouldCatchupFromOffsets,
      final PushPhysicalPlanCreator pushPhysicalPlanCreator,
      final PushRoutingOptions pushRoutingOptions
  ) {
    System.out.println("executeOrRouteQuery node.isLocal():" + node.isLocal());
    if (node.isLocal()) {
      LOG.info("Query {} id {} executed locally at host {} at timestamp {}.",
          statement.getStatementText(), pushPhysicalPlan.get().getQueryId(), node.location(),
          System.currentTimeMillis());
      scalablePushQueryMetrics
          .ifPresent(metrics -> metrics.recordLocalRequests(1));

      final AtomicReference<PushPhysicalPlan> thisPushPhysicalPlan
          = new AtomicReference<>(pushPhysicalPlan.get());
      final AtomicReference<BufferedPublisher<QueryRow>> publisherRef
          = new AtomicReference<>(null);
      return CompletableFuture.completedFuture(null)
          .thenApply(v -> {
            if (thisPushPhysicalPlan.get().isClosed()) {
              thisPushPhysicalPlan.set(
                  pushPhysicalPlanCreator.create(Optional.of(offsetsTracker.getOffsetRange()), Optional.empty()));
              pushPhysicalPlan.set(thisPushPhysicalPlan.get());
            }
            return thisPushPhysicalPlan.get().execute();
          })
          .thenApply(publisher -> {
            publisherRef.set(publisher);
            publisher.subscribe(new LocalQueryStreamSubscriber(publisher.getContext(),
                transientQueryQueue, callback, node, pushPhysicalPlan.get().getQueryId(),
                offsetsTracker, pushRoutingOptions));
            System.out.println("Successful local node");
            return new RoutingResult(RoutingResultStatus.SUCCESS, () -> {
              thisPushPhysicalPlan.get().close();
              publisher.close();
            });
          })
          .exceptionally(t -> {
            LOG.error("Error executing query {} locally at node {}",
                statement.getStatementText(), node.location(), t.getCause());
            final BufferedPublisher<QueryRow> publisher = publisherRef.get();
            thisPushPhysicalPlan.get().close();
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
      LOG.info("Query {} routed to host {} at timestamp {}.",
          statement.getStatementText(), node.location(), System.currentTimeMillis());
      scalablePushQueryMetrics
              .ifPresent(metrics -> metrics.recordRemoteRequests(1));
      final AtomicReference<BufferedPublisher<StreamedRow>> publisherRef
          = new AtomicReference<>(null);
      final CompletableFuture<BufferedPublisher<StreamedRow>> publisherFuture
          = forwardTo(node, statement, serviceContext, outputSchema,
          shouldCatchupFromOffsets, offsetsTracker,
          pushPhysicalPlan.get().getCatchupConsumerGroupId());
      return publisherFuture.thenApply(publisher -> {
        publisherRef.set(publisher);
        publisher.subscribe(new RemoteStreamSubscriber(publisher.getContext(), transientQueryQueue,
            callback, node, pushPhysicalPlan.get().getQueryId(), offsetsTracker,
            pushRoutingOptions));
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
      final boolean shouldCatchupFromOffsets,
      final OffsetsTracker offsetsTracker,
      final String catchupConsumerGroup
  ) {
    // Add skip forward flag to properties
    final ImmutableMap.Builder<String, Object> requestPropertiesBuilder
        = ImmutableMap.<String, Object>builder()
        .put(KsqlRequestConfig.KSQL_REQUEST_QUERY_PUSH_SKIP_FORWARDING, true)
        .put(KsqlRequestConfig.KSQL_REQUEST_INTERNAL_REQUEST, true);

    if (shouldCatchupFromOffsets) {
      System.out.println("Forwarding node needs to catchup starting at " + offsetsTracker.getOffsets());
      requestPropertiesBuilder.put(KsqlRequestConfig.KSQL_REQUEST_QUERY_PUSH_CONTINUATION_TOKEN,
          offsetsTracker.getSerializedOffsetRange());
      requestPropertiesBuilder.put(KsqlRequestConfig.KSQL_REQUEST_QUERY_PUSH_CATCHUP_CONSUMER_GROUP,
          catchupConsumerGroup);
    }

    final Map<String, Object> requestProperties = requestPropertiesBuilder.build();

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
      final AtomicReference<PushPhysicalPlan> pushPhysicalPlan,
      final ConfiguredStatement<Query> statement,
      final Set<KsqlNode> hosts,
      final LogicalSchema outputSchema,
      final TransientQueryQueue transientQueryQueue,
      final PushConnectionsHandle pushConnectionsHandle,
      final Optional<ScalablePushQueryMetrics> scalablePushQueryMetrics,
      final PushPhysicalPlanCreator pushPhysicalPlanCreator,
      final PushRoutingOptions pushRoutingOptions
  ) {
    pushPhysicalPlan.get().getContext().runOnContext(v ->
        checkForNewHosts(serviceContext, pushPhysicalPlan, statement, outputSchema,
            transientQueryQueue, pushConnectionsHandle, scalablePushQueryMetrics,
                pushPhysicalPlanCreator, pushRoutingOptions));
  }

  private void checkForNewHosts(
      final ServiceContext serviceContext,
      final AtomicReference<PushPhysicalPlan> pushPhysicalPlan,
      final ConfiguredStatement<Query> statement,
      final LogicalSchema outputSchema,
      final TransientQueryQueue transientQueryQueue,
      final PushConnectionsHandle pushConnectionsHandle,
      final Optional<ScalablePushQueryMetrics> scalablePushQueryMetrics,
      final PushPhysicalPlanCreator pushPhysicalPlanCreator,
      final PushRoutingOptions pushRoutingOptions
  ) {
    VertxUtils.checkContext(pushPhysicalPlan.get().getContext());
    if (pushConnectionsHandle.isClosed()) {
      return;
    }
    final Set<KsqlNode> updatedHosts = registryToNodes.apply(
        pushPhysicalPlan.get().getScalablePushRegistry());
    final Set<KsqlNode> hosts = pushConnectionsHandle.getActiveHosts();
    System.out.println("CHECKING updatedHosts " + updatedHosts + " active hosts " + hosts);
    final Set<KsqlNode> newHosts = Sets.difference(updatedHosts, hosts).stream()
        .filter(node ->
            pushConnectionsHandle.get(node)
              .map(routingResult -> routingResult.getStatus() != RoutingResultStatus.IN_PROGRESS)
              .orElse(true))
        .collect(Collectors.toSet());
    System.out.println("newHosts " + newHosts);
    final Set<KsqlNode> removedHosts = Sets.difference(hosts, updatedHosts);
    if (newHosts.size() > 0) {
      LOG.info("Dynamically adding new hosts {} for {}", newHosts,
          pushPhysicalPlan.get().getQueryId());
      System.out.println("Dynamically adding new hosts " + newHosts);
      final Set<KsqlNode> catchupHosts = newHosts.stream()
          .filter(node ->
              pushConnectionsHandle.get(node)
                  .map(routingResult ->
                      routingResult.getStatus() == RoutingResultStatus.OFFSET_GAP_FOUND)
                  .orElse(false))
          .collect(Collectors.toSet());
      System.out.println("Catchup hosts " + catchupHosts);
      connectToHosts(serviceContext, pushPhysicalPlan, statement, newHosts, outputSchema,
          transientQueryQueue, pushConnectionsHandle, true,
          scalablePushQueryMetrics, catchupHosts, pushPhysicalPlanCreator, pushRoutingOptions);
    }
    if (removedHosts.size() > 0) {
      LOG.info("Dynamically removing hosts {} for {}", removedHosts,
          pushPhysicalPlan.get().getQueryId());
      System.out.println("Dynamically removing hosts " + removedHosts);
      for (final KsqlNode node : removedHosts) {
        final RoutingResult result = pushConnectionsHandle.remove(node);
        result.close();
        result.updateStatus(RoutingResultStatus.REMOVED);
      }
    }
    pushPhysicalPlan.get().getContext().owner().setTimer(clusterCheckInterval, timerId ->
        checkForNewHosts(serviceContext, pushPhysicalPlan, statement, outputSchema,
            transientQueryQueue, pushConnectionsHandle, scalablePushQueryMetrics,
                pushPhysicalPlanCreator, pushRoutingOptions));
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
    FAILED,
    // The request was stopped due to finding a gap in offsets and will be restarted
    OFFSET_GAP_FOUND;

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

    @Override
    public String toString() {
      return "RoutingResult{" + status + "}";
    }
  }

  private static class StreamSubscriber<T> extends BaseSubscriber<T> {

    protected final TransientQueryQueue transientQueryQueue;
    protected final CompletableFuture<Void> callback;
    protected final KsqlNode node;
    protected final QueryId queryId;
    protected final OffsetsTracker offsetsTracker;
    protected final PushRoutingOptions pushRoutingOptions;
    protected boolean closed;

    public StreamSubscriber(final Context context,
        final TransientQueryQueue transientQueryQueue,
        final CompletableFuture<Void> callback,
        final KsqlNode node,
        final QueryId queryId,
        final OffsetsTracker offsetsTracker,
        final PushRoutingOptions pushRoutingOptions) {
      super(context);
      this.transientQueryQueue = transientQueryQueue;
      this.callback = callback;
      this.node = node;
      this.queryId = queryId;
      this.offsetsTracker = offsetsTracker;
      this.pushRoutingOptions = pushRoutingOptions;
    }

    @Override
    protected void afterSubscribe(final Subscription subscription) {
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

    protected Optional<PushOffsetRange> handleProgressToken(
        final Optional<PushOffsetRange> offsetRangeOptional,
        final boolean isSourceNode
    ) {
      if (!offsetRangeOptional.isPresent()) {
        return offsetRangeOptional;
      }
      final PushOffsetRange offsetRange = offsetRangeOptional.get();
      final PushOffsetVector currentOffsets = offsetsTracker.getOffsets();
      Preconditions.checkState(offsetRange.getStartOffsets().isPresent());
      final PushOffsetVector startOffsets = currentOffsets.mergeCopy(
          offsetRange.getStartOffsets().get());
      final PushOffsetVector endOffsets = currentOffsets.mergeCopy(offsetRange.getEndOffsets());
      final PushOffsetRange updatedToken
          = new PushOffsetRange(Optional.of(startOffsets), endOffsets);
      if (isSourceNode && !offsetRange.getStartOffsets().get().lessThanOrEqualTo(currentOffsets)) {
        LOG.warn(name() + "Found a gap in offsets for {} and id {}: start: {}, current: {}", node, queryId,
            offsetRange.getStartOffsets(), offsetsTracker.getOffsets());
        System.out.println(name() + " Found gap in offsets start: " +  offsetRange.getStartOffsets() + " current: " + offsetsTracker.getOffsets());
        callback.completeExceptionally(new GapFoundException());
        close();
        return Optional.empty();
      } else {
        System.out.println(name() + " UPDATE " + offsetRange.getEndOffsets());
        offsetsTracker.updateFromToken(offsetRange.getEndOffsets());
      }
      return Optional.of(updatedToken);
    }

    public String name() {
      return "common";
    }
  }

  /**
   * Subscriber for handling remote data.
   */
  private static class RemoteStreamSubscriber extends StreamSubscriber<StreamedRow> {

    RemoteStreamSubscriber(final Context context,
        final TransientQueryQueue transientQueryQueue,
        final CompletableFuture<Void> callback,
        final KsqlNode node,
        final QueryId queryId,
        final OffsetsTracker offsetsTracker,
        final PushRoutingOptions pushRoutingOptions
    ) {
      super(context, transientQueryQueue, callback, node, queryId, offsetsTracker,
          pushRoutingOptions);
    }

    @Override
    protected synchronized void handleValue(final StreamedRow row) {
      System.out.println("REMOTE Got row " + row + " token " + row.getContinuationToken().map(t -> PushOffsetRange.deserialize(t.getContinuationToken())));
      if (closed) {
        return;
      }
      if (row.getFinalMessage().isPresent()) {
        close();
        return;
      }

      if (row.getRow().isPresent() || row.getContinuationToken().isPresent()) {
        final boolean isSourceNode = !pushRoutingOptions.getHasBeenForwarded();
        final Optional<PushOffsetRange> currentOffsetRange = handleProgressToken(
            row.getContinuationToken().map(t -> PushOffsetRange.deserialize(t.getContinuationToken())), isSourceNode);
        System.out.println("REMOTE Got row " + row + " range " + currentOffsetRange);
        if (row.getContinuationToken().isPresent() && !currentOffsetRange.isPresent()) {
          return;
        }
        if (!currentOffsetRange.isPresent() || pushRoutingOptions.shouldOutputContinuationToken()) {
          final Optional<RowMetadata> rowMetadata = currentOffsetRange.map(
              or -> new RowMetadata(Optional.of(or)));
          final KeyValueMetadata<List<?>, GenericRow> keyValueMetadata = rowMetadata.isPresent()
              ? new KeyValueMetadata<>(rowMetadata.get())
              : new KeyValueMetadata<>(
                  new KeyValue<>(null, GenericRow.fromList(row.getRow().get().getColumns())));
          System.out.println("ALAN: REMOTE Outputting " + node + " value " + row.getRow().get().getColumns() + " row metadata " + rowMetadata + " for query " + queryId);
          if (!transientQueryQueue.acceptRowNonBlocking(keyValueMetadata)) {
            callback.completeExceptionally(new KsqlException("Hit limit of request queue"));
            close();
            return;
          }
        } else {
          LOG.info("Not outputting continuation token " + currentOffsetRange.get());
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

    public String name() {
      return "Remote";
    }
  }

  /**
   * Subscriber for handling local data.
   */
  private static class LocalQueryStreamSubscriber extends StreamSubscriber<QueryRow> {

    LocalQueryStreamSubscriber(
        final Context context,
        final TransientQueryQueue transientQueryQueue,
        final CompletableFuture<Void> callback,
        final KsqlNode localNode,
        final QueryId queryId,
        final OffsetsTracker offsetsTracker,
        final PushRoutingOptions pushRoutingOptions
    ) {
      super(context, transientQueryQueue, callback, localNode, queryId, offsetsTracker,
          pushRoutingOptions);
    }

    @Override
    protected synchronized void handleValue(final QueryRow row) {
      System.out.println("handleValue " + row.value() + " closed " + closed + " range " + row.getOffsetRange());
      if (closed) {
        return;
      }

      final boolean isSourceNode = !pushRoutingOptions.getHasBeenForwarded();
      final Optional<PushOffsetRange> currentOffsetRange
          = handleProgressToken(row.getOffsetRange(), isSourceNode);
      if (!currentOffsetRange.isPresent() && row.getOffsetRange().isPresent()) {
        return;
      }
      if (!currentOffsetRange.isPresent() || pushRoutingOptions.shouldOutputContinuationToken()) {
        final Optional<RowMetadata> rowMetadata = currentOffsetRange.map(
            t -> new RowMetadata(Optional.of(t)));
        final KeyValueMetadata<List<?>, GenericRow> keyValueMetadata = rowMetadata.isPresent()
            ? new KeyValueMetadata<>(rowMetadata.get())
            : new KeyValueMetadata<>(new KeyValue<>(null, row.value()));
        System.out.println("ALAN: Outputting " + node + " value " + row.value() + " row metadata " + rowMetadata + " for query " + queryId);
        if (!transientQueryQueue.acceptRowNonBlocking(keyValueMetadata)) {
          callback.completeExceptionally(new KsqlException("Hit limit of request queue"));
          close();
          return;
        }
      } else {
        LOG.info("Not outputting continuation token " + currentOffsetRange.get());
      }

      makeRequest(1);
    }

    public String name() {
      return "Local";
    }
  }

  /**
   * An object containing references to all of the {@link RoutingResult} objects and their publisher
   * connections.
   */
  public static class PushConnectionsHandle {
    private final Map<KsqlNode, RoutingResult> results = new ConcurrentHashMap<>();
    private final CompletableFuture<Void> callback;
    private volatile boolean closed = false;
    private final OffsetsTracker offsetsTracker = new OffsetsTracker();

    @SuppressFBWarnings(value = "EI_EXPOSE_REP")
    public PushConnectionsHandle() {
      this.callback = new CompletableFuture<>();

      // If anything calls the error callback. all results are closed.
      callback.exceptionally(t -> {
        close();
        return null;
      });
    }

    public void add(final KsqlNode ksqlNode, final RoutingResult result) {
      System.out.println("PushConnectionsHandle ADDING " + ksqlNode + " result " + result);
      results.put(ksqlNode, result);
      // Make sure that we close the result if the handle has been closed.
      if (isClosed()) {
        result.close();
      }
    }

    public RoutingResult remove(final KsqlNode ksqlNode) {
      return results.remove(ksqlNode);
    }

    public Optional<RoutingResult> get(final KsqlNode ksqlNode) {
      return Optional.ofNullable(results.getOrDefault(ksqlNode, null));
    }

    public void close() {
      closed = true;
      System.out.println("CLOSING HANDLE");
      for (Map.Entry<KsqlNode, RoutingResult> result : results.entrySet()) {
        System.out.println("CLOSING result" + result.getKey() + ": " + result.getValue().getStatus());
        result.getValue().close();
      }
      // Completes normally, unless already completed exceptionally.
      callback.complete(null);
    }

    public Set<KsqlNode> getAllHosts() {
      return ImmutableSet.copyOf(results.keySet());
    }

    public Set<KsqlNode> getActiveHosts() {
      System.out.println("ALL RESULTS " + results);
      return results.entrySet().stream()
          .filter(e -> RoutingResultStatus.isHostActive(e.getValue().getStatus()))
          .map(Entry::getKey)
          .collect(ImmutableSet.toImmutableSet());
    }

    public boolean isClosed() {
      return closed || callback.isDone();
    }

    public void onException(final Consumer<Throwable> consumer) {
      callback.exceptionally(t -> {
        consumer.accept(t);
        return null;
      });
    }

    public void completeExceptionally(final Throwable throwable) {
      if (!callback.isDone()) {
        callback.completeExceptionally(throwable);
        close();
      }
    }

    public Throwable getError() throws InterruptedException {
      try {
        callback.get();
      } catch (ExecutionException e) {
        return e.getCause();
      }
      return null;
    }

    public void onCompletionOrException(final BiConsumer<Void, Throwable> biConsumer) {
      callback.handle((v, t) -> {
        biConsumer.accept(v, t);
        return null;
      });
    }

    public OffsetsTracker getOffsetsTracker() {
      return offsetsTracker;
    }
  }

  public static class OffsetsTracker {
    private final PushOffsetVector currentOffsets = new PushOffsetVector();

    public OffsetsTracker() {
    }

    public PushOffsetVector getOffsets() {
      return currentOffsets;
    }

    public PushOffsetRange getOffsetRange() {
      return new PushOffsetRange(Optional.empty(), currentOffsets);
    }

    public String getSerializedOffsetRange() {
      return getOffsetRange().serialize();
    }

    public void updateFromToken(final OffsetVector update) {
      System.out.println("ALAN: updateFromToken from " + currentOffsets);
      currentOffsets.merge(update);
      System.out.println("ALAN: updateFromToken to " + currentOffsets);
    }
  }

  public static class GapFoundException extends RuntimeException {
  }
}
