/*
 * Copyright 2020 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"; you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.physical.pull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.confluent.ksql.execution.streams.RoutingFilter.Host;
import io.confluent.ksql.execution.streams.RoutingFilter.RoutingFilterFactory;
import io.confluent.ksql.execution.streams.RoutingOptions;
import io.confluent.ksql.execution.streams.materialization.Locator.KsqlNode;
import io.confluent.ksql.execution.streams.materialization.Locator.KsqlPartitionLocation;
import io.confluent.ksql.execution.streams.materialization.MaterializationException;
import io.confluent.ksql.internal.PullQueryExecutorMetrics;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.physical.pull.PullPhysicalPlan.PullPhysicalPlanType;
import io.confluent.ksql.query.PullQueryWriteStream;
import io.confluent.ksql.rest.client.RestResponse;
import io.confluent.ksql.rest.entity.KsqlHostInfoEntity;
import io.confluent.ksql.rest.entity.StreamedRow;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.util.ConsistencyOffsetVector;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.KsqlRequestConfig;
import io.confluent.ksql.util.OffsetVector;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings({"checkstyle:ClassDataAbstractionCoupling", "checkstyle:CyclomaticComplexity"})
public final class HARouting implements AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(HARouting.class);

  private final ExecutorService coordinatorExecutorService;
  private final ExecutorService routerExecutorService;
  private final RoutingFilterFactory routingFilterFactory;
  private final Optional<PullQueryExecutorMetrics> pullQueryMetrics;

  public HARouting(
      final RoutingFilterFactory routingFilterFactory,
      final Optional<PullQueryExecutorMetrics> pullQueryMetrics,
      final KsqlConfig ksqlConfig
  ) {
    this.routingFilterFactory =
        Objects.requireNonNull(routingFilterFactory, "routingFilterFactory");
    this.coordinatorExecutorService = Executors.newFixedThreadPool(
        ksqlConfig.getInt(KsqlConfig.KSQL_QUERY_PULL_THREAD_POOL_SIZE_CONFIG),
        new ThreadFactoryBuilder().setNameFormat("pull-query-coordinator-%d").build());
    this.routerExecutorService = Executors.newFixedThreadPool(
        ksqlConfig.getInt(KsqlConfig.KSQL_QUERY_PULL_ROUTER_THREAD_POOL_SIZE_CONFIG),
        new ThreadFactoryBuilder().setNameFormat("pull-query-router-%d").build());
    this.pullQueryMetrics = Objects.requireNonNull(pullQueryMetrics, "pullQueryMetrics");
  }

  @Override
  public void close() {
    coordinatorExecutorService.shutdown();
    routerExecutorService.shutdown();
  }

  public CompletableFuture<Void> handlePullQuery(
      final ServiceContext serviceContext,
      final PullPhysicalPlan pullPhysicalPlan,
      final ConfiguredStatement<Query> statement,
      final RoutingOptions routingOptions,
      final PullQueryWriteStream pullQueryQueue,
      final CompletableFuture<Void> shouldCancelRequests,
      final Optional<ConsistencyOffsetVector> consistencyOffsetVector
  ) {
    final List<KsqlPartitionLocation> allLocations = pullPhysicalPlan.getMaterialization().locator()
        .locate(
            pullPhysicalPlan.getKeys(),
            routingOptions,
            routingFilterFactory,
            pullPhysicalPlan.getPlanType() == PullPhysicalPlanType.RANGE_SCAN
    );

    final Map<Integer, List<Host>> emptyPartitions = allLocations.stream()
        .filter(loc -> loc.getNodes().stream().noneMatch(node -> node.getHost().isSelected()))
        .collect(Collectors.toMap(
            KsqlPartitionLocation::getPartition,
            loc -> loc.getNodes().stream().map(KsqlNode::getHost).collect(Collectors.toList())));

    if (!emptyPartitions.isEmpty()) {
      final MaterializationException materializationException = new MaterializationException(
          "Unable to execute pull query. "
              + emptyPartitions.entrySet()
              .stream()
              .map(kv -> String.format(
                  "Partition %s failed to find valid host. Hosts scanned: %s",
                  kv.getKey(), kv.getValue()))
              .collect(Collectors.joining(", ", "[", "]")));

      LOG.debug(materializationException.getMessage());
      throw materializationException;
    }

    // at this point we should filter out the hosts that we should not route to
    final List<KsqlPartitionLocation> locations = allLocations
        .stream()
        .map(KsqlPartitionLocation::removeFilteredHosts)
        .collect(Collectors.toList());

    // Required for integration test, to be removed in follow-up PR
    consistencyOffsetVector.ifPresent(this::updateConsistencyOffsetVectorDummy);

    final CompletableFuture<Void> completableFuture = new CompletableFuture<>();
    coordinatorExecutorService.submit(() -> {
      try {
        executeRounds(serviceContext, pullPhysicalPlan, statement, routingOptions,
            locations, pullQueryQueue, shouldCancelRequests, consistencyOffsetVector);
        completableFuture.complete(null);
      } catch (Throwable t) {
        completableFuture.completeExceptionally(t);
      }
    });

    return completableFuture;
  }

  private void executeRounds(
      final ServiceContext serviceContext,
      final PullPhysicalPlan pullPhysicalPlan,
      final ConfiguredStatement<Query> statement,
      final RoutingOptions routingOptions,
      final List<KsqlPartitionLocation> locations,
      final PullQueryWriteStream pullQueryQueue,
      final CompletableFuture<Void> shouldCancelRequests,
      final Optional<ConsistencyOffsetVector> consistencyOffsetVector
  ) throws InterruptedException {
    final ExecutorCompletionService<PartitionFetchResult> completionService =
        new ExecutorCompletionService<>(routerExecutorService);
    final int totalPartitions = locations.size();
    int processedPartitions = 0;

    for (final KsqlPartitionLocation partition : locations) {
      final KsqlNode node = getNodeForRound(partition);
      pullQueryMetrics.ifPresent(queryExecutorMetrics ->
          queryExecutorMetrics.recordPartitionFetchRequest(1));
      completionService.submit(
          () -> executeOrRouteQuery(
          node, partition, statement, serviceContext, routingOptions,
          pullQueryMetrics, pullPhysicalPlan, pullQueryQueue,
          shouldCancelRequests, consistencyOffsetVector)
      );
    }

    while (processedPartitions < totalPartitions) {
      final Future<PartitionFetchResult> future = completionService.take();
      try {
        final PartitionFetchResult fetchResult = future.get();
        if (fetchResult.isError()) {
          final KsqlPartitionLocation nextRoundPartition = nextNode(fetchResult.getLocation());
          final KsqlNode node = getNodeForRound(nextRoundPartition);
          pullQueryMetrics.ifPresent(queryExecutorMetrics ->
              queryExecutorMetrics.recordResubmissionRequest(1));
          completionService.submit(
              () -> executeOrRouteQuery(
              node, nextRoundPartition, statement, serviceContext, routingOptions,
              pullQueryMetrics, pullPhysicalPlan, pullQueryQueue,
              shouldCancelRequests, consistencyOffsetVector)
          );
        } else {
          Preconditions.checkState(fetchResult.getResult() == RoutingResult.SUCCESS);
          processedPartitions++;
          if (consistencyOffsetVector.isPresent() && fetchResult.offsetVector.isPresent()) {
            consistencyOffsetVector.get().merge(fetchResult.getOffsetVector().get());
          }
        }
      } catch (ExecutionException e) {
        throw new MaterializationException("Unable to execute pull query", e);
      }

    }

    pullQueryQueue.close();
  }

  private KsqlPartitionLocation nextNode(final KsqlPartitionLocation partition) {
    return partition.removeHeadHost();
  }

  private static KsqlNode getNodeForRound(
      final KsqlPartitionLocation location) {
    if (location.getNodes().isEmpty()) {
      throw new MaterializationException("Exhausted standby hosts to try.");
    }
    return location.getNodes().get(0);
  }

  @SuppressWarnings("ParameterNumber")
  @VisibleForTesting
  static PartitionFetchResult executeOrRouteQuery(
      final KsqlNode node,
      final KsqlPartitionLocation location,
      final ConfiguredStatement<Query> statement,
      final ServiceContext serviceContext,
      final RoutingOptions routingOptions,
      final Optional<PullQueryExecutorMetrics> pullQueryMetrics,
      final PullPhysicalPlan pullPhysicalPlan,
      final PullQueryWriteStream pullQueryQueue,
      final CompletableFuture<Void> shouldCancelRequests,
      final Optional<ConsistencyOffsetVector> consistencyOffsetVector
  ) {
    final Function<StreamedRow, StreamedRow> addHostInfo
        = sr -> sr.withSourceHost(routingOptions.getIsDebugRequest() ? toKsqlHostInfo(node) : null);
    if (node.isLocal()) {
      try {
        LOG.debug("Query {} partition {} executed locally at host {} at timestamp {}.",
            pullPhysicalPlan.getQueryId(), location.getPartition(),
            node.location(), System.currentTimeMillis());
        pullQueryMetrics
          .ifPresent(queryExecutorMetrics -> queryExecutorMetrics.recordLocalRequests(1));
        synchronized (pullPhysicalPlan) {
          pullPhysicalPlan.execute(ImmutableList.of(location), pullQueryQueue, addHostInfo);
          return new PartitionFetchResult(RoutingResult.SUCCESS, location, Optional.empty());
        }
      } catch (StandbyFallbackException e) {
        LOG.warn("Error executing query locally at node {}. Falling back to standby state which "
            + "may return stale results", node, e.getCause());
        return new PartitionFetchResult(RoutingResult.STANDBY_FALLBACK, location, Optional.empty());
      } catch (Exception e) {
        throw new KsqlException(
          String.format("Error executing query locally at node %s: %s", node.location(),
            e.getMessage()),
          e
        );
      }
    } else {
      try {
        LOG.debug("Query {} partition {} routed to host {} at timestamp {}.",
            pullPhysicalPlan.getQueryId(), location.getPartition(),
            node.location(), System.currentTimeMillis());
        pullQueryMetrics
          .ifPresent(queryExecutorMetrics -> queryExecutorMetrics.recordRemoteRequests(1));
        forwardTo(node, ImmutableList.of(location), statement, serviceContext, pullQueryQueue,
            shouldCancelRequests, consistencyOffsetVector, addHostInfo);
        return new PartitionFetchResult(RoutingResult.SUCCESS, location, Optional.empty());
      } catch (StandbyFallbackException e) {
        LOG.warn("Error forwarding query to node {}. Falling back to standby state which may "
            + "return stale results", node.location(), e.getCause());
        return new PartitionFetchResult(RoutingResult.STANDBY_FALLBACK, location, Optional.empty());
      } catch (Exception e) {
        throw new KsqlException(
          String.format("Error forwarding query to node %s: %s", node.location(), e.getMessage()),
          e
        );
      }
    }
  }

  /**
   * Converts the KsqlNode to KsqlHostInfoEntity
   */
  private static KsqlHostInfoEntity toKsqlHostInfo(final KsqlNode node) {
    return new KsqlHostInfoEntity(node.location().getHost(), node.location().getPort());
  }

  private static void forwardTo(
      final KsqlNode owner,
      final List<KsqlPartitionLocation> locations,
      final ConfiguredStatement<Query> statement,
      final ServiceContext serviceContext,
      final PullQueryWriteStream pullQueryQueue,
      final CompletableFuture<Void> shouldCancelRequests,
      final Optional<ConsistencyOffsetVector> consistencyOffsetVector,
      final Function<StreamedRow, StreamedRow> addHostInfo) {

    // Specify the partitions we specifically want to read.  This will prevent reading unintended
    // standby data when we are reading active for example.
    final String partitions = locations.stream()
        .map(location -> Integer.toString(location.getPartition()))
        .collect(Collectors.joining(","));
    // Add skip forward flag to properties
    final ImmutableMap.Builder<String, Object> builder = new ImmutableMap.Builder<String, Object>()
        .put(KsqlRequestConfig.KSQL_REQUEST_QUERY_PULL_SKIP_FORWARDING, true)
        .put(KsqlRequestConfig.KSQL_REQUEST_INTERNAL_REQUEST, true)
        .put(KsqlRequestConfig.KSQL_REQUEST_QUERY_PULL_PARTITIONS, partitions);
    if (consistencyOffsetVector.isPresent()) {
      builder.put(KsqlRequestConfig.KSQL_REQUEST_QUERY_PULL_CONSISTENCY_OFFSET_VECTOR,
          consistencyOffsetVector.get().serialize());
    }
    final Map<String, Object> requestProperties = builder.build();
    final RestResponse<Integer> response;

    try {
      response = serviceContext
          .getKsqlClient()
          .makeQueryRequest(
              owner.location(),
              statement.getUnMaskedStatementText(),
              statement.getSessionConfig().getOverrides(),
              requestProperties,
              pullQueryQueue,
              shouldCancelRequests,
              addHostInfo
          );
    } catch (Exception e) {
      // If we threw some explicit exception, then let it bubble up. All of the row handling is
      // wrapped in a KsqlException, so any intentional exception or bug will be surfaced.
      final KsqlException ksqlException = causedByKsqlException(e);
      if (ksqlException != null) {
        throw ksqlException;
      }
      // If the exception was caused by closing the connection, we consider this intentional and
      // just return.
      if (shouldCancelRequests.isDone()) {
        LOG.warn("Connection canceled, so returning");
        return;
      }
      // If we get some kind of unknown error, we assume it's network or other error from the
      // KsqlClient and try standbys
      throw new StandbyFallbackException(String.format(
          "Forwarding pull query request [%s, %s] failed with error %s ",
          statement.getSessionConfig().getOverrides(), requestProperties,
          e.getMessage()), e);
    }

    if (response.isErroneous()) {
      throw new KsqlException(String.format(
          "Forwarding pull query request [%s, %s] failed with error %s ",
          statement.getSessionConfig().getOverrides(), requestProperties,
          response.getErrorMessage()));
    }

    final int numRows = response.getResponse();
    if (numRows == 0) {
      throw new KsqlException(String.format(
          "Forwarding pull query request [%s, %s] failed due to invalid "
              + "empty response from forwarding call, expected a header row.",
          statement.getSessionConfig().getOverrides(), requestProperties));
    }
  }

  private static KsqlException causedByKsqlException(final Exception e) {
    Throwable throwable = e;
    while (throwable != null) {
      if (throwable instanceof KsqlException) {
        return (KsqlException) throwable;
      }
      throwable = throwable.getCause();
    }
    return null;
  }

  private enum RoutingResult {
    SUCCESS,
    STANDBY_FALLBACK
  }

  private static class PartitionFetchResult {

    private final RoutingResult routingResult;
    private final KsqlPartitionLocation location;
    private final Optional<OffsetVector> offsetVector;

    PartitionFetchResult(final RoutingResult routingResult, final KsqlPartitionLocation location,
                         final Optional<OffsetVector> offsetVector
    ) {
      this.routingResult = routingResult;
      this.location = location;
      this.offsetVector = offsetVector;
    }

    public boolean isError() {
      return routingResult == RoutingResult.STANDBY_FALLBACK;
    }

    public RoutingResult getResult() {
      return routingResult;
    }

    public KsqlPartitionLocation getLocation() {
      return location;
    }

    public Optional<OffsetVector> getOffsetVector() {
      return offsetVector;
    }
  }

  private void updateConsistencyOffsetVectorDummy(final ConsistencyOffsetVector ct) {
    ct.setVersion(2);
    ct.addTopicOffsets("dummy", ImmutableMap.of(5, 5L, 6, 6L, 7, 7L));
  }

}
