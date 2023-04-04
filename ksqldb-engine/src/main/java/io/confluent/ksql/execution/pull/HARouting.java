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

package io.confluent.ksql.execution.pull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.confluent.ksql.execution.pull.PullPhysicalPlan.PullPhysicalPlanType;
import io.confluent.ksql.execution.streams.RoutingFilter.Host;
import io.confluent.ksql.execution.streams.RoutingFilter.RoutingFilterFactory;
import io.confluent.ksql.execution.streams.RoutingOptions;
import io.confluent.ksql.execution.streams.materialization.Locator.KsqlNode;
import io.confluent.ksql.execution.streams.materialization.Locator.KsqlPartitionLocation;
import io.confluent.ksql.execution.streams.materialization.MaterializationException;
import io.confluent.ksql.internal.PullQueryExecutorMetrics;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.query.PullQueryWriteStream;
import io.confluent.ksql.rest.client.RestResponse;
import io.confluent.ksql.rest.entity.KsqlHostInfoEntity;
import io.confluent.ksql.rest.entity.StreamedRow;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.KsqlRequestConfig;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
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
  private final KsqlConfig ksqlConfig;
  private final int coordinatorPoolSize;
  private final int routerPoolSize;

  public HARouting(
      final RoutingFilterFactory routingFilterFactory,
      final Optional<PullQueryExecutorMetrics> pullQueryMetrics,
      final KsqlConfig ksqlConfig
  ) {
    this.routingFilterFactory =
        Objects.requireNonNull(routingFilterFactory, "routingFilterFactory");
    this.ksqlConfig = Objects.requireNonNull(ksqlConfig, "ksqlConfig");
    this.coordinatorPoolSize = ksqlConfig.getInt(
        KsqlConfig.KSQL_QUERY_PULL_THREAD_POOL_SIZE_CONFIG);
    this.routerPoolSize = ksqlConfig.getInt(
        KsqlConfig.KSQL_QUERY_PULL_ROUTER_THREAD_POOL_SIZE_CONFIG);
    this.coordinatorExecutorService = Executors.newFixedThreadPool(
        coordinatorPoolSize,
        new ThreadFactoryBuilder().setNameFormat("pull-query-coordinator-%d").build());
    this.routerExecutorService = Executors.newFixedThreadPool(
        routerPoolSize,
        new ThreadFactoryBuilder().setNameFormat("pull-query-router-%d").build());
    this.pullQueryMetrics = Objects.requireNonNull(pullQueryMetrics, "pullQueryMetrics");
    this.pullQueryMetrics.ifPresent(pm -> pm.registerCoordinatorThreadPoolSupplier(
        () -> coordinatorPoolSize
            - ((ThreadPoolExecutor) coordinatorExecutorService).getActiveCount()));
    this.pullQueryMetrics.ifPresent(pm -> pm.registerRouterThreadPoolSupplier(
        () -> routerPoolSize - ((ThreadPoolExecutor) routerExecutorService).getActiveCount()));
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
      final CompletableFuture<Void> shouldCancelRequests
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

    final CompletableFuture<Void> completableFuture = new CompletableFuture<>();
    coordinatorExecutorService.submit(() -> {
      try {
        executeRounds(serviceContext, pullPhysicalPlan, statement, routingOptions,
            locations, pullQueryQueue, shouldCancelRequests);
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
      final CompletableFuture<Void> shouldCancelRequests
  ) {
    // The remaining partition locations to retrieve without error
    List<KsqlPartitionLocation> remainingLocations = ImmutableList.copyOf(locations);
    final Map<KsqlNode, List<Exception>> exceptionsPerNode = new HashMap<>();

    // For each round, each set of partition location objects is grouped by host, and all
    // keys associated with that host are batched together. For any requests that fail,
    // the partition location objects will be added to remainingLocations, and the next round
    // will attempt to fetch them from the next node in their prioritized list.
    // For example, locations might be:
    // [ Partition 0 <Host 1, Host 2>,
    //   Partition 1 <Host 2, Host 1>,
    //   Partition 2 <Host 1, Host 2> ]
    // In Round 0, fetch from Host 1: [Partition 0, Partition 2], from Host 2: [Partition 1]
    // If everything succeeds, we're done.  If Host 1 failed, then we'd have a Round 1:
    // In Round 1, fetch from Host 2: [Partition 0, Partition 2].
    try {
      for (int round = 0; ; round++) {
        // Group all partition location objects by their nth round node
        final Map<KsqlNode, List<KsqlPartitionLocation>> groupedByHost
            = groupByHost(statement, remainingLocations, round);

        // Make requests to each host, specifying the partitions we're interested in from
        // this host.
        final Map<KsqlNode, Future<NodeFetchResult>> futures = new LinkedHashMap<>();
        for (Map.Entry<KsqlNode, List<KsqlPartitionLocation>> entry : groupedByHost.entrySet()) {
          final KsqlNode node = entry.getKey();
          futures.put(node, routerExecutorService.submit(
              () -> executeOrRouteQuery(
                  node, entry.getValue(), statement, serviceContext, routingOptions,
                  pullQueryMetrics, pullPhysicalPlan, pullQueryQueue,
                  shouldCancelRequests)
          ));
        }

        // Go through all of the results of the requests, either aggregating rows or adding
        // the locations to the nextRoundRemaining list.
        final ImmutableList.Builder<KsqlPartitionLocation> nextRoundRemaining
            = ImmutableList.builder();

        for (Map.Entry<KsqlNode, Future<NodeFetchResult>> entry : futures.entrySet()) {
          final Future<NodeFetchResult> future = entry.getValue();
          final KsqlNode node = entry.getKey();
          final NodeFetchResult routingResult  = future.get();
          if (routingResult.isError()) {
            nextRoundRemaining.addAll(groupedByHost.get(node));
            exceptionsPerNode.computeIfAbsent(
                    routingResult.node, v -> new ArrayList<>())
                .add(routingResult.exception.get());
          } else {
            Preconditions.checkState(routingResult.getResult() == RoutingResult.SUCCESS);
          }
        }
        remainingLocations = nextRoundRemaining.build();

        // If there are no partition locations remaining, then we're done.
        if (remainingLocations.size() == 0) {
          pullQueryQueue.close();
          return;
        }
      }
    } catch (final Exception e) {
      final MaterializationException exception =
          new MaterializationException(
              "Unable to execute pull query: " + e.getMessage());
      for (Entry<KsqlNode, List<Exception>> entry : exceptionsPerNode.entrySet()) {
        for (Exception excp : entry.getValue()) {
          exception.addSuppressed(excp);
        }
      }
      throw exception;
    } finally {
      pullQueryQueue.close();
    }
  }

  /**
   * Groups all of the partition locations by the round-th entry in their prioritized list of host
   * nodes.
   *
   * @param statement the statement from which this request came
   * @param locations the list of partition locations to parse
   * @param round which round this is
   * @return A map of node to list of partition locations
   */
  private static Map<KsqlNode, List<KsqlPartitionLocation>> groupByHost(
      final ConfiguredStatement<Query> statement,
      final List<KsqlPartitionLocation> locations,
      final int round) {
    final Map<KsqlNode, List<KsqlPartitionLocation>> groupedByHost = new LinkedHashMap<>();
    for (KsqlPartitionLocation location : locations) {
      // If one of the partitions required is out of nodes, then we cannot continue.
      if (round >= location.getNodes().size()) {
        throw new MaterializationException("Exhausted standby hosts to try.");
      }
      final KsqlNode nextHost = location.getNodes().get(round);
      groupedByHost.computeIfAbsent(nextHost, h -> new ArrayList<>()).add(location);
    }
    return groupedByHost;
  }

  @SuppressWarnings("ParameterNumber")
  @VisibleForTesting
  static NodeFetchResult executeOrRouteQuery(
      final KsqlNode node,
      final List<KsqlPartitionLocation> locations,
      final ConfiguredStatement<Query> statement,
      final ServiceContext serviceContext,
      final RoutingOptions routingOptions,
      final Optional<PullQueryExecutorMetrics> pullQueryMetrics,
      final PullPhysicalPlan pullPhysicalPlan,
      final PullQueryWriteStream pullQueryQueue,
      final CompletableFuture<Void> shouldCancelRequests
  ) {
    final Function<StreamedRow, StreamedRow> addHostInfo
        = sr -> sr.withSourceHost(routingOptions.getIsDebugRequest() ? toKsqlHostInfo(node) : null);
    if (node.isLocal()) {
      try {
        LOG.debug("Query {} partitions {} executed locally at host {} at timestamp {}.",
            pullPhysicalPlan.getQueryId(), locations,
            node.location(), System.currentTimeMillis());
        pullQueryMetrics
          .ifPresent(queryExecutorMetrics -> queryExecutorMetrics.recordLocalRequests(1));
        pullPhysicalPlan.execute(locations, pullQueryQueue, addHostInfo);
        return new NodeFetchResult(RoutingResult.SUCCESS, node, Optional.empty());

      } catch (StandbyFallbackException e) {
        LOG.warn("Error executing query locally at node {}. Falling back to standby state which "
            + "may return stale results. Cause {}", node, e.getMessage());
        return new NodeFetchResult(RoutingResult.STANDBY_FALLBACK, node, Optional.of(e));
      } catch (Exception e) {
        throw new KsqlException(
          String.format("Error executing query locally at node %s: %s", node.location(),
            e.getMessage()),
          e
        );
      }
    } else {
      try {
        if (routingOptions.getIsSkipForwardRequest()) {
          throw new MaterializationException(
              "Unable to execute pull query: the request has already been forwarded and failed. "
                  + "Cannot forward again.");
        }
        LOG.debug("Query {} partitions {} routed to host {} at timestamp {}.",
            pullPhysicalPlan.getQueryId(), locations,
            node.location(), System.currentTimeMillis());
        pullQueryMetrics
          .ifPresent(queryExecutorMetrics -> queryExecutorMetrics.recordRemoteRequests(1));
        forwardTo(node, locations, statement, serviceContext, pullQueryQueue,
            shouldCancelRequests, addHostInfo);
        return new NodeFetchResult(RoutingResult.SUCCESS, node, Optional.empty());
      } catch (StandbyFallbackException e) {
        LOG.warn("Error forwarding query to node {}. Falling back to standby state which may "
            + "return stale results", node.location(), e.getCause());
        return new NodeFetchResult(RoutingResult.STANDBY_FALLBACK, node, Optional.of(e));
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
      // If the exception was caused by closing the connection, we consider this intentional and
      // just return.
      if (shouldCancelRequests.isDone()) {
        LOG.warn("Connection canceled, so returning");
        return;
      }

      // If we threw some explicit exception, then let it bubble up. All of the row handling is
      // wrapped in a KsqlException, so any intentional exception or bug will be surfaced.
      final KsqlException ksqlException = causedByKsqlException(e);

      // If we get some kind of unknown error, we assume it's network or other error from the
      // KsqlClient and try standbys
      final String exceptionMessage = ksqlException == null ? e.getMessage() :
          ksqlException.getMessage();
      throw new StandbyFallbackException(String.format(
          "Forwarding pull query request failed with error %s ", exceptionMessage), e);
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

  private static class NodeFetchResult {

    private final RoutingResult routingResult;
    private final KsqlNode node;
    private final Optional<Exception> exception;

    NodeFetchResult(
        final RoutingResult routingResult,
        final KsqlNode node,
        final Optional<Exception> exception
    ) {
      this.routingResult = routingResult;
      this.node = node;
      this.exception = exception;
    }

    public boolean isError() {
      return routingResult == RoutingResult.STANDBY_FALLBACK;
    }

    public RoutingResult getResult() {
      return routingResult;
    }

    public KsqlNode getNode() {
      return node;
    }

    public Optional<Exception> getException() {
      return exception;
    }
  }
}
