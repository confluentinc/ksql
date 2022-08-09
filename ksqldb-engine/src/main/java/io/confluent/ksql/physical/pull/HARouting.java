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
import io.confluent.ksql.query.PullQueryQueue;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.rest.client.RestResponse;
import io.confluent.ksql.rest.entity.StreamedRow;
import io.confluent.ksql.rest.entity.StreamedRow.Header;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.KsqlRequestConfig;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class HARouting implements AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(HARouting.class);

  private final ExecutorService executorService;
  private final RoutingFilterFactory routingFilterFactory;
  private final Optional<PullQueryExecutorMetrics> pullQueryMetrics;
  private final RouteQuery routeQuery;

  public HARouting(
      final RoutingFilterFactory routingFilterFactory,
      final Optional<PullQueryExecutorMetrics> pullQueryMetrics,
      final KsqlConfig ksqlConfig
  ) {
    this(routingFilterFactory, pullQueryMetrics, ksqlConfig,
         HARouting::executeOrRouteQuery);
  }


  @VisibleForTesting
  HARouting(
      final RoutingFilterFactory routingFilterFactory,
      final Optional<PullQueryExecutorMetrics> pullQueryMetrics,
      final KsqlConfig ksqlConfig,
      final RouteQuery routeQuery
  ) {
    this.routingFilterFactory =
        Objects.requireNonNull(routingFilterFactory, "routingFilterFactory");
    this.executorService = Executors.newFixedThreadPool(
        ksqlConfig.getInt(KsqlConfig.KSQL_QUERY_PULL_THREAD_POOL_SIZE_CONFIG),
        new ThreadFactoryBuilder().setNameFormat("pull-query-executor-%d").build());
    this.pullQueryMetrics = Objects.requireNonNull(pullQueryMetrics, "pullQueryMetrics");
    this.routeQuery = Objects.requireNonNull(routeQuery);
  }

  @Override
  public void close() {
    executorService.shutdown();
  }

  public CompletableFuture<Void> handlePullQuery(
      final ServiceContext serviceContext,
      final PullPhysicalPlan pullPhysicalPlan,
      final ConfiguredStatement<Query> statement,
      final RoutingOptions routingOptions,
      final LogicalSchema outputSchema,
      final QueryId queryId,
      final PullQueryQueue pullQueryQueue
  ) {
    final List<KsqlPartitionLocation> allLocations = pullPhysicalPlan.getMaterialization().locator()
        .locate(
            pullPhysicalPlan.getKeys(),
            routingOptions,
            routingFilterFactory
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
    executorService.submit(() -> {
      try {
        executeRounds(serviceContext, pullPhysicalPlan, statement, routingOptions, outputSchema,
            queryId, locations, pullQueryQueue);
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
      final LogicalSchema outputSchema,
      final QueryId queryId,
      final List<KsqlPartitionLocation> locations,
      final PullQueryQueue pullQueryQueue
  ) throws InterruptedException {
    // The remaining partition locations to retrieve without error
    List<KsqlPartitionLocation> remainingLocations = ImmutableList.copyOf(locations);
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
    for (int round = 0; ; round++) {
      // Group all partition location objects by their nth round node
      final Map<KsqlNode, List<KsqlPartitionLocation>> groupedByHost
          = groupByHost(statement, remainingLocations, round);

      // Make requests to each host, specifying the partitions we're interested in from
      // this host.
      final Map<KsqlNode, Future<RoutingResult>> futures = new LinkedHashMap<>();
      for (Map.Entry<KsqlNode, List<KsqlPartitionLocation>> entry : groupedByHost.entrySet()) {
        final KsqlNode node = entry.getKey();
        futures.put(node, executorService.submit(
            () -> routeQuery.routeQuery(
                node, entry.getValue(), statement, serviceContext, routingOptions,
                pullQueryMetrics, pullPhysicalPlan, outputSchema, queryId, pullQueryQueue)
        ));
      }

      // Go through all of the results of the requests, either aggregating rows or adding
      // the locations to the nextRoundRemaining list.
      final ImmutableList.Builder<KsqlPartitionLocation> nextRoundRemaining
          = ImmutableList.builder();
      for (Map.Entry<KsqlNode, Future<RoutingResult>> entry : futures.entrySet()) {
        final Future<RoutingResult> future = entry.getValue();
        final KsqlNode node = entry.getKey();
        RoutingResult routingResult = null;
        try {
          routingResult = future.get();
        } catch (ExecutionException e) {
          throw new MaterializationException("Unable to execute pull query", e);
        }
        if (routingResult == RoutingResult.STANDBY_FALLBACK) {
          nextRoundRemaining.addAll(groupedByHost.get(node));
        } else {
          Preconditions.checkState(routingResult == RoutingResult.SUCCESS);
        }
      }
      remainingLocations = nextRoundRemaining.build();

      // If there are no partition locations remaining, then we're done.
      if (remainingLocations.size() == 0) {
        pullQueryQueue.close();
        return;
      }
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

  @VisibleForTesting
  interface RouteQuery {
    RoutingResult routeQuery(
        KsqlNode node,
        List<KsqlPartitionLocation> locations,
        ConfiguredStatement<Query> statement,
        ServiceContext serviceContext,
        RoutingOptions routingOptions,
        Optional<PullQueryExecutorMetrics> pullQueryMetrics,
        PullPhysicalPlan pullPhysicalPlan,
        LogicalSchema outputSchema,
        QueryId queryId,
        PullQueryQueue pullQueryQueue
    );
  }

  @VisibleForTesting
  static RoutingResult executeOrRouteQuery(
      final KsqlNode node,
      final List<KsqlPartitionLocation> locations,
      final ConfiguredStatement<Query> statement,
      final ServiceContext serviceContext,
      final RoutingOptions routingOptions,
      final Optional<PullQueryExecutorMetrics> pullQueryMetrics,
      final PullPhysicalPlan pullPhysicalPlan,
      final LogicalSchema outputSchema,
      final QueryId queryId,
      final PullQueryQueue pullQueryQueue
  ) {
    final BiFunction<List<?>, LogicalSchema, PullQueryRow> rowFactory = (rawRow, schema) ->
        new PullQueryRow(rawRow, schema, Optional.ofNullable(
            routingOptions.getIsDebugRequest() ? node : null));
    if (node.isLocal()) {
      try {
        LOG.debug("Query {} executed locally at host {} at timestamp {}.",
                  statement.getMaskedStatementText(), node.location(), System.currentTimeMillis());
        pullQueryMetrics
            .ifPresent(queryExecutorMetrics -> queryExecutorMetrics.recordLocalRequests(1));
        pullPhysicalPlan.execute(locations, pullQueryQueue,  rowFactory);
        return RoutingResult.SUCCESS;
      } catch (StandbyFallbackException e) {
        LOG.warn("Error executing query locally at node {}. Falling back to standby state which "
                + "may return stale results", node, e.getCause());
        return RoutingResult.STANDBY_FALLBACK;
      } catch (Exception e) {
        throw new KsqlException(
            String.format("Error executing query locally at node %s: %s", node.location(),
                e.getMessage()),
            e
        );
      }
    } else {
      try {
        LOG.debug("Query {} routed to host {} at timestamp {}.",
            statement.getMaskedStatementText(), node.location(), System.currentTimeMillis());
        pullQueryMetrics
            .ifPresent(queryExecutorMetrics -> queryExecutorMetrics.recordRemoteRequests(1));
        forwardTo(node, locations, statement, serviceContext, pullQueryQueue, rowFactory,
            outputSchema);
        return RoutingResult.SUCCESS;
      } catch (StandbyFallbackException e) {
        LOG.warn("Error forwarding query to node {}. Falling back to standby state which may "
                + "return stale results", node.location(), e.getCause());
        return RoutingResult.STANDBY_FALLBACK;
      } catch (Exception e) {
        throw new KsqlException(
            String.format("Error forwarding query to node %s: %s", node.location(), e.getMessage()),
            e
        );
      }
    }
  }

  private static void forwardTo(
      final KsqlNode owner,
      final List<KsqlPartitionLocation> locations,
      final ConfiguredStatement<Query> statement,
      final ServiceContext serviceContext,
      final PullQueryQueue pullQueryQueue,
      final BiFunction<List<?>, LogicalSchema, PullQueryRow> rowFactory,
      final LogicalSchema outputSchema
  ) {

    // Specify the partitions we specifically want to read.  This will prevent reading unintended
    // standby data when we are reading active for example.
    final String partitions = locations.stream()
        .map(location -> Integer.toString(location.getPartition()))
        .collect(Collectors.joining(","));
    // Add skip forward flag to properties
    final Map<String, Object> requestProperties = ImmutableMap.of(
        KsqlRequestConfig.KSQL_REQUEST_QUERY_PULL_SKIP_FORWARDING, true,
        KsqlRequestConfig.KSQL_REQUEST_INTERNAL_REQUEST, true,
        KsqlRequestConfig.KSQL_REQUEST_QUERY_PULL_PARTITIONS, partitions);
    final RestResponse<Integer> response;

    try {
      response = serviceContext
          .getKsqlClient()
          .makeQueryRequest(
              owner.location(),
              statement.getUnMaskedStatementText(),
              statement.getSessionConfig().getOverrides(),
              requestProperties,
              streamedRowsHandler(owner, pullQueryQueue, rowFactory, outputSchema)
          );
    } catch (Exception e) {
      // If we threw some explicit exception, then let it bubble up. All of the row handling is
      // wrapped in a KsqlException, so any intentional exception or bug will be surfaced.
      final KsqlException ksqlException = causedByKsqlException(e);
      if (ksqlException != null) {
        throw ksqlException;
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

  private static Consumer<List<StreamedRow>> streamedRowsHandler(
      final KsqlNode owner,
      final PullQueryQueue pullQueryQueue,
      final BiFunction<List<?>, LogicalSchema, PullQueryRow> rowFactory,
      final LogicalSchema outputSchema
  ) {
    final AtomicInteger processedRows = new AtomicInteger(0);
    final AtomicReference<Header> header = new AtomicReference<>();
    return streamedRows -> {
      try {
        if (streamedRows == null || streamedRows.isEmpty()) {
          return;
        }
        final List<PullQueryRow> rows = new ArrayList<>();

        // If this is the first row overall, skip the header
        final int previousProcessedRows = processedRows.getAndAdd(streamedRows.size());
        for (int i = 0; i < streamedRows.size(); i++) {
          final StreamedRow row = streamedRows.get(i);
          if (i == 0 && previousProcessedRows == 0) {
            final Optional<Header> optionalHeader = row.getHeader();
            optionalHeader.ifPresent(h -> validateSchema(outputSchema, h.getSchema(), owner));
            optionalHeader.ifPresent(header::set);
            continue;
          }

          if (row.getErrorMessage().isPresent()) {
            // If we receive an error that's not a network error, we let that bubble up.
            throw new KsqlException(row.getErrorMessage().get().getMessage());
          }

          if (!row.getRow().isPresent()) {
            throw new KsqlException("Missing row data on row " + i + " of chunk");
          }

          final List<?> r = row.getRow().get().getColumns();
          Preconditions.checkNotNull(header.get());
          rows.add(rowFactory.apply(r, header.get().getSchema()));
        }

        if (!pullQueryQueue.acceptRows(rows)) {
          LOG.error("Failed to queue all rows");
        }
      } catch (Exception e) {
        throw new KsqlException("Error handling streamed rows: " + e.getMessage(), e);
      }
    };
  }

  private static void validateSchema(
      final LogicalSchema expectedSchema,
      final LogicalSchema forwardedSchema,
      final KsqlNode forwardedNode
  ) {
    if (!forwardedSchema.equals(expectedSchema)) {
      throw new KsqlException(String.format(
          "Schemas %s from host %s differs from schema %s",
          forwardedSchema, forwardedNode, expectedSchema));
    }
  }

  private enum RoutingResult {
    SUCCESS,
    STANDBY_FALLBACK
  }
}
