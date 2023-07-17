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
import io.confluent.ksql.util.KsqlServerException;
import io.confluent.ksql.util.KsqlStatementException;
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
    final List<KsqlPartitionLocation> locations = pullPhysicalPlan.getMaterialization().locator()
        .locate(
            pullPhysicalPlan.getKeys(),
            routingOptions,
            routingFilterFactory
    );

    final boolean anyPartitionsEmpty = locations.stream()
        .anyMatch(location -> location.getNodes().isEmpty());
    if (anyPartitionsEmpty) {
      LOG.debug("Unable to execute pull query: {}. All nodes are dead or exceed max allowed lag.",
                statement.getMaskedStatementText());
      throw new MaterializationException(String.format(
          "Unable to execute pull query %s. All nodes are dead or exceed max allowed lag.",
          statement.getMaskedStatementText()));
    }

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
      final Map<KsqlNode, Future<Void>> futures = new LinkedHashMap<>();
      for (Map.Entry<KsqlNode, List<KsqlPartitionLocation>> entry : groupedByHost.entrySet()) {
        final KsqlNode node = entry.getKey();
        futures.put(node, executorService.submit(
            () -> {
              routeQuery.routeQuery(
                  node, entry.getValue(), statement, serviceContext, routingOptions,
                  pullQueryMetrics, pullPhysicalPlan, outputSchema, queryId, pullQueryQueue);
              return null;
            }
        ));
      }

      // Go through all of the results of the requests, either aggregating rows or adding
      // the locations to the nextRoundRemaining list.
      final ImmutableList.Builder<KsqlPartitionLocation> nextRoundRemaining
          = ImmutableList.builder();
      for (Map.Entry<KsqlNode, Future<Void>> entry : futures.entrySet()) {
        final Future<Void> future = entry.getValue();
        final KsqlNode node = entry.getKey();
        try {
          future.get();
        } catch (ExecutionException e) {
          LOG.warn("Error routing query {} to host {} at timestamp {} with exception {}",
              statement.getMaskedStatementText(), node, System.currentTimeMillis(), e.getCause());
          nextRoundRemaining.addAll(groupedByHost.get(node));
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
        throw new MaterializationException(String.format(
            "Unable to execute pull query: %s. Exhausted standby hosts to try.",
            statement.getMaskedStatementText()));
      }
      final KsqlNode nextHost = location.getNodes().get(round);
      groupedByHost.computeIfAbsent(nextHost, h -> new ArrayList<>()).add(location);
    }
    return groupedByHost;
  }

  @VisibleForTesting
  interface RouteQuery {
    void routeQuery(
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
  static void executeOrRouteQuery(
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
      } catch (Exception e) {
        LOG.error("Error executing query {} locally at node {} with exception",
            statement.getMaskedStatementText(), node, e.getCause());
        throw new KsqlException(
            String.format("Error executing query %s locally at node %s",
                          statement.getMaskedStatementText(), node),
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
      } catch (Exception e) {
        LOG.error("Error forwarding query {} to node {} with exception {}",
                  statement.getMaskedStatementText(), node, e.getCause());
        throw new KsqlException(
            String.format("Error forwarding query %s to node %s",
                          statement.getMaskedStatementText(), node),
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
    final RestResponse<Integer> response = serviceContext
        .getKsqlClient()
        .makeQueryRequest(
            owner.location(),
            statement.getUnMaskedStatementText(),
            statement.getSessionConfig().getOverrides(),
            requestProperties,
            streamedRowsHandler(owner, statement, requestProperties, pullQueryQueue, rowFactory,
                outputSchema)
        );

    if (response.isErroneous()) {
      throw new KsqlServerException(String.format(
          "Forwarding pull query request [%s, %s, %s] to node %s failed with error %s ",
          statement.getStatement(), statement.getSessionConfig().getOverrides(), requestProperties,
          owner, response.getErrorMessage()));
    }

    final int numRows = response.getResponse();
    if (numRows == 0) {
      throw new KsqlServerException(String.format(
          "Forwarding pull query request [%s, %s, %s] to node %s failed due to invalid "
              + "empty response from forwarding call, expected a header row.",
          statement.getStatement(), statement.getSessionConfig().getOverrides(), requestProperties,
          owner));
    }
  }

  private static Consumer<List<StreamedRow>> streamedRowsHandler(
      final KsqlNode owner,
      final ConfiguredStatement<Query> statement,
      final Map<String, Object> requestProperties,
      final PullQueryQueue pullQueryQueue,
      final BiFunction<List<?>, LogicalSchema, PullQueryRow> rowFactory,
      final LogicalSchema outputSchema
  ) {
    final AtomicInteger processedRows = new AtomicInteger(0);
    final AtomicReference<Header> header = new AtomicReference<>();
    return streamedRows -> {
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
          throw new KsqlStatementException(
              row.getErrorMessage().get().getMessage(),
              statement.getMaskedStatementText()
          );
        }

        if (!row.getRow().isPresent()) {
          throw new KsqlServerException(String.format(
              "Forwarding pull query request [%s, %s, %s] to node %s failed due to "
                  + "missing row data.",
              statement.getStatement(), statement.getSessionConfig().getOverrides(),
              requestProperties, owner));
        }

        final List<?> r = row.getRow().get().getColumns();
        Preconditions.checkNotNull(header.get());
        rows.add(rowFactory.apply(r, header.get().getSchema()));
      }

      if (!pullQueryQueue.acceptRows(rows)) {
        LOG.info("Failed to queue all rows");
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
}
