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
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.rest.client.RestResponse;
import io.confluent.ksql.rest.entity.StreamedRow;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.KsqlRequestConfig;
import io.confluent.ksql.util.KsqlServerException;
import io.confluent.ksql.util.KsqlStatementException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
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

  public PullQueryResult handlePullQuery(
      final ServiceContext serviceContext,
      final PullPhysicalPlan pullPhysicalPlan,
      final ConfiguredStatement<Query> statement,
      final RoutingOptions routingOptions,
      final LogicalSchema outputSchema,
      final QueryId queryId
  ) throws InterruptedException {
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
                statement.getStatementText());
      throw new MaterializationException(String.format(
          "Unable to execute pull query %s. All nodes are dead or exceed max allowed lag.",
          statement.getStatementText()));
    }

    // The source nodes associated with each of the rows
    final List<KsqlNode> sourceNodes = new ArrayList<>();
    // Each of the table rows returned, aggregated across nodes
    final List<List<?>> tableRows = new ArrayList<>();
    // Each of the schemas returned, grouped by node
    final Map<KsqlNode, List<LogicalSchema>> schemasByHost = new HashMap<>();
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
      final Map<KsqlNode, Future<PullQueryResult>> futures = new LinkedHashMap<>();
      for (Map.Entry<KsqlNode, List<KsqlPartitionLocation>> entry : groupedByHost.entrySet()) {
        final KsqlNode node = entry.getKey();
        futures.put(node, executorService.submit(
            () -> routeQuery.routeQuery(
                node, entry.getValue(), statement, serviceContext, routingOptions,
                pullQueryMetrics, pullPhysicalPlan, outputSchema, queryId)
        ));
      }

      // Go through all of the results of the requests, either aggregating rows or adding
      // the locations to the nextRoundRemaining list.
      final ImmutableList.Builder<KsqlPartitionLocation> nextRoundRemaining
          = ImmutableList.builder();
      for (Map.Entry<KsqlNode, Future<PullQueryResult>> entry : futures.entrySet()) {
        final Future<PullQueryResult> future = entry.getValue();
        final KsqlNode node = entry.getKey();
        try {
          final PullQueryResult result = future.get();
          result.getSourceNodes().ifPresent(sourceNodes::addAll);
          schemasByHost.putIfAbsent(node, new ArrayList<>());
          schemasByHost.get(node).add(result.getSchema());
          tableRows.addAll(result.getTableRows());
        } catch (ExecutionException e) {
          LOG.warn("Error routing query {} to host {} at timestamp {} with exception {}",
                   statement.getStatementText(), node, System.currentTimeMillis(), e.getCause());
          nextRoundRemaining.addAll(groupedByHost.get(node));
        }
      }
      remainingLocations = nextRoundRemaining.build();

      // If there are no partition locations remaining, then we're done.
      if (remainingLocations.size() == 0) {
        final LogicalSchema schema = validateSchemas(schemasByHost);
        return new PullQueryResult(
            tableRows,
            sourceNodes.isEmpty() ? Optional.empty() : Optional.of(sourceNodes),
            schema,
            queryId);
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
            statement.getStatementText()));
      }
      final KsqlNode nextHost = location.getNodes().get(round);
      groupedByHost.computeIfAbsent(nextHost, h -> new ArrayList<>()).add(location);
    }
    return groupedByHost;
  }

  @VisibleForTesting
  interface RouteQuery {
    PullQueryResult routeQuery(
        KsqlNode node,
        List<KsqlPartitionLocation> locations,
        ConfiguredStatement<Query> statement,
        ServiceContext serviceContext,
        RoutingOptions routingOptions,
        Optional<PullQueryExecutorMetrics> pullQueryMetrics,
         PullPhysicalPlan pullPhysicalPlan,
         LogicalSchema outputSchema,
         QueryId queryId
    );
  }

  @VisibleForTesting
  static PullQueryResult executeOrRouteQuery(
      final KsqlNode node,
      final List<KsqlPartitionLocation> locations,
      final ConfiguredStatement<Query> statement,
      final ServiceContext serviceContext,
      final RoutingOptions routingOptions,
      final Optional<PullQueryExecutorMetrics> pullQueryMetrics,
      final PullPhysicalPlan pullPhysicalPlan,
      final LogicalSchema outputSchema,
      final QueryId queryId
  ) {
    List<List<?>> rows = null;
    if (node.isLocal()) {
      LOG.debug("Query {} executed locally at host {} at timestamp {}.",
                statement.getStatementText(), node.location(), System.currentTimeMillis());
      pullQueryMetrics
          .ifPresent(queryExecutorMetrics -> queryExecutorMetrics.recordLocalRequests(1));
      rows = pullPhysicalPlan.execute(locations);
    } else {
      LOG.debug("Query {} routed to host {} at timestamp {}.",
                statement.getStatementText(), node.location(), System.currentTimeMillis());
      pullQueryMetrics
          .ifPresent(queryExecutorMetrics -> queryExecutorMetrics.recordRemoteRequests(1));
      rows = forwardTo(node, locations, statement, serviceContext);
    }
    final Optional<List<KsqlNode>> debugNodes = Optional.ofNullable(
        routingOptions.getIsDebugRequest()
            ? Collections.nCopies(rows.size(), node) : null);
    return new PullQueryResult(
        rows,
        debugNodes,
        outputSchema,
        queryId);
  }

  private static List<List<?>> forwardTo(
      final KsqlNode owner,
      final List<KsqlPartitionLocation> locations,
      final ConfiguredStatement<Query> statement,
      final ServiceContext serviceContext
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
    final RestResponse<List<StreamedRow>> response = serviceContext
        .getKsqlClient()
        .makeQueryRequest(
            owner.location(),
            statement.getStatementText(),
            statement.getSessionConfig().getOverrides(),
            requestProperties
        );

    if (response.isErroneous()) {
      throw new KsqlServerException(String.format(
          "Forwarding pull query request [%s, %s, %s] to node %s failed with error %s ",
          statement.getStatement(), statement.getSessionConfig().getOverrides(), requestProperties,
          owner, response.getErrorMessage()));
    }

    final List<StreamedRow> streamedRows = response.getResponse();
    if (streamedRows.isEmpty()) {
      throw new KsqlServerException(String.format(
          "Forwarding pull query request [%s, %s, %s] to node %s failed due to invalid "
              + "empty response from forwarding call, expected a header row.",
          statement.getStatement(), statement.getSessionConfig().getOverrides(), requestProperties,
          owner));
    }

    final List<List<?>> rows = new ArrayList<>();

    for (final StreamedRow row : streamedRows.subList(1, streamedRows.size())) {
      if (row.getErrorMessage().isPresent()) {
        throw new KsqlStatementException(
            row.getErrorMessage().get().getMessage(),
            statement.getStatementText()
        );
      }

      if (!row.getRow().isPresent()) {
        throw new KsqlServerException(String.format(
            "Forwarding pull query request [%s, %s, %s] to node %s failed due to "
                + "missing row data.",
            statement.getStatement(), statement.getSessionConfig().getOverrides(),
            requestProperties, owner));
      }

      rows.add(row.getRow().get().getColumns());
    }

    return rows;
  }

  private LogicalSchema validateSchemas(final Map<KsqlNode, List<LogicalSchema>> schemasByNode) {
    LogicalSchema compareAgainst = null;
    KsqlNode host = null;
    for (Entry<KsqlNode, List<LogicalSchema>> entry: schemasByNode.entrySet()) {
      final KsqlNode node = entry.getKey();
      final List<LogicalSchema> schemas = entry.getValue();
      if (compareAgainst == null) {
        compareAgainst = schemas.get(0);
        host = node;
      }
      for (LogicalSchema s : schemas) {
        if (!s.equals(compareAgainst)) {
          throw new KsqlException(String.format(
              "Schemas %s from host %s differs from schema %s from hosts %s",
              s, node, compareAgainst, host));
        }
      }
    }
    return compareAgainst;
  }


}
