package io.confluent.ksql.physical.pull;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import io.confluent.ksql.execution.streams.materialization.Locator.KsqlNode;
import io.confluent.ksql.execution.streams.materialization.Locator.KsqlPartitionLocation;
import io.confluent.ksql.execution.streams.materialization.MaterializationException;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.physical.pull.HARouting.RoutingResult;
import io.confluent.ksql.statement.ConfiguredStatement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PullHARoundExecutor implements HARoundExecutor {

  private static final Logger LOG = LoggerFactory.getLogger(PullHARoundExecutor.class);

  public void executeRounds(
      final ExecutorService executorService,
      final ConfiguredStatement<Query> statement,
      final List<KsqlPartitionLocation> locations,
      final QueryRouter queryRouter,
      final Runnable onComplete
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
            () -> queryRouter.routeQuery(node, entry.getValue()))
        );
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
          LOG.warn("Error routing query {} to host {} at timestamp {} with exception {}",
              statement.getStatementText(), node, System.currentTimeMillis(), e.getCause());
          throw new MaterializationException(String.format(
              "Unable to execute pull query \"%s\". %s",
              statement.getStatementText(), e.getCause().getMessage()));
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
        onComplete.run();
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
            "Unable to execute pull query: \"%s\". Exhausted standby hosts to try.",
            statement.getStatementText()));
      }
      final KsqlNode nextHost = location.getNodes().get(round);
      groupedByHost.computeIfAbsent(nextHost, h -> new ArrayList<>()).add(location);
    }
    return groupedByHost;
  }
}
