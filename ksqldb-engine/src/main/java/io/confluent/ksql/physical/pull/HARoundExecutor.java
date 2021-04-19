package io.confluent.ksql.physical.pull;

import io.confluent.ksql.execution.streams.materialization.Locator.KsqlNode;
import io.confluent.ksql.execution.streams.materialization.Locator.KsqlPartitionLocation;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.physical.pull.HARouting.RoutingResult;
import io.confluent.ksql.statement.ConfiguredStatement;
import java.util.List;
import java.util.concurrent.ExecutorService;

public interface HARoundExecutor {

  void executeRounds(
      ExecutorService executorService,
      ConfiguredStatement<Query> statement,
      List<KsqlPartitionLocation> locations,
      QueryRouter queryRouter,
      Runnable onComplete
  ) throws InterruptedException;

  interface QueryRouter {
    RoutingResult routeQuery(KsqlNode node, List<KsqlPartitionLocation> partitionLocations);
  }
}
