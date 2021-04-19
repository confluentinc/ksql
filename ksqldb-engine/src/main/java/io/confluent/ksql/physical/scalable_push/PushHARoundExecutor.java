package io.confluent.ksql.physical.scalable_push;

import io.confluent.ksql.execution.streams.materialization.Locator.KsqlPartitionLocation;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.physical.pull.HARoundExecutor;
import io.confluent.ksql.statement.ConfiguredStatement;
import java.util.List;
import java.util.concurrent.ExecutorService;

public class PushHARoundExecutor implements HARoundExecutor {

  @Override
  public void executeRounds(ExecutorService executorService, ConfiguredStatement<Query> statement,
      List<KsqlPartitionLocation> locations, QueryRouter queryRouter, Runnable onComplete)
      throws InterruptedException {

  }
}
