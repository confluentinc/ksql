package io.confluent.ksql.physical.scalablepush;

import io.confluent.ksql.physical.common.QueryRow;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.reactive.BufferedPublisher;
import io.confluent.ksql.util.PushOffsetRange;
import io.vertx.core.Context;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

public class PushPhysicalPlanManager {

  private final PushPhysicalPlanCreator pushPhysicalPlanCreator;
  private final AtomicReference<PushPhysicalPlan> pushPhysicalPlan = new AtomicReference<>();

  public PushPhysicalPlanManager(
      final PushPhysicalPlanCreator pushPhysicalPlanCreator,
      final Optional<String> catchupConsumerGroup,
      final Optional<PushOffsetRange> initialOffsetRange
  ) {
    this.pushPhysicalPlanCreator = pushPhysicalPlanCreator;
    pushPhysicalPlan.set(pushPhysicalPlanCreator.create(initialOffsetRange, catchupConsumerGroup));
  }

  public QueryId getQueryId() {
    return pushPhysicalPlan.get().getQueryId();
  }

  public PushPhysicalPlan getPhysicalPlan() {
    return pushPhysicalPlan.get();
  }

  public void reset(final Optional<PushOffsetRange> newOffsetRange) {
    final PushPhysicalPlan newPlan = pushPhysicalPlanCreator.create(newOffsetRange,
        Optional.of(pushPhysicalPlan.get().getCatchupConsumerGroupId()));
    pushPhysicalPlan.set(newPlan);
  }

  public boolean isClosed() {
    return pushPhysicalPlan.get().isClosed();
  }

  public ScalablePushRegistry getScalablePushRegistry() {
    return pushPhysicalPlan.get().getScalablePushRegistry();
  }

  public String getCatchupConsumerGroupId() {
    return pushPhysicalPlan.get().getCatchupConsumerGroupId();
  }

  public BufferedPublisher<QueryRow> execute() {
    return pushPhysicalPlan.get().execute();
  }

  public Runnable closeable() {
    final PushPhysicalPlan plan = pushPhysicalPlan.get();
    return plan::close;
  }

  public Context getContext() {
    return pushPhysicalPlan.get().getContext();
  }
}
