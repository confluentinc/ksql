package io.confluent.ksql.physical.scalablepush;

import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.util.PushOffsetRange;
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
  }

  public boolean isClosed() {
    return pushPhysicalPlan.get().isClosed();
  }
}
