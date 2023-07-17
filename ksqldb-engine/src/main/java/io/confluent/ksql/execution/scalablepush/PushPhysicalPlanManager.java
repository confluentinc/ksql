/*
 * Copyright 2021 Confluent Inc.
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

package io.confluent.ksql.execution.scalablepush;

import com.google.common.base.Preconditions;
import io.confluent.ksql.execution.common.QueryRow;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.reactive.BufferedPublisher;
import io.confluent.ksql.util.PushOffsetRange;
import io.vertx.core.Context;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Manages the lifecycle of PushPhysicalPlan objects. Effectively, it wraps them to give a nice
 * consistent interface while allowing reset operations which just create whole new physical plans.
 */
public class PushPhysicalPlanManager {

  private final PushPhysicalPlanCreator pushPhysicalPlanCreator;
  private final AtomicReference<PushPhysicalPlan> pushPhysicalPlan = new AtomicReference<>();

  /**
   * Creates a new manager.
   * @param pushPhysicalPlanCreator The creator to use as a factory for a {@link PushPhysicalPlan}
   * @param catchupConsumerGroup The consumer group to use, if provided
   * @param initialOffsetRange The initial offset range to use when starting the first plan.
   */
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
    Preconditions.checkState(isClosed(), "Must be closed in order to reset");
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
