/*
 * Copyright 2021 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.execution.scalablepush.consumer;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.scalablepush.ProcessingQueue;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.PushOffsetRange;
import java.time.Clock;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A consumer which runs alongside a {@link LatestConsumer}, keeping the same assignments that
 * latest has. This allows a given {@link CatchupConsumer} to eventually catchup and coordinate a
 * swap over so that the catchup can be closed.
 */
public class CatchupConsumer extends ScalablePushConsumer {
  private static final Logger LOG = LoggerFactory.getLogger(CatchupConsumer.class);
  @VisibleForTesting
  static final long WAIT_FOR_ASSIGNMENT_MS = 15000;

  @VisibleForTesting
  protected final Supplier<LatestConsumer> latestConsumerSupplier;
  private final CatchupCoordinator catchupCoordinator;
  private final PushOffsetRange offsetRange;
  private final Consumer<Long> sleepMs;
  private final BiConsumer<Object, Long> waitMs;
  private final long catchupWindow;
  private final AtomicBoolean signalledLatest = new AtomicBoolean(false);
  @VisibleForTesting
  protected final Consumer<ProcessingQueue> caughtUpCallback;

  @SuppressWarnings("ParameterNumber")
  public CatchupConsumer(
      final String topicName,
      final boolean windowed,
      final LogicalSchema logicalSchema,
      final KafkaConsumer<Object, GenericRow> consumer,
      final Supplier<LatestConsumer> latestConsumerSupplier,
      final CatchupCoordinator catchupCoordinator,
      final PushOffsetRange offsetRange,
      final Clock clock,
      final Consumer<Long> sleepMs,
      final BiConsumer<Object, Long> waitMs,
      final long catchupWindow,
      final Consumer<ProcessingQueue> caughtUpCallback
  ) {
    super(topicName, windowed, logicalSchema, consumer, clock);
    this.latestConsumerSupplier = latestConsumerSupplier;
    this.catchupCoordinator = catchupCoordinator;
    this.offsetRange = offsetRange;
    this.sleepMs = sleepMs;
    this.waitMs = waitMs;
    this.catchupWindow = catchupWindow;
    this.caughtUpCallback = caughtUpCallback;
  }

  public CatchupConsumer(
      final String topicName,
      final boolean windowed,
      final LogicalSchema logicalSchema,
      final KafkaConsumer<Object, GenericRow> consumer,
      final Supplier<LatestConsumer> latestConsumerSupplier,
      final CatchupCoordinator catchupCoordinator,
      final PushOffsetRange offsetRange,
      final Clock clock,
      final long catchupWindow,
      final Consumer<ProcessingQueue> caughtUpCallback
  ) {
    this(topicName, windowed, logicalSchema, consumer, latestConsumerSupplier, catchupCoordinator,
        offsetRange, clock, CatchupConsumer::sleep, CatchupConsumer::wait, catchupWindow,
        caughtUpCallback);
  }

  public interface CatchupConsumerFactory {
    CatchupConsumer create(
        String topicName,
        boolean windowed,
        LogicalSchema logicalSchema,
        KafkaConsumer<Object, GenericRow> consumer,
        Supplier<LatestConsumer> latestConsumerSupplier,
        CatchupCoordinator catchupCoordinator,
        PushOffsetRange pushOffsetRange,
        Clock clock,
        long catchupWindow,
        Consumer<ProcessingQueue> caughtUpCallback
    );
  }

  @Override
  protected void onEmptyRecords() {
    checkCaughtUp();
  }

  @Override
  protected void afterBatchProcessed() {
    checkCaughtUp();
  }

  /**
   * Called when a new assignment has been made with a call to {@link #newAssignment(Collection)}.
   * If it's being called manually, a mapping of starting offsets is given.  This ensures that we
   * take our current position to be the one we seek to rather than the last committed offsets.
   * @param startingOffsets The starting offsets to use, if any.  Otherwise, we use the last
   *     committed.
   */
  protected void onNewAssignment(final Optional<Map<Integer, Long>> startingOffsets) {
    final Set<TopicPartition> tps = waitForNewAssignmentFromLatestConsumer();

    consumer.assign(tps);
    updateCurrentPositions(startingOffsets);
    newAssignment = false;
  }

  /**
   * Called when a new assignment has been made with a call to {@link #newAssignment(Collection)}.
   * Assumes it should take the current position to be the last committed offsets.
   */
  @Override
  protected void onNewAssignment() {
    onNewAssignment(Optional.empty());
  }

  /**
   * The first time we start up a LatestConsumer, if it doesn't already have an assignment, we need
   * to wait for it to give us that assignment by calling {@link #newAssignment}
   * @return The assignment given by latest
   */
  protected synchronized Set<TopicPartition> waitForNewAssignmentFromLatestConsumer() {
    final long startMs = clock.millis();
    Set<TopicPartition> tps = this.topicPartitions.get();
    while (tps == null) {
      final long timeMs = clock.millis();
      final long elapsedMs = timeMs - startMs;
      if (elapsedMs >= WAIT_FOR_ASSIGNMENT_MS) {
        throw new KsqlException("Timed out waiting for assignment from Latest");
      }
      waitMs.accept(this, WAIT_FOR_ASSIGNMENT_MS - elapsedMs);
      tps = this.topicPartitions.get();
    }
    return tps;
  }

  @Override
  protected void subscribeOrAssign() {
    // We should always have a latest consumer running
    final LatestConsumer latestConsumer = latestConsumerSupplier.get();
    Preconditions.checkNotNull(latestConsumer,
        "Latest should always be started before catchup is run");
    // Assign current offsets from the latest consumer.  If the latest consumer doesn't have its
    // assignment yet, this might be null, but we then count on the latest updating this consumer
    // with the assignment when it's received.
    this.newAssignment(latestConsumer.getAssignment());
    final Map<Integer, Long> startingOffsets
        = offsetRange.getEndOffsets().getSparseRepresentation();
    onNewAssignment(Optional.of(startingOffsets));
    // Seek to the provided starting offsets
    for (TopicPartition tp : consumer.assignment()) {
      if (startingOffsets.containsKey(tp.partition()) && startingOffsets.get(tp.partition()) >= 0) {
        consumer.seek(tp, startingOffsets.get(tp.partition()));
      }
    }
  }

  protected void afterOfferedRow(final ProcessingQueue processingQueue) {
    // Since we handle only one request at a time, we can afford to block and wait for the request.
    while (processingQueue.isAtLimit()) {
      LOG.info("Sleeping for a bit since queue is full queryid {}", processingQueue.getQueryId());
      sleepMs.accept(50L);
    }
  }

  /**
   * Checks if the catchup consumer is caught up and will even switch over to latest and close this
   * consumer if finished.
   * @return If the catchup consumer has switched over.
   */
  @VisibleForTesting
  protected void checkCaughtUp() {
    LOG.info("Checking to see if we're caught up");
    final Function<Boolean, Boolean> isCaughtUp = softCaughtUp -> {
      final LatestConsumer lc = latestConsumerSupplier.get();
      if (lc == null) {
        return false;
      }
      final Map<TopicPartition, Long> latestOffsets = lc.getCurrentOffsets();
      if (latestOffsets.isEmpty()) {
        return false;
      }
      return caughtUp(latestOffsets, currentPositions.get(), softCaughtUp, catchupWindow);
    };

    final Runnable switchOver = () -> {
      final LatestConsumer lc = latestConsumerSupplier.get();
      if (lc == null) {
        LOG.warn("Couldn't switch over to latest yet because it's not running");
        return;
      }
      for (final ProcessingQueue processingQueue : processingQueues.values()) {
        LOG.info("Switching over from catchup queryid {} to latest", processingQueue.getQueryId());
        lc.register(processingQueue);
        caughtUpCallback.accept(processingQueue);
      }
      processingQueues.clear();
      close();
    };
    catchupCoordinator.checkShouldCatchUp(signalledLatest, isCaughtUp, switchOver);
  }

  @Override
  public void close() {
    super.close();
    catchupCoordinator.catchupIsClosing(signalledLatest);
  }

  /**
   * Checks if the catchup consumer is caught up.
   * @param latestOffsets The offsets from the latest consumer
   * @param offsets The offsets from this consumer
   * @param softCatchUp If we're doing a soft comparison, as in it's within a margin rather than
   *                    fully caught up.
   * @return If the catchup consumer is caught up
   */
  private static boolean caughtUp(
      final Map<TopicPartition, Long> latestOffsets,
      final Map<TopicPartition, Long> offsets,
      final boolean softCatchUp,
      final long catchupWindow
  ) {
    if (!latestOffsets.keySet().equals(offsets.keySet())) {
      return false;
    } else {
      for (final Map.Entry<TopicPartition, Long> entry : latestOffsets.entrySet()) {
        final TopicPartition tp = entry.getKey();
        final Long latestOffset = entry.getValue();
        final Long offset = offsets.get(tp);
        if (latestOffset == null || offset == null) {
          return false;
        }
        if (softCatchUp) {
          if (offset < latestOffset && (latestOffset - offset) > catchupWindow) {
            return false;
          }
        } else if (offset < latestOffset) {
          return false;
        }
      }
      return true;
    }
  }

  private static void sleep(final long waitMs) {
    try {
      Thread.sleep(waitMs);
    } catch (InterruptedException e) {
      LOG.warn("Interrupted while sleeping", e);
      Thread.currentThread().interrupt();
    }
  }

  private static void wait(final Object object, final long waitMs) {
    try {
      object.wait(waitMs);
    } catch (InterruptedException e) {
      LOG.warn("Interrupted while sleeping", e);
      Thread.currentThread().interrupt();
      throw new KsqlException("Interrupted while waiting for assignment");
    }
  }
}
