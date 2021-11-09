package io.confluent.ksql.physical.scalablepush.consumer;

import com.google.common.base.Preconditions;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.physical.scalablepush.ProcessingQueue;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.PushOffsetRange;
import java.time.Clock;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CatchupConsumer extends ScalablePushConsumer {
  private static final Logger LOG = LoggerFactory.getLogger(CatchupConsumer.class);
  private static final long WAIT_FOR_ASSIGNMENT_MS = 15000;

  private final Supplier<LatestConsumer> latestConsumerSupplier;
  private final CatchupCoordinator catchupCoordinator;
  private final PushOffsetRange offsetRange;
  private final Consumer<Long> sleepMs;
  private AtomicBoolean signalledLatest = new AtomicBoolean(false);

  public CatchupConsumer(
      String topicName,
      boolean windowed,
      LogicalSchema logicalSchema,
      KafkaConsumer<Object, GenericRow> consumer,
      Supplier<LatestConsumer> latestConsumerSupplier,
      CatchupCoordinator catchupCoordinator,
      PushOffsetRange offsetRange,
      Clock clock,
      Consumer<Long> sleepMs
  ) {
    super(topicName, windowed, logicalSchema, consumer, clock);
    this.latestConsumerSupplier = latestConsumerSupplier;
    this.catchupCoordinator = catchupCoordinator;
    this.offsetRange = offsetRange;
    this.sleepMs = sleepMs;
  }

  public CatchupConsumer(
      String topicName,
      boolean windowed,
      LogicalSchema logicalSchema,
      KafkaConsumer<Object, GenericRow> consumer,
      Supplier<LatestConsumer> latestConsumerSupplier,
      CatchupCoordinator catchupCoordinator,
      PushOffsetRange offsetRange,
      Clock clock
  ) {
    this(topicName, windowed, logicalSchema, consumer, latestConsumerSupplier, catchupCoordinator,
        offsetRange, clock, CatchupConsumer::sleep);
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
        Clock clock
    );
  }

  @Override
  protected boolean onEmptyRecords() {
    return checkCaughtUp();
  }

  @Override
  protected boolean afterCommit() {
    return checkCaughtUp();
  }

  protected void onNewAssignment(final Optional<Map<Integer, Long>> startingOffsets) {
    System.out.println("onNewAssignment ");
    Set<TopicPartition> tps = waitForNewAssignmentFromLatestConsumer();

    System.out.println("onNewAssignment assign");
    consumer.assign(tps);
    resetCurrentPosition(startingOffsets);
    System.out.println("onNewAssignment doneReset");
    newAssignment = false;
  }

  @Override
  protected void onNewAssignment() {
    onNewAssignment(Optional.empty());
  }

  /**
   * The first time we start up a LatestConsumer, if it doesn't already have an assignment, we need
   * to wait for it to give us that assignment by calling {@link #newAssignment}
   * @return
   */
  protected Set<TopicPartition> waitForNewAssignmentFromLatestConsumer() {
    long startMs = clock.millis();
    Set<TopicPartition> tps = this.topicPartitions.get();
    while (tps == null) {
      if (clock.millis() - startMs > WAIT_FOR_ASSIGNMENT_MS) {
        throw new KsqlException("Timed out waiting for assignment from Latest");
      }
      sleepMs.accept(100L);
      tps = this.topicPartitions.get();
    }
    return tps;
  }

  @Override
  protected void subscribeOrAssign() {
    final LatestConsumer latestConsumer = latestConsumerSupplier.get();
    Preconditions.checkNotNull(latestConsumer,
        "Latest should always be started before catchup is run");
    this.newAssignment(latestConsumer.getAssignment());
    Map<Integer, Long> startingOffsets = offsetRange.getEndOffsets().getSparseRepresentation();
    onNewAssignment(Optional.of(startingOffsets));
    System.out.println("subscribeOrAssign CATCHUP " + startingOffsets);
    for (TopicPartition tp : consumer.assignment()) {
      if (startingOffsets.containsKey(tp.partition()) && startingOffsets.get(tp.partition()) >= 0) {
        consumer.seek(tp, startingOffsets.get(tp.partition()));
      }
    }
  }

  protected void afterOfferedRow(final ProcessingQueue processingQueue) {
    // Since we handle only one request at a time, we can afford to block and wait for the request.
    while (processingQueue.isAtLimit()) {
      System.out.println("Sleeping for a bit");
      sleepMs.accept(1000L);
    }
  }

  private boolean checkCaughtUp() {
    System.out.println("CHECKING CAUGHT UP!!");

    final Supplier<Boolean> isCaughtUp = () -> {
      LatestConsumer lc = latestConsumerSupplier.get();
      System.out.println("LATEST IS " + lc);
      if (lc == null) {
        return false;
      }
      Map<TopicPartition, Long> latestOffsets = lc.getCurrentOffsets();
      System.out.println("latestOffsets " + latestOffsets);
      if (latestOffsets.isEmpty()) {
        return false;
      }
      System.out.println("Comparing " + currentPositions + " and " + latestOffsets);
      return caughtUp(latestOffsets, currentPositions);
    };

    final Runnable switchOver = () -> {
      System.out.println("SWITCHING OVER");
      LatestConsumer lc = latestConsumerSupplier.get();
      if (lc == null) {
        System.out.println("NULL LATEST!");
        return;
      }
      for (final ProcessingQueue processingQueue : processingQueues.values()) {
        lc.register(processingQueue);
      }
      processingQueues.clear();
      close();
    };
    return catchupCoordinator.checkShouldCatchUp(signalledLatest, isCaughtUp, switchOver);
  }

  private static boolean caughtUp(
      final Map<TopicPartition, Long> latestOffsets,
      final Map<TopicPartition, Long> offsets
  ) {
    if (!latestOffsets.keySet().equals(offsets.keySet())) {
      return false;
    } else {
      for (TopicPartition tp : latestOffsets.keySet()) {
        final Long latestOffset = latestOffsets.get(tp);
        final Long offset = offsets.get(tp);
        if (latestOffset == null || offset == null) {
          return false;
        }
        if (offset < latestOffset) {
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
}
