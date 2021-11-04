package io.confluent.ksql.physical.scalablepush.consumer;

import io.confluent.ksql.GenericRow;
import io.confluent.ksql.physical.scalablepush.ProcessingQueue;
import io.confluent.ksql.physical.scalablepush.TokenUtils;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.util.PushOffsetRange;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CatchupConsumer extends ScalablePushConsumer {
  private static final Logger LOG = LoggerFactory.getLogger(CatchupConsumer.class);

  private final Runnable ensureLatestIsRunning;
  private final Supplier<LatestConsumer> latestConsumerSupplier;
  private final CatchupCoordinator catchupCoordinator;
  private final PushOffsetRange offsetRange;
  private AtomicBoolean blocked = new AtomicBoolean(false);

  public CatchupConsumer(
      String topicName,
      boolean windowed,
      LogicalSchema logicalSchema,
      KafkaConsumer<Object, GenericRow> consumer,
      Runnable ensureLatestIsRunning,
      Supplier<LatestConsumer> latestConsumerSupplier,
      CatchupCoordinator catchupCoordinator,
      PushOffsetRange offsetRange
  ) {
    super(topicName, windowed, logicalSchema, consumer);
    this.ensureLatestIsRunning = ensureLatestIsRunning;
    this.latestConsumerSupplier = latestConsumerSupplier;
    this.catchupCoordinator = catchupCoordinator;
    this.offsetRange = offsetRange;
  }

  @Override
  protected boolean onEmptyRecords() {
    return checkCaughtUp(consumer, blocked);
  }

  @Override
  protected boolean afterCommit() {
    return checkCaughtUp(consumer, blocked);
  }

  @Override
  protected void onNewAssignment() {
    Set<TopicPartition> tps = waitForNewAssignmentFromLatestConsumer();

    consumer.assign(tps);
    resetCurrentPosition();
    newAssignment = false;
  }

  protected Set<TopicPartition> waitForNewAssignmentFromLatestConsumer() {
    Set<TopicPartition> tps = this.topicPartitions.get();
    while (tps == null) {
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      tps = this.topicPartitions.get();
    }
    return tps;
  }

  @Override
  protected void subscribeOrAssign() {
    ensureLatestIsRunning.run();
    onNewAssignment();
    Map<Integer, Long> startingOffsets = TokenUtils.parseToken(Optional.of(offsetRange.getEndOffsets().getDenseRepresentation()));
    for (TopicPartition tp : consumer.assignment()) {
      consumer.seek(tp,
          startingOffsets.containsKey(tp.partition()) ? startingOffsets.get(tp.partition())
              : 0);
    }
  }

  protected void afterOfferedRow(final ProcessingQueue processingQueue) {
    // Since we handle only one request at a time, we can afford to block and wait for the request.
    while (processingQueue.isAtLimit()) {
      try {
        System.out.println("Sleeping for a bit");
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }

  private boolean checkCaughtUp(KafkaConsumer<Object, GenericRow> consumer, AtomicBoolean blocked) {
    System.out.println("CHECKING CAUGHT UP!!");
    Map<TopicPartition, OffsetAndMetadata> offsets = consumer.committed(new HashSet<>(topicPartitions.get()));

    final Supplier<Boolean> isCaughtUp = () -> {
      LatestConsumer lc = latestConsumerSupplier.get();
      System.out.println("LATEST IS " + lc);
      if (lc == null) {
        return false;
      }
      Map<TopicPartition, OffsetAndMetadata> latestOffsets = lc.getCommittedOffsets();
      System.out.println("latestOffsets " + latestOffsets);
      if (latestOffsets.isEmpty()) {
        return false;
      }
      System.out.println("Comparing " + offsets + " and " + latestOffsets);
      return caughtUp(latestOffsets, offsets);
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
    return catchupCoordinator.checkShouldCatchUp(blocked, isCaughtUp, switchOver);
  }

  private static boolean caughtUp(Map<TopicPartition, OffsetAndMetadata> latestOffsets, Map<TopicPartition, OffsetAndMetadata> offsets) {
    if (!latestOffsets.keySet().equals(offsets.keySet())) {
      return false;
    } else {
      for (TopicPartition tp : latestOffsets.keySet()) {
        OffsetAndMetadata latestOam = latestOffsets.get(tp);
        OffsetAndMetadata oam = offsets.get(tp);
        if (latestOam == null || oam == null) {
          return false;
        }
        long latestOffset = latestOam.offset();
        long offset = oam.offset();
        if (offset < latestOffset) {
          return false;
        }
      }
      return true;
    }
  }
}
