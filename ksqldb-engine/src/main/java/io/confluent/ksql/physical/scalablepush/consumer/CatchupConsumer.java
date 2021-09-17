package io.confluent.ksql.physical.scalablepush.consumer;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.GenericKey;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.physical.scalablepush.ProcessingQueue;
import io.confluent.ksql.physical.scalablepush.TokenUtils;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

public class CatchupConsumer extends Consumer {

  private final Runnable ensureLatestIsRunning;
  private final Supplier<LatestConsumer> latestConsumerSupplier;
  private final CatchupCoordinator catchupCoordinator;
  private final String token;
  private AtomicBoolean blocked = new AtomicBoolean(false);
  private boolean explicitAssignmentMode = false;

  public CatchupConsumer(
      int partitions,
      String topicName,
      boolean windowed,
      LogicalSchema logicalSchema,
      KafkaConsumer<GenericKey, GenericRow> consumer,
      Runnable ensureLatestIsRunning,
      Supplier<LatestConsumer> latestConsumerSupplier,
      CatchupCoordinator catchupCoordinator,
      String token
  ) {
    super(partitions, topicName, windowed, logicalSchema, consumer);
    this.ensureLatestIsRunning = ensureLatestIsRunning;
    this.latestConsumerSupplier = latestConsumerSupplier;
    this.catchupCoordinator = catchupCoordinator;
    this.token = token;
  }

  public void setExplicitAssignmentMode(boolean explicitAssignmentMode) {
    this.explicitAssignmentMode = explicitAssignmentMode;
  }

  public boolean isExplicitAssignmentMode() {
    return explicitAssignmentMode;
  }

  @Override
  protected boolean onEmptyRecords() {
    return checkCaughtUp(consumer, blocked);
  }

  @Override
  protected boolean afterCommit() {
    if (checkNearEnd(consumer)) {
      return false;
    }
    return checkCaughtUp(consumer, blocked);
  }

  @Override
  protected void onNewAssignment() {
    if (!isExplicitAssignmentMode()) {
      return;
    }
    Set<TopicPartition> tps = waitForNewAssignmentFromLatestConsumer();

    consumer.assign(tps);
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
  protected void afterFirstPoll() {
  }

  @Override
  protected void initialize() {
    if (isExplicitAssignmentMode()) {
      onNewAssignment();
      Map<Integer, Long> startingOffsets = TokenUtils.parseToken(Optional.of(token));
      for (TopicPartition tp : consumer.assignment()) {
        consumer.seek(tp,
            startingOffsets.containsKey(tp.partition()) ? startingOffsets.get(tp.partition())
                : 0);
      }
    } else {
      consumer.subscribe(ImmutableList.of(topicName),
          new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> collection) {
              System.out.println("CATCHUP: Revoked assignment" + collection);
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> collection) {
              System.out.println("CATCHUP:  Got assignment " + collection);
              newAssignment(collection);
              Map<Integer, Long> startingOffsets = TokenUtils.parseToken(Optional.of(token));
              for (TopicPartition tp : consumer.assignment()) {
                consumer.seek(tp, startingOffsets.containsKey(tp.partition()) ? startingOffsets
                    .get(tp.partition()) : 0);
              }
            }
          });
    }
  }

  private boolean checkNearEnd(KafkaConsumer<GenericKey, GenericRow> consumer) {
    if (isExplicitAssignmentMode()) {
      return false;
    }
    System.out.println("CHECKING Near end!!");
    Map<TopicPartition, OffsetAndMetadata> offsets = consumer.committed(topicPartitions.get());
    Map<TopicPartition, Long> endOffsets = consumer.endOffsets(topicPartitions.get());
    System.out.println("Comparing " + offsets + " and " + endOffsets);
    for (TopicPartition tp : endOffsets.keySet()) {
      OffsetAndMetadata latestOam = offsets.get(tp);
      long endOffset = endOffsets.get(tp);
      if (endOffset - latestOam.offset() < 1000) {
        this.topicPartitions.set(null);
        consumer.unsubscribe();
        ensureLatestIsRunning.run();
        setExplicitAssignmentMode(true);
        onNewAssignment();
        return true;
      }
    }
    return false;
  }

  private boolean checkCaughtUp(KafkaConsumer<GenericKey, GenericRow> consumer, AtomicBoolean blocked) {
    System.out.println("CHECKING CAUGHT UP!!");
    Map<TopicPartition, OffsetAndMetadata> offsets = consumer.committed(new HashSet<>(topicPartitions.get()));

    final Supplier<Boolean> isCaughtUp = () -> {
      LatestConsumer lc = latestConsumerSupplier.get();
      if (lc == null) {
        return false;
      }
      Map<TopicPartition, OffsetAndMetadata> latestOffsets = lc.getCurrentOffsets();
      if (latestOffsets == null) {
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
