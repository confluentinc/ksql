package io.confluent.ksql.physical.scalablepush.consumer;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.GenericKey;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.TopicPartition;

public class LatestConsumer extends Consumer {

  private static final long LATEST_CONSUMER_OLDEST_COMMIT_AGE_MS = 10 * 60000;

  private final CatchupCoordinator catchupCoordinator;
  private final java.util.function.Consumer<Collection<TopicPartition>> catchupAssignmentUpdater;

  public LatestConsumer(
      int partitions, String topicName, boolean windowed,
      LogicalSchema logicalSchema, KafkaConsumer<GenericKey, GenericRow> consumer,
      CatchupCoordinator catchupCoordinator,
      java.util.function.Consumer<Collection<TopicPartition>> catchupAssignmentUpdater) {
    super(partitions, topicName, windowed, logicalSchema, consumer);
    this.catchupCoordinator = catchupCoordinator;
    this.catchupAssignmentUpdater = catchupAssignmentUpdater;
  }

  @Override
  protected void initialize() {
    // Initial wait time, giving client connections a chance to be made to avoid having to do
    // any catchups.
    try {
      Thread.sleep(5000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    consumer.subscribe(ImmutableList.of(topicName),
        new ConsumerRebalanceListener() {
          @Override
          public void onPartitionsRevoked(Collection<TopicPartition> collection) {
            System.out.println("Revoked assignment" + collection + " from " + this);
          }

          @Override
          public void onPartitionsAssigned(Collection<TopicPartition> collection) {
            System.out.println("Got assignment " + collection + " from " + this);
            newAssignment(collection);
            catchupAssignmentUpdater.accept(collection);
          }
        });
  }

  @Override
  protected boolean onEmptyRecords() {
    catchupCoordinator.checkShouldWaitForCatchup();
    return false;
  }

  @Override
  protected boolean afterCommit() {
    catchupCoordinator.checkShouldWaitForCatchup();
    return false;
  }

  @Override
  protected void onNewAssignment() {
  }

  @Override
  protected void afterFirstPoll() {
    final Set<TopicPartition> topicPartitions = this.topicPartitions.get();
    long timeMs = System.currentTimeMillis() - LATEST_CONSUMER_OLDEST_COMMIT_AGE_MS;
    HashMap<TopicPartition, Long> timestamps = new HashMap<>();
    for (TopicPartition tp : topicPartitions) {
      timestamps.put(tp, timeMs);
    }
    Map<TopicPartition, OffsetAndTimestamp> offsetAndTimestampMap = consumer.offsetsForTimes(timestamps);
    Map<TopicPartition, OffsetAndMetadata> offsetAndMetadataMap = consumer.committed(topicPartitions);
    for (Map.Entry<TopicPartition, OffsetAndTimestamp> entry : offsetAndTimestampMap.entrySet()) {
      OffsetAndMetadata metadata = offsetAndMetadataMap.get(entry.getKey());
      if (metadata != null && entry.getValue().offset() > metadata.offset()) {
        consumer.seekToEnd(topicPartitions);
        return;
      }
    }
  }
}
