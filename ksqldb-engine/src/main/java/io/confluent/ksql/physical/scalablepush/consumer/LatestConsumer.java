package io.confluent.ksql.physical.scalablepush.consumer;

import io.confluent.ksql.GenericKey;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.TopicPartition;

public class LatestConsumer extends Consumer {

  private static final long LATEST_CONSUMER_OLDEST_COMMIT_AGE_MS = 10 * 60000;

  private final CatchupCoordinator catchupCoordinator;

  public LatestConsumer(int partitions, String topicName, boolean windowed,
      LogicalSchema logicalSchema, KafkaConsumer<GenericKey, GenericRow> consumer,
      CatchupCoordinator catchupCoordinator) {
    super(partitions, topicName, windowed, logicalSchema, consumer);
    this.catchupCoordinator = catchupCoordinator;
  }

  @Override
  public boolean onEmptyRecords() {
    catchupCoordinator.checkShouldWaitForCatchup();
    return false;
  }

  @Override
  public boolean afterCommit() {
    catchupCoordinator.checkShouldWaitForCatchup();
    return false;
  }

  @Override
  public void onNewAssignment() {
  }

  @Override
  public void afterFirstPoll() {
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
