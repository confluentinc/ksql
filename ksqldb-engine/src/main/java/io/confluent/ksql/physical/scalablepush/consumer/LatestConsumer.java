package io.confluent.ksql.physical.scalablepush.consumer;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.GenericKey;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.util.KsqlConfig;
import java.time.Clock;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LatestConsumer extends Consumer {

  private static final Logger LOG = LoggerFactory.getLogger(LatestConsumer.class);
  private static final long LATEST_CONSUMER_OLDEST_COMMIT_AGE_MS = 10000;

  private final CatchupCoordinator catchupCoordinator;
  private final java.util.function.Consumer<Collection<TopicPartition>> catchupAssignmentUpdater;
  private final KsqlConfig ksqlConfig;
  private final Clock clock;
  private boolean gotFirstAssignment = false;

  public LatestConsumer(
      final int partitions,
      final String topicName,
      final boolean windowed,
      final LogicalSchema logicalSchema,
      final KafkaConsumer<Object, GenericRow> consumer,
      final CatchupCoordinator catchupCoordinator,
      final java.util.function.Consumer<Collection<TopicPartition>> catchupAssignmentUpdater,
      final KsqlConfig ksqlConfig,
      final Clock clock) {
    super(partitions, topicName, windowed, logicalSchema, consumer);
    this.catchupCoordinator = catchupCoordinator;
    this.catchupAssignmentUpdater = catchupAssignmentUpdater;
    this.ksqlConfig = ksqlConfig;
    this.clock = clock;
  }

  public static LatestConsumer create(
      final int partitions,
      final String topicName,
      final boolean windowed,
      final LogicalSchema logicalSchema,
      final KafkaConsumer<Object, GenericRow> consumer,
      final CatchupCoordinator catchupCoordinator,
      final java.util.function.Consumer<Collection<TopicPartition>> catchupAssignmentUpdater,
      final KsqlConfig ksqlConfig,
      final Clock clock
  ) {
    return new LatestConsumer(partitions, topicName, windowed, logicalSchema, consumer,
        catchupCoordinator, catchupAssignmentUpdater, ksqlConfig, clock);
  }

  public interface LatestConsumerFactory {
    LatestConsumer create(
        final int partitions,
        final String topicName,
        final boolean windowed,
        final LogicalSchema logicalSchema,
        final KafkaConsumer<Object, GenericRow> consumer,
        final CatchupCoordinator catchupCoordinator,
        final java.util.function.Consumer<Collection<TopicPartition>> catchupAssignmentUpdater,
        final KsqlConfig ksqlConfig,
        final Clock clock
    );
  }

  @Override
  protected void initialize() {
    // Initial wait time, giving client connections a chance to be made to avoid having to do
    // any catchups.
    try {
      Thread.sleep(ksqlConfig.getLong(KsqlConfig.KSQL_QUERY_PUSH_V2_NEW_LATEST_DELAY_MS));
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Got interrupted", e);
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
            if (collection == null) {
              return;
            }
            newAssignment(collection);
            catchupAssignmentUpdater.accept(collection);
            if (!gotFirstAssignment) {
              maybeSeekToEnd();
            }
            gotFirstAssignment = true;
          }
        });
  }

  @Override
  protected void onEmptyRecords() {
    catchupCoordinator.checkShouldWaitForCatchup();
  }

  @Override
  protected void afterCommit() {
    catchupCoordinator.checkShouldWaitForCatchup();
  }

  @Override
  protected void onNewAssignment() {
  }

  /**
   * We use the same consumer group id so that we're ensured that all nodes in the cluster are
   * part of the same group -- otherwise, we'd have to use some consensus mechanism to ensure that
   * they all agreed on the same new group id. With the same group, we periodically have to seek to
   * the end if there have been no readers recently so that the user doesn't get a deluge of
   * historical values.
   */
  private void maybeSeekToEnd() {
    final Set<TopicPartition> topicPartitions = this.topicPartitions.get();
    long timeMs = clock.millis() - LATEST_CONSUMER_OLDEST_COMMIT_AGE_MS;
    HashMap<TopicPartition, Long> timestamps = new HashMap<>();
    for (TopicPartition tp : topicPartitions) {
      timestamps.put(tp, timeMs);
    }
    Map<TopicPartition, OffsetAndTimestamp> offsetAndTimestampMap =
        consumer.offsetsForTimes(timestamps);
    Map<TopicPartition, OffsetAndMetadata> offsetAndMetadataMap =
        consumer.committed(topicPartitions);
    for (Map.Entry<TopicPartition, OffsetAndTimestamp> entry : offsetAndTimestampMap.entrySet()) {
      OffsetAndMetadata metadata = offsetAndMetadataMap.get(entry.getKey());
      if (metadata != null && entry.getValue() != null
          && entry.getValue().offset() > metadata.offset()) {
        consumer.seekToEnd(topicPartitions);
        resetCurrentPosition();
        return;
      }
    }
  }
}
