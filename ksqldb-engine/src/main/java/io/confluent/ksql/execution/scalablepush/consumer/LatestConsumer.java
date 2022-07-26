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

import com.google.common.collect.ImmutableList;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
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

public class LatestConsumer extends ScalablePushConsumer {

  private static final Logger LOG = LoggerFactory.getLogger(LatestConsumer.class);

  private final CatchupCoordinator catchupCoordinator;
  private final java.util.function.Consumer<Collection<TopicPartition>> catchupAssignmentUpdater;
  private final KsqlConfig ksqlConfig;
  private boolean gotFirstAssignment = false;

  @SuppressFBWarnings(value = "EI_EXPOSE_REP2")
  public LatestConsumer(
      final String topicName,
      final boolean windowed,
      final LogicalSchema logicalSchema,
      final KafkaConsumer<Object, GenericRow> consumer,
      final CatchupCoordinator catchupCoordinator,
      final java.util.function.Consumer<Collection<TopicPartition>> catchupAssignmentUpdater,
      final KsqlConfig ksqlConfig,
      final Clock clock) {
    super(topicName, windowed, logicalSchema, consumer, clock);
    this.catchupCoordinator = catchupCoordinator;
    this.catchupAssignmentUpdater = catchupAssignmentUpdater;
    this.ksqlConfig = ksqlConfig;
  }

  public interface LatestConsumerFactory {
    LatestConsumer create(
        String topicName,
        boolean windowed,
        LogicalSchema logicalSchema,
        KafkaConsumer<Object, GenericRow> consumer,
        CatchupCoordinator catchupCoordinator,
        java.util.function.Consumer<Collection<TopicPartition>> catchupAssignmentUpdater,
        KsqlConfig ksqlConfig,
        Clock clock
    );
  }

  @Override
  protected void subscribeOrAssign() {
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
          public void onPartitionsRevoked(final Collection<TopicPartition> collection) {
            LOG.info("Latest consumer had partitions revoked {}", collection);
            newAssignment(null);
          }

          @Override
          public void onPartitionsAssigned(final Collection<TopicPartition> collection) {
            LOG.info("Latest consumer had partitions assigned {}", collection);
            if (collection == null) {
              return;
            }
            newAssignment(collection);
            updateCurrentPositions();
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
  protected void afterBatchProcessed() {
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
    final long maxAge = ksqlConfig.getLong(KsqlConfig.KSQL_QUERY_PUSH_V2_LATEST_RESET_AGE_MS);
    final long timeMs = clock.millis() - maxAge;
    final HashMap<TopicPartition, Long> timestamps = new HashMap<>();
    for (TopicPartition tp : topicPartitions) {
      timestamps.put(tp, timeMs);
    }
    final Map<TopicPartition, OffsetAndTimestamp> offsetAndTimestampMap =
        consumer.offsetsForTimes(timestamps);
    final Map<TopicPartition, OffsetAndMetadata> offsetAndMetadataMap =
        consumer.committed(topicPartitions);
    LOG.info("Latest maybe seeking to end offsetAndTimestampMap {}, offsetAndMetadataMap {}",
        offsetAndTimestampMap, offsetAndMetadataMap);
    // If even one partition has recent commits, then we don't seek to end.
    boolean foundAtLeastOneRecent = false;
    for (Map.Entry<TopicPartition, OffsetAndTimestamp> entry : offsetAndTimestampMap.entrySet()) {
      final OffsetAndMetadata metadata = offsetAndMetadataMap.get(entry.getKey());
      if (metadata != null && entry.getValue() != null
          && entry.getValue().offset() <= metadata.offset()) {
        foundAtLeastOneRecent = true;
      }
    }
    if (!foundAtLeastOneRecent) {
      consumer.seekToEnd(topicPartitions);
      updateCurrentPositions();
      LOG.info("LatestConsumer seeking to end {}", currentPositions);
    }
  }
}
