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

package io.confluent.ksql.physical.scalablepush.consumer;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.physical.common.QueryRow;
import io.confluent.ksql.physical.scalablepush.ProcessingQueue;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class Consumer implements AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(Consumer.class);
  private static final Duration POLL_TIMEOUT = Duration.ofMillis(5000L);

  protected final int partitions;
  protected final String topicName;
  protected final boolean windowed;
  protected final LogicalSchema logicalSchema;
  protected KafkaConsumer<Object, GenericRow> consumer;
  protected Map<TopicPartition, Long> currentPositions = new HashMap<>();
  protected volatile boolean newAssignment = false;
  protected final ConcurrentHashMap<QueryId, ProcessingQueue> processingQueues
      = new ConcurrentHashMap<>();
  private volatile boolean closed = false;
  private AtomicLong numRowsReceived = new AtomicLong(0);

  protected AtomicReference<Set<TopicPartition>> topicPartitions = new AtomicReference<>();
  private final AtomicReference<Map<TopicPartition, OffsetAndMetadata>> latestCommittedOffsets
      = new AtomicReference<>(null);

  public Consumer(
      final int partitions,
      final String topicName,
      final boolean windowed,
      final LogicalSchema logicalSchema,
      final KafkaConsumer<Object, GenericRow> consumer
  ) {
    this.partitions = partitions;
    this.topicName = topicName;
    this.windowed = windowed;
    this.logicalSchema = logicalSchema;
    this.consumer = consumer;
  }


  protected abstract void onEmptyRecords();

  protected abstract void afterCommit();

  protected abstract void onNewAssignment();

  protected void initialize() {
  }

  protected void afterOfferedRow(final ProcessingQueue queue) {
  }

  public void newAssignment(final Collection<TopicPartition> tps) {
    newAssignment = true;
    topicPartitions.set(tps != null ? ImmutableSet.copyOf(tps) : null);
    // This must be called from the new assignment callback in order to ensure the accuracy of the
    // calls to position() giving us the right offsets before the first rows are returned after
    // an assignment.
    if (tps != null) {
      resetCurrentPosition();

      LOG.info("Consumer got assignment {} and current position {}", tps, currentPositions);
    }
  }

  protected void resetCurrentPosition() {
    for (int i = 0; i < partitions; i++) {
      currentPositions.put(new TopicPartition(topicName, i), 0L);
    }
    for (TopicPartition tp : topicPartitions.get()) {
      currentPositions.put(tp, consumer.position(tp));
    }
  }

  public void run() {
    initialize();
    while (true) {
      if (closed) {
        return;
      }
      final ConsumerRecords<?, GenericRow> records = consumer.poll(POLL_TIMEOUT);
      // No assignment yet
      if (this.topicPartitions.get() == null) {
        continue;
      }
      if (records.isEmpty()) {
        updateCurrentPositions();
        onEmptyRecords();
        continue;
      }

      if (newAssignment) {
        newAssignment = false;
        onNewAssignment();
      }

      for (ConsumerRecord<?, GenericRow> rec : records) {
        handleRow(rec.key(), rec.value(), rec.timestamp());
      }

      updateCurrentPositions();
      try {
        consumer.commitSync();
        final Map<TopicPartition, OffsetAndMetadata> offsets
            = consumer.committed(new HashSet<>(topicPartitions.get()));
        latestCommittedOffsets.set(ImmutableMap.copyOf(offsets));
      } catch (CommitFailedException e) {
        LOG.warn("Failed to commit, likely due to rebalance.  Will wait for new assignment", e);
      }

      afterCommit();
    }
  }

  private void updateCurrentPositions() {
    for (TopicPartition tp : topicPartitions.get()) {
      currentPositions.put(tp, consumer.position(tp));
    }
  }

  private boolean handleRow(final Object key, final GenericRow value, final long timestamp) {
    // We don't currently handle null in either field
    if ((key == null && !logicalSchema.key().isEmpty()) || value == null) {
      return false;
    }
    numRowsReceived.incrementAndGet();
    for (ProcessingQueue queue : processingQueues.values()) {
      try {
        // The physical operators may modify the keys and values, so we make a copy to ensure
        // that there's no cross-query interference.
        final QueryRow row = RowUtil.createRow(key, value, timestamp, windowed, logicalSchema);
        queue.offer(row);
        afterOfferedRow(queue);
      } catch (final Throwable t) {
        LOG.error("Error while offering row", t);
      }
    }
    return false;
  }

  public void close() {
    for (final ProcessingQueue processingQueue : processingQueues.values()) {
      processingQueue.close();
    }
    closed = true;
  }

  public boolean isClosed() {
    return closed;
  }

  public void register(final ProcessingQueue processingQueue) {
    processingQueues.put(processingQueue.getQueryId(), processingQueue);
  }

  public void unregister(final ProcessingQueue processingQueue) {
    processingQueues.remove(processingQueue.getQueryId());
  }

  public Set<TopicPartition> getAssignment() {
    return topicPartitions.get();
  }

  public Map<TopicPartition, OffsetAndMetadata> getCommittedOffsets() {
    return ImmutableMap.copyOf(latestCommittedOffsets.get());
  }

  public Map<TopicPartition, Long> getCurrentOffsets() {
    return ImmutableMap.copyOf(currentPositions);
  }

  public long getNumRowsReceived() {
    return numRowsReceived.get();
  }

  public int numRegistered() {
    return processingQueues.size();
  }

  @VisibleForTesting
  public List<ProcessingQueue> processingQueues() {
    return ImmutableList.copyOf(processingQueues.values());
  }

  public void onError() {
    for (ProcessingQueue queue : processingQueues.values()) {
      queue.onError();
    }
  }
}