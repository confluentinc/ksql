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
import io.confluent.ksql.physical.common.OffsetsRow;
import io.confluent.ksql.physical.common.QueryRow;
import io.confluent.ksql.physical.scalablepush.ProcessingQueue;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.util.PushOffsetRange;
import io.confluent.ksql.util.PushOffsetVector;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class ScalablePushConsumer implements AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(ScalablePushConsumer.class);
  private static final Duration POLL_TIMEOUT = Duration.ofMillis(5000L);

  protected final String topicName;
  protected final boolean windowed;
  protected final LogicalSchema logicalSchema;
  protected final KafkaConsumer<Object, GenericRow> consumer;
  protected final Clock clock;
  protected int partitions;
  protected boolean started = false;
  protected Map<TopicPartition, Long> currentPositions = new HashMap<>();
  protected volatile boolean newAssignment = false;
  protected final ConcurrentHashMap<QueryId, ProcessingQueue> processingQueues
      = new ConcurrentHashMap<>();
  private volatile boolean closed = false;
  private AtomicLong numRowsReceived = new AtomicLong(0);

  protected AtomicReference<Set<TopicPartition>> topicPartitions = new AtomicReference<>();
  private final AtomicReference<Map<TopicPartition, OffsetAndMetadata>> latestCommittedOffsets
      = new AtomicReference<>(null);

  public ScalablePushConsumer(
      final String topicName,
      final boolean windowed,
      final LogicalSchema logicalSchema,
      final KafkaConsumer<Object, GenericRow> consumer,
      final Clock clock
  ) {
    this.topicName = topicName;
    this.windowed = windowed;
    this.logicalSchema = logicalSchema;
    this.consumer = consumer;
    this.clock = clock;
  }


  protected abstract boolean onEmptyRecords();

  protected abstract boolean afterCommit();

  protected abstract void onNewAssignment();

  protected abstract void subscribeOrAssign();

  protected void afterOfferedRow(final ProcessingQueue queue) {
  }

  public void newAssignment(final Collection<TopicPartition> tps) {
    newAssignment = true;
    topicPartitions.set(tps != null ? ImmutableSet.copyOf(tps) : null);
  }

  protected void resetCurrentPosition() {
    resetCurrentPosition(Optional.empty());
  }

  protected void resetCurrentPosition(final Optional<Map<Integer, Long>> startingOffsets) {
    for (int i = 0; i < partitions; i++) {
      currentPositions.put(new TopicPartition(topicName, i), -1L);
    }
    System.out.println("Consumer got assignment " + topicPartitions.get()
        + " about to get positions");
    for (TopicPartition tp : topicPartitions.get()) {
      currentPositions.put(tp, startingOffsets
          .map(offsets -> offsets.get(tp.partition()))
          .orElse(consumer.position(tp)));
    }
    System.out.println("Consumer got assignment " + topicPartitions.get()
        + " and current position " + currentPositions);
    LOG.info("Consumer got assignment {} and current position {}", topicPartitions.get(),
        currentPositions);
  }

  private void initialize() {
    partitions = consumer.partitionsFor(topicName).size();
    LOG.info("Found {} partitions for {}", partitions, topicName);
  }

  public void run() {
    if (started) {
      LOG.error("Already ran consumer");
      throw new IllegalStateException("Already ran consumer");
    }
    started = true;
    try {
      initialize();
      subscribeOrAssign();
      while (!closed) {
        final ConsumerRecords<?, GenericRow> records = consumer.poll(POLL_TIMEOUT);
        // No assignment yet
        if (this.topicPartitions.get() == null) {
          continue;
        }

        if (newAssignment) {
          newAssignment = false;
          onNewAssignment();
        }

        final PushOffsetVector startOffsetVector
            = getOffsetVector(currentPositions, topicName, partitions);
        if (records.isEmpty()) {
          resetCurrentPosition();
          computeProgressToken(Optional.of(startOffsetVector));
          onEmptyRecords();
          continue;
        }

        for (ConsumerRecord<?, GenericRow> rec : records) {
          handleRow(rec.key(), rec.value(), rec.timestamp());
        }

        resetCurrentPosition();
        computeProgressToken(Optional.of(startOffsetVector));
        try {
          consumer.commitSync();
          updateCommittedOffsets();
        } catch (CommitFailedException e) {
          LOG.warn("Failed to commit, likely due to rebalance.  Will wait for new assignment", e);
        }

        afterCommit();
      }
    } catch (WakeupException e) {
      // This is expected when we get closed.
    }
  }

  private void updateCommittedOffsets() {
    final Map<TopicPartition, OffsetAndMetadata> offsets
        = consumer.committed(new HashSet<>(topicPartitions.get()));
    latestCommittedOffsets.set(ImmutableMap.copyOf(offsets));
  }

  private void computeProgressToken(
      final Optional<PushOffsetVector> givenStartOffsetVector
  ) {
    System.out.println("END TOKEN " + currentPositions);
    final PushOffsetVector endOffsetVector
        = getOffsetVector(currentPositions, topicName, partitions);
    final PushOffsetVector startOffsetVector = givenStartOffsetVector.orElse(endOffsetVector);

    System.out.println("Sending tokens start " + startOffsetVector + " end " + endOffsetVector);
    handleProgressToken(startOffsetVector, endOffsetVector);
  }

  private static PushOffsetVector getOffsetVector(
      final Map<TopicPartition, Long> offsets,
      final String topic,
      final int numPartitions
  ) {
    final List<Long> offsetList = new ArrayList<>();
    for (int i = 0; i < numPartitions; i++) {
      final TopicPartition tp = new TopicPartition(topic, i);
      offsetList.add(offsets.getOrDefault(tp, -1L));
    }
    return new PushOffsetVector(offsetList);
  }

  private void handleProgressToken(
      final PushOffsetVector startOffsetVector,
      final PushOffsetVector endOffsetVector) {
    final PushOffsetRange range = new PushOffsetRange(
        Optional.of(startOffsetVector), endOffsetVector);
    for (ProcessingQueue queue : processingQueues.values()) {
      System.out.println("Consumer: " + consumer.assignment() + " range" + range);
      final QueryRow row = OffsetsRow.of(clock.millis(), range);
      queue.offer(row);
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
        System.out.println("Consumer: " + consumer.assignment()
            + " Sending down row key " + key + " value " + value);
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

  /**
   * Closes everything immediately, may block.
   */
  public void close() {
    try {
      closeAsync();
    } catch (final Throwable t) {
      LOG.error("Error closing consumer", t);
    }
    try {
      consumer.close();
    } catch (final Throwable t) {
      LOG.error("Error closing kafka consumer", t);
    }
  }

  /**
   * Closes async, avoiding blocking the caller.
   */
  public void closeAsync() {
    closed = true;
    for (final ProcessingQueue processingQueue : processingQueues.values()) {
      processingQueue.close();
    }
    consumer.wakeup();
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
    final Map<TopicPartition, OffsetAndMetadata> offsets = latestCommittedOffsets.get();
    return offsets == null ? Collections.emptyMap() : offsets;
  }

  public Map<TopicPartition, Long> getCurrentOffsets() {
    return ImmutableMap.copyOf(currentPositions);
  }

  public PushOffsetVector getCurrentToken() {
    return getOffsetVector(currentPositions, topicName, partitions);
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
