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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.common.OffsetsRow;
import io.confluent.ksql.execution.common.QueryRow;
import io.confluent.ksql.execution.scalablepush.ProcessingQueue;
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
  protected AtomicReference<Map<TopicPartition, Long>> currentPositions
      = new AtomicReference<>(Collections.emptyMap());
  protected volatile boolean newAssignment = false;
  protected final ConcurrentHashMap<QueryId, ProcessingQueue> processingQueues
      = new ConcurrentHashMap<>();
  private volatile boolean closed = false;
  private AtomicLong numRowsReceived = new AtomicLong(0);

  protected AtomicReference<Set<TopicPartition>> topicPartitions = new AtomicReference<>();

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


  protected abstract void onEmptyRecords();

  protected abstract void afterBatchProcessed();

  protected abstract void onNewAssignment();

  protected abstract void subscribeOrAssign();

  protected void afterOfferedRow(final ProcessingQueue queue) {
  }

  // Called when there is a new assignment.  Notifies this object, so that any subclass waiting
  // for a new assignment can be woken up.
  public synchronized void newAssignment(final Collection<TopicPartition> tps) {
    newAssignment = true;
    topicPartitions.set(tps != null ? ImmutableSet.copyOf(tps) : null);
    notify();
  }

  protected void updateCurrentPositions() {
    updateCurrentPositions(Optional.empty());
  }

  protected void updateCurrentPositions(final Optional<Map<Integer, Long>> startingOffsets) {
    final HashMap<TopicPartition, Long> updatedCurrentPositions = new HashMap<>();
    for (int i = 0; i < partitions; i++) {
      updatedCurrentPositions.put(new TopicPartition(topicName, i), -1L);
    }
    for (TopicPartition tp : topicPartitions.get()) {
      updatedCurrentPositions.put(tp, startingOffsets
          .map(offsets -> offsets.get(tp.partition()))
          .orElse(consumer.position(tp)));
    }
    LOG.debug("Consumer has assignment {} and current position {}", topicPartitions,
        updatedCurrentPositions);
    currentPositions.set(ImmutableMap.copyOf(updatedCurrentPositions));
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
            = getOffsetVector(currentPositions.get(), topicName, partitions);
        if (records.isEmpty()) {
          updateCurrentPositions();
          computeProgressToken(Optional.of(startOffsetVector));
          onEmptyRecords();
          continue;
        }

        for (ConsumerRecord<?, GenericRow> rec : records) {
          handleRow(rec.key(), rec.value(), rec.timestamp());
        }

        updateCurrentPositions();
        computeProgressToken(Optional.of(startOffsetVector));
        try {
          consumer.commitSync();
        } catch (CommitFailedException e) {
          LOG.warn("Failed to commit, likely due to rebalance.  Will wait for new assignment", e);
        }

        afterBatchProcessed();
      }
    } catch (WakeupException e) {
      // This is expected when we get closed.
    }
  }

  private void computeProgressToken(
      final Optional<PushOffsetVector> givenStartOffsetVector
  ) {
    final PushOffsetVector endOffsetVector
        = getOffsetVector(currentPositions.get(), topicName, partitions);
    final PushOffsetVector startOffsetVector = givenStartOffsetVector.orElse(endOffsetVector);

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
   * Closes async, avoiding blocking the caller. Can be closed by another thread.
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

  public Map<TopicPartition, Long> getCurrentOffsets() {
    return currentPositions.get();
  }

  public PushOffsetVector getCurrentToken() {
    return getOffsetVector(currentPositions.get(), topicName, partitions);
  }

  public long getNumRowsReceived() {
    return numRowsReceived.get();
  }

  public int numRegistered() {
    return processingQueues.size();
  }

  public void onError() {
    for (ProcessingQueue queue : processingQueues.values()) {
      queue.onError();
    }
  }
}
