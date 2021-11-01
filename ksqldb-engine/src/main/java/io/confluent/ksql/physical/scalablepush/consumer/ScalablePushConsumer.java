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
import io.confluent.ksql.physical.scalablepush.TokenUtils;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.util.PushOffsetRange;
import io.confluent.ksql.util.PushOffsetVector;
import java.time.Duration;
import java.util.Collection;
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
      final KafkaConsumer<Object, GenericRow> consumer
  ) {
    this.topicName = topicName;
    this.windowed = windowed;
    this.logicalSchema = logicalSchema;
    this.consumer = consumer;
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

        List<Long> startToken = TokenUtils.getToken(currentPositions, topicName, partitions);
        updateCurrentPositions();
        computeProgressToken(Optional.of(startToken));
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
    } catch (WakeupException e) {
      // This is expected when we get closed.
    }
  }

  private void updateCurrentPositions() {
    for (TopicPartition tp : topicPartitions.get()) {
      currentPositions.put(tp, consumer.position(tp));
    }
  }

  private void computeProgressToken(
      final Optional<List<Long>> givenStartToken
  ) {
    System.out.println("END TOKEN " + currentPositions);
    List<Long> endToken = TokenUtils.getToken(currentPositions, topicName, partitions);
    List<Long> startToken = givenStartToken.orElse(endToken);

    System.out.println("Sending tokens start " + startToken + " end " + endToken);
    handleProgressToken(startToken, endToken);
  }

  private void handleProgressToken(
      final List<Long> startToken,
      final List<Long> endToken) {
    for (ProcessingQueue queue : processingQueues.values()) {
      PushOffsetVector offsetVectorStart = new PushOffsetVector(startToken);
      PushOffsetVector offsetVectorEnd = new PushOffsetVector(endToken);
      final QueryRow row = OffsetsRow.of(
          System.currentTimeMillis(),
          new PushOffsetRange(Optional.of(offsetVectorStart), offsetVectorEnd));
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
    return ImmutableMap.copyOf(latestCommittedOffsets.get());
  }

  public Map<TopicPartition, Long> getCurrentOffsets() {
    return ImmutableMap.copyOf(currentPositions);
  }

  public List<Long> getCurrentToken() {
    return TokenUtils.getToken(currentPositions, topicName, partitions);
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
