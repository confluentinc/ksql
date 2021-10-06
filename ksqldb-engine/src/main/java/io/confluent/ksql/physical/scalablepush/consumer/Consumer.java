package io.confluent.ksql.physical.scalablepush.consumer;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.confluent.ksql.GenericKey;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.Window;
import io.confluent.ksql.physical.common.QueryRow;
import io.confluent.ksql.physical.common.QueryRowImpl;
import io.confluent.ksql.physical.scalablepush.ProcessingQueue;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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
import org.apache.kafka.streams.kstream.Windowed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class Consumer implements AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(Consumer.class);
  private static final Duration POLL_TIMEOUT = Duration.ofMillis(30000L);

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
    }
  }

  protected void resetCurrentPosition() {
    for (int i = 0; i < partitions; i++) {
      currentPositions.put(new TopicPartition(topicName, i), 0L);
    }
    for (TopicPartition tp : topicPartitions.get()) {
      currentPositions.put(tp, consumer.position(tp));
    }
    System.out.println("RESETTING CURRENT POS TO BE " + currentPositions);
  }

  public void run() {
    System.out.println("foo " + currentPositions);
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
      System.out.println("Got NUM RECORDS " + records.count());
      if (records.isEmpty()) {
        updateCurrentPositions();
        onEmptyRecords();
        continue;
      }

      if (newAssignment) {
        System.out.println("GOT A NEW ASSIGNMENT THIS ROUND");
        newAssignment = false;
        onNewAssignment();
      }

      for (ConsumerRecord<?, GenericRow> rec : records) {
        System.out.println("Got record " + rec);
        System.out.println("KEY " + rec.key());
        System.out.println("VALUE " + rec.value());
        handleRow(rec.key(), rec.value(), rec.timestamp());
      }

      updateCurrentPositions();
      try {
        consumer.commitSync();
        Map<TopicPartition, OffsetAndMetadata> offsets = consumer.committed(new HashSet<>(topicPartitions.get()));
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

  @SuppressWarnings("unchecked")
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
        final QueryRow row;
        if (!windowed) {
          final GenericKey keyCopy = GenericKey.fromList(
              key != null ? ((GenericKey) key).values() : Collections.emptyList());
          final GenericRow valueCopy = GenericRow.fromList(value.values());
          row = QueryRowImpl.of(logicalSchema, keyCopy, Optional.empty(), valueCopy, timestamp);
        } else {
          final Windowed<GenericKey> windowedKey = (Windowed<GenericKey>) key;
          final GenericKey keyCopy = GenericKey.fromList(windowedKey.key().values());
          final GenericRow valueCopy = GenericRow.fromList(value.values());
          row = QueryRowImpl.of(logicalSchema, keyCopy, Optional.of(Window.of(
              windowedKey.window().startTime(),
              windowedKey.window().endTime()
          )), valueCopy, timestamp);
        }
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