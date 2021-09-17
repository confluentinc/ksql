package io.confluent.ksql.physical.scalablepush.consumer;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.confluent.ksql.GenericKey;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.streams.materialization.Row;
import io.confluent.ksql.execution.streams.materialization.TableRow;
import io.confluent.ksql.execution.streams.materialization.WindowedRow;
import io.confluent.ksql.physical.scalablepush.ProcessingQueue;
import io.confluent.ksql.physical.scalablepush.TokenUtils;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.rest.entity.ProgressToken;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.TimeWindow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class Consumer implements AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(Consumer.class);
  private static final LogicalSchema EMPTY_SCHEMA = LogicalSchema.builder().build();

  protected final int partitions;
  protected final String topicName;
  protected final boolean windowed;
  protected final LogicalSchema logicalSchema;
  protected KafkaConsumer<GenericKey, GenericRow> consumer;
  protected Map<TopicPartition, Long> currentPositions = new HashMap<>();
  protected volatile boolean newAssignment = false;
  protected final ConcurrentHashMap<QueryId, ProcessingQueue> processingQueues
      = new ConcurrentHashMap<>();
  private volatile boolean closed = false;
  private boolean firstPoll = true;

  protected AtomicReference<Set<TopicPartition>> topicPartitions = new AtomicReference<>();
  private final AtomicReference<Map<TopicPartition, OffsetAndMetadata>> latestOffsets
      = new AtomicReference<>(null);

  public Consumer(
      final int partitions,
      final String topicName,
      final boolean windowed,
      final LogicalSchema logicalSchema,
      final KafkaConsumer<GenericKey, GenericRow> consumer
  ) {
    this.partitions = partitions;
    this.topicName = topicName;
    this.windowed = windowed;
    this.logicalSchema = logicalSchema;
    this.consumer = consumer;
  }


  protected abstract boolean onEmptyRecords();

  protected abstract boolean afterCommit();

  protected abstract void onNewAssignment();

  protected abstract void afterFirstPoll();

  protected void initialize() {
  }

  public void newAssignment(final Collection<TopicPartition> tps) {
    newAssignment = true;
    topicPartitions.set(ImmutableSet.copyOf(tps));
  }

  private void resetCurrentPosition(
      KafkaConsumer<GenericKey, GenericRow> consumer,
      Map<TopicPartition, Long> currentPositions
  ) {
    for (int i = 0; i < partitions; i++) {
      currentPositions.put(new TopicPartition(topicName, i), 0L);
    }
    for (TopicPartition tp : topicPartitions.get()) {
      currentPositions.put(tp, consumer.position(tp));
    }
  }

  public void run() {
    System.out.println("foo " + currentPositions);
    initialize();
    while (true) {
      if (closed) {
        return;
      }
      final ConsumerRecords<GenericKey, GenericRow> records = consumer.poll(Duration.ofMillis(1000));
      // No assignment yet
      if (this.topicPartitions.get() == null) {
        continue;
      }
      if (firstPoll) {
        firstPoll = false;
        afterFirstPoll();
      }
      if (records.isEmpty()) {
        onEmptyRecords();
        continue;
      }

      if (newAssignment) {
        newAssignment = false;
        resetCurrentPosition(consumer, currentPositions);
        onNewAssignment();
      }

      String startToken = TokenUtils.getToken(currentPositions, topicName, partitions);

      for (ConsumerRecord<GenericKey, GenericRow> rec : records) {
        System.out.println("Got record " + rec);
        System.out.println("KEY " + rec.key());
        System.out.println("VALUE " + rec.value());
        handleRow(processingQueues, rec.key(), rec.value(), rec.timestamp(),
            TokenUtils.getToken(currentPositions, rec.topic(), partitions));
      }
      for (TopicPartition tp : topicPartitions.get()) {
        currentPositions.put(tp, consumer.position(tp));
      }

      String endToken = TokenUtils.getToken(currentPositions, topicName, partitions);

      System.out.println("Sending tokens start " + startToken + " end " + endToken);
      handleProgressToken(processingQueues, startToken, endToken);
      try {
        consumer.commitSync();
        Map<TopicPartition, OffsetAndMetadata> offsets = consumer.committed(new HashSet<>(topicPartitions.get()));
        latestOffsets.set(ImmutableMap.copyOf(offsets));
      } catch (CommitFailedException e) {
        LOG.warn("Failed to commit, likely due to rebalance.  Will wait for new assignment", e);
      }

      afterCommit();
    }
  }

  private void handleProgressToken(
      final Map<QueryId, ProcessingQueue> processingQueues,
      final String startToken,
      final String endToken) {
    for (ProcessingQueue queue : processingQueues.values()) {
      final TableRow row;
      if (!windowed) {
        row = Row.of(EMPTY_SCHEMA, GenericKey.genericKey(), GenericRow.genericRow(),
            System.currentTimeMillis(), Optional.of(new ProgressToken(startToken, endToken)));
      } else {
        row = WindowedRow.of(EMPTY_SCHEMA, new Windowed<>(GenericKey.genericKey(),
                new TimeWindow(0, 0)), GenericRow.genericRow(),
            System.currentTimeMillis());
      }
      queue.offer(row);
    }
  }

  @SuppressWarnings("unchecked")
  private boolean handleRow(
      final Map<QueryId, ProcessingQueue> processingQueues,
      final Object key, final GenericRow value, final long timestamp, final String token) {
    // We don't currently handle null in either field
    if ((key == null && !logicalSchema.key().isEmpty()) || value == null) {
      return false;
    }
    for (ProcessingQueue queue : processingQueues.values()) {
      try {
        // The physical operators may modify the keys and values, so we make a copy to ensure
        // that there's no cross-query interference.
        final TableRow row;
        if (!windowed) {
          final GenericKey keyCopy = GenericKey.fromList(
              key != null ? ((GenericKey) key).values() : Collections.emptyList());
          final GenericRow valueCopy = GenericRow.fromList(value.values());
          row = Row.of(logicalSchema, keyCopy, valueCopy, timestamp, Optional.empty());
        } else {
          final Windowed<GenericKey> windowedKey = (Windowed<GenericKey>) key;
          final Windowed<GenericKey> keyCopy =
              new Windowed<>(GenericKey.fromList(windowedKey.key().values()),
                  windowedKey.window());
          final GenericRow valueCopy = GenericRow.fromList(value.values());
          row = WindowedRow.of(logicalSchema, keyCopy, valueCopy, timestamp);
        }
        return queue.offer(row);
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

  public Map<TopicPartition, OffsetAndMetadata> getCurrentOffsets() {
    return latestOffsets.get();
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