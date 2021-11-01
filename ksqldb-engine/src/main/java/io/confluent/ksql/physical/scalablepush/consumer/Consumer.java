package io.confluent.ksql.physical.scalablepush.consumer;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.confluent.ksql.GenericKey;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.Window;
import io.confluent.ksql.physical.common.OffsetsRow;
import io.confluent.ksql.physical.common.QueryRow;
import io.confluent.ksql.physical.common.QueryRowImpl;
import io.confluent.ksql.physical.scalablepush.ProcessingQueue;
import io.confluent.ksql.physical.scalablepush.TokenUtils;
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

  // Used in testing
  private final AtomicReference<Set<PartitionOffset>> droppedMessages = new AtomicReference<>(null);

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

  protected void afterOfferedRow(final ProcessingQueue queue) {
  }

  public void newAssignment(final Collection<TopicPartition> tps) {
    newAssignment = true;
    topicPartitions.set(tps != null ? ImmutableSet.copyOf(tps) : null);
    // This must be called from the new assignment callback in order to ensure the accuracy of the
    // calls to position() giving us the right offsets before the first rows are returned after
    // an assignment.
    if (tps != null) {
      resetCurrentPosition(consumer, currentPositions);
    }
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
    System.out.println("RESETTING CURRENT POS TO BE " + currentPositions);
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
      System.out.println("Got NUM RECORDS " + records.count());
      if (firstPoll) {
        firstPoll = false;
        afterFirstPoll();
      }
      if (records.isEmpty()) {
        computeProgressToken(Optional.empty(), false);
        onEmptyRecords();
        continue;
      }

      if (newAssignment) {
        System.out.println("GOT A NEW ASSIGNMENT THIS ROUND");
        newAssignment = false;
//        resetCurrentPosition(consumer, currentPositions);
        onNewAssignment();
      }

      boolean dropped = false;
      final Set<PartitionOffset> droppedPartitionOffsets = droppedMessages.get();

      for (ConsumerRecord<GenericKey, GenericRow> rec : records) {
        System.out.println("Got record " + rec);
        System.out.println("KEY " + rec.key());
        System.out.println("VALUE " + rec.value());
        final PartitionOffset partitionOffset = new PartitionOffset(rec.partition(), rec.offset());
        if ((droppedPartitionOffsets != null
            && droppedPartitionOffsets.contains(partitionOffset))) {
          droppedPartitionOffsets.remove(partitionOffset);
          dropped = true;
          System.out.println("Simulating dropped row for " + partitionOffset);
        } else {
          handleRow(rec.key(), rec.value(), rec.timestamp(),
              TokenUtils.getToken(currentPositions, rec.topic(), partitions));
        }
      }

      List<Long> startToken = TokenUtils.getToken(currentPositions, topicName, partitions);
      System.out.println("START TOKEN " + currentPositions);
      computeProgressToken(Optional.of(startToken), dropped);
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

  private void computeProgressToken(
      final Optional<List<Long>> givenStartToken,
      final boolean dropped
  ) {
    for (TopicPartition tp : topicPartitions.get()) {
      currentPositions.put(tp, consumer.position(tp));
    }

    System.out.println("END TOKEN " + currentPositions);
    List<Long> endToken = TokenUtils.getToken(currentPositions, topicName, partitions);
    List<Long> startToken = givenStartToken.orElse(endToken);

    if (!dropped) {
      System.out.println("Sending tokens start " + startToken + " end " + endToken);
      handleProgressToken(startToken, endToken);
    }
  }

  private void handleProgressToken(
      final List<Long> startToken,
      final List<Long> endToken) {
    for (ProcessingQueue queue : processingQueues.values()) {
//      final QueryRow row = OffsetsRow.of(
//          System.currentTimeMillis(), Optional.of(startToken), endToken);
//      queue.offer(row);
    }
  }

  @SuppressWarnings("unchecked")
  private boolean handleRow(
      final Object key, final GenericRow value, final long timestamp, final List<Long> token) {
    // We don't currently handle null in either field
    if ((key == null && !logicalSchema.key().isEmpty()) || value == null) {
      return false;
    }
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
              new Windowed<>(GenericKey.fromList(windowedKey.key().values()),
                  windowedKey.window());
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

  public Map<TopicPartition, OffsetAndMetadata> getCurrentOffsets() {
    return latestOffsets.get();
  }

  public List<Long> getCurrentToken() {
    return TokenUtils.getToken(currentPositions, topicName, partitions);
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

  public void simulateDroppedMessages(final Set<PartitionOffset> droppedMessages) {
    this.droppedMessages.set(droppedMessages);
  }

  public static class PartitionOffset {

    private final int partition;
    private final long offset;

    public PartitionOffset(int partition, long offset) {
      this.partition = partition;
      this.offset = offset;
    }

    @Override
    public boolean equals(final Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      final PartitionOffset that = (PartitionOffset) o;
      return Objects.equals(partition, that.partition)
          && Objects.equals(offset, that.offset);
    }

    @Override
    public int hashCode() {
      return Objects.hash(partition, offset);
    }
  }

}