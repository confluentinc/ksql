/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.ksql.rest.server.computation;

import io.confluent.ksql.parser.tree.Statement;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Wrapper class for the command topic. Used for reading from the topic (either all messages from the beginning until
 * now, or any new messages since then), and writing to it.
 */
public class CommandStore implements Closeable {

  private static final Logger log = LoggerFactory.getLogger(CommandStore.class);

  private final String commandTopic;
  private final Consumer<CommandId, String> commandConsumer;
  private final Producer<CommandId, String> commandProducer;
  private final CommandIdAssigner commandIdAssigner;
  private final AtomicBoolean closed;

  public CommandStore(
      String commandTopic,
      Consumer<CommandId, String> commandConsumer,
      Producer<CommandId, String> commandProducer,
      CommandIdAssigner commandIdAssigner
  ) {
    this.commandTopic = commandTopic;
    // TODO: Remove commandConsumer/commandProducer as parameters if not needed in testing
    this.commandConsumer = commandConsumer;
    this.commandProducer = commandProducer;
    this.commandIdAssigner = commandIdAssigner;

    commandConsumer.assign(Collections.singleton(new TopicPartition(commandTopic, 0)));

    closed = new AtomicBoolean(false);
  }

  /**
   * Close the store, rendering it unable to read or write commands
   */
  @Override
  public void close() {
    closed.set(true);
    commandConsumer.wakeup();
    commandProducer.close();
  }

  /**
   * Write the given statement to the command topic, to be read by all nodes in the current cluster. Does not return
   * until the statement has been successfully written, or an exception is thrown.
   * @param statementString The string of the statement to be distributed
   * @param statement The statement to be distributed
   * @return The ID assigned to the statement
   * @throws Exception TODO: Refine this
   */
  public CommandId distributeStatement(String statementString, Statement statement) throws Exception {
    CommandId commandId = commandIdAssigner.getCommandId(statement);
    commandProducer.send(new ProducerRecord<>(commandTopic, commandId, statementString)).get();
    return commandId;
  }

  /**
   * Poll for new commands, blocking until at least one is available.
   * @return The commands that have been polled from the command topic
   */
  public ConsumerRecords<CommandId, String> getNewCommands() {
    return commandConsumer.poll(Long.MAX_VALUE);
  }

  /**
   * Collect all commands that have been written to the command topic, starting at the earliest offset and proceeding
   * until it appears that all have been returned.
   * @return The commands that have been read from the command topic
   */
  public LinkedHashMap<CommandId, String> getPriorCommands() {
    LinkedHashMap<CommandId, String> result = new LinkedHashMap<>();
    for (ConsumerRecord<CommandId, String> commandRecord : getAllPriorCommandRecords()) {
      CommandId commandId = commandRecord.key();
      String command = commandRecord.value();
      if (command != null) {
        result.put(commandId, command);
      } else {
        // Handle potential log compaction by ignoring commands with null values
        result.remove(commandId);
      }
    }

    return result;
  }

  private List<ConsumerRecord<CommandId, String>> getAllPriorCommandRecords() {
    Collection<TopicPartition> commandTopicPartitions = getTopicPartitionsForTopic(commandTopic);

    commandConsumer.poll(0); // Have to poll to make sure subscription has taken effect (subscribe() is lazy)
    commandConsumer.seekToBeginning(commandTopicPartitions);

    Map<TopicPartition, Long> currentOffsets = new HashMap<>();

    List<ConsumerRecord<CommandId, String>> result = new ArrayList<>();
    log.debug("Polling end offset(s) for command topic");
    Map<TopicPartition, Long> endOffsets = commandConsumer.endOffsets(commandTopicPartitions);
    // Only want to poll for end offsets at the very beginning, and when we think we may be caught up.
    // So, this outer loop tries to catch up (via the inner loop), then when it believes it has (signalled by having
    // exited the inner loop), end offsets are polled again and another check is performed to see if anything new has
    // been written (which would be signalled by the end offsets having changed). If something new has been written,
    // the outer loop is repeated; if not, we're caught up to the end offsets we just polled and can
    // continue.
    do {
      while (!offsetsCaughtUp(currentOffsets, endOffsets)) {
        log.debug("Polling for prior command records");
        ConsumerRecords<CommandId, String> records = commandConsumer.poll(30000);
        if (records.isEmpty()) {
          log.warn("No records received after 30 seconds of polling; something may be wrong");
        } else {
          log.debug("Received {} records from poll", records.count());
          for (ConsumerRecord<CommandId, String> record : records) {
            result.add(record);
            TopicPartition recordTopicPartition = new TopicPartition(record.topic(), record.partition());
            Long currentOffset = currentOffsets.get(recordTopicPartition);
            if (currentOffset == null || currentOffset < record.offset()) {
              currentOffsets.put(recordTopicPartition, record.offset());
            }
          }
        }
      }
      log.debug("Polling end offset(s) for command topic");
      endOffsets = commandConsumer.endOffsets(commandTopicPartitions);
    } while (!offsetsCaughtUp(currentOffsets, endOffsets));

    return result;
  }

  private Collection<TopicPartition> getTopicPartitionsForTopic(String topic) {
    List<PartitionInfo> partitionInfoList = commandConsumer.partitionsFor(topic);

    Collection<TopicPartition> result = new HashSet<>();
    for (PartitionInfo partitionInfo : partitionInfoList) {
      result.add(new TopicPartition(partitionInfo.topic(), partitionInfo.partition()));
    }

    return result;
  }

  private boolean offsetsCaughtUp(Map<TopicPartition, Long> offsets, Map<TopicPartition, Long> endOffsets) {
    log.debug("Checking to see if consumed command records are caught up with end offset(s)");
    for (Map.Entry<TopicPartition, Long> endOffset : endOffsets.entrySet()) {
      long offset = offsets.getOrDefault(endOffset.getKey(), 0L);
      /*
          From https://kafka.apache.org/0101/javadoc/index.html?org/apache/kafka/streams/kstream/KTable.html
          "The last offset of a partition is the offset of the upcoming message,
          i.e. the offset of the last available message + 1"
          Hence, "offset + 1" instead of just "offset"
       */
      if (offset + 1 < endOffset.getValue()) {
        log.debug(
            "Consumed command records are not yet caught up with offset for partition {}; end offset is {}, but last "
                + "consumed offset is {}",
            endOffset.getKey().partition(),
            endOffset.getValue(),
            offset
        );
        return false;
      }
    }
    log.debug("Consumed command records are caught up with end offset(s)");
    return true;
  }
}
