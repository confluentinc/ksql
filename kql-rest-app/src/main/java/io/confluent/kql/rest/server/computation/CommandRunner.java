/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.kql.rest.server.computation;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Handles the logic of polling for new queries, assigning them an ID, and then delegating their execution to a
 * {@link StatementExecutor}. Also responsible for taking care of any exceptions that occur in the process.
 */
public class CommandRunner implements Runnable, Closeable {

  private static final Logger log = LoggerFactory.getLogger(CommandRunner.class);

  private final StatementExecutor statementExecutor;
  private final String commandTopic;
  private final String nodeId;
  private final Consumer<String, String> commandConsumer;
  private final Producer<String, String> commandProducer;
  private final AtomicBoolean closed;
  private final AtomicInteger statementSuffix;

  public CommandRunner(
      StatementExecutor statementExecutor,
      String commandTopic,
      String nodeId,
      Consumer<String, String> commandConsumer,
      Producer<String, String> commandProducer
  ) {
    this.statementExecutor = statementExecutor;
    this.commandTopic = commandTopic;
    this.nodeId = nodeId;
    // TODO: Remove commandConsumer/commandProducer as parameters if not needed in testing
    this.commandConsumer = commandConsumer;
    this.commandProducer = commandProducer;

    commandConsumer.subscribe(Collections.singleton(commandTopic));

    closed = new AtomicBoolean(false);
    statementSuffix = new AtomicInteger();
  }

  /**
   * Read and execute all commands on the command topic, starting at the earliest offset.
   * @throws Exception TODO: Refine this.
   */
  public void processPriorCommands() throws Exception {
    String statementPrefix = String.format("%s_", nodeId).toUpperCase();
    int newStatementSuffix = 0;

    LinkedHashMap<String, String> priorCommands = getPriorCommands();
    statementExecutor.handleStatements(priorCommands);

    for (String statementId : priorCommands.keySet()) {
      if (statementId.startsWith(statementPrefix)) {
        int commandNumber = Integer.parseInt(statementId.substring(statementPrefix.length()));
        newStatementSuffix = Math.max(newStatementSuffix, commandNumber);
      }
    }

    statementSuffix.set(newStatementSuffix);
  }

  /**
   * Begin a continuous poll-execute loop for the command topic, stopping only if either a {@link WakeupException} is
   * thrown or the {@link #close()} method is called.
   */
  @Override
  public void run() {
    try {
      while (!closed.get()) {
        log.info("Polling for new writes to command topic");
        ConsumerRecords<String, String> records = commandConsumer.poll(Long.MAX_VALUE);
        log.info("Found {} new writes to command topic", records.count());
        for (ConsumerRecord<String, String> record : records) {
          String statementId = record.key();
          String statementStr = record.value();
          if (statementStr != null) {
            executeStatement(statementStr, statementId);
          } else {
            log.info("Skipping null statement for ID {}", statementId);
          }
        }
      }
    } catch (WakeupException wue) {
      if (!closed.get()) {
        throw wue;
      }
    } finally {
      commandConsumer.close();
    }
  }

  /**
   * Halt the poll-execute loop.
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
   * @param statement The string of the statement to be distributed.
   * @return The ID assigned to the statement.
   */
  public String distributeStatement(String statement) throws InterruptedException, ExecutionException {
    String commandId = getNewCommandId();
    commandProducer.send(new ProducerRecord<>(commandTopic, commandId, statement)).get();
    return commandId;
  }

  private LinkedHashMap<String, String> getPriorCommands() {
    LinkedHashMap<String, String> result = new LinkedHashMap<>();
    for (ConsumerRecord<String, String> commandRecord : getAllPriorCommandRecords()) {
      String commandId = commandRecord.key();
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

  private List<ConsumerRecord<String, String>> getAllPriorCommandRecords() {
    Collection<TopicPartition> commandTopicPartitions = getTopicPartitionsForTopic(commandTopic);

    commandConsumer.poll(0); // Have to poll to make sure subscription has taken effect (subscribe() is lazy)
    commandConsumer.seekToBeginning(commandTopicPartitions);

    Map<TopicPartition, Long> currentOffsets = new HashMap<>();

    List<ConsumerRecord<String, String>> result = new ArrayList<>();
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
        ConsumerRecords<String, String> records = commandConsumer.poll(30000);
        if (records.isEmpty()) {
          log.warn("No records received after 30 seconds of polling; something may be wrong");
        } else {
          log.debug("Received {} records from poll", records.count());
          for (ConsumerRecord<String, String> record : records) {
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

  private void executeStatement(String statementStr, String statementId) {
    log.info("Executing statement: " + statementStr);
    try {
      statementExecutor.handleStatement(statementStr, statementId);
    } catch (WakeupException wue) {
      throw wue;
    } catch (Exception exception) {
      StringWriter stringWriter = new StringWriter();
      PrintWriter printWriter = new PrintWriter(stringWriter);
      exception.printStackTrace(printWriter);
      log.error("Exception encountered during poll-parse-execute loop: " + stringWriter.toString());
    }
  }

  private String getNewCommandId() {
    return String.format("%s_%d", nodeId, statementSuffix.incrementAndGet()).toUpperCase();
  }
}
