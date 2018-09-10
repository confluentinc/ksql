/*
 * Copyright 2017 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.confluent.ksql.rest.server.computation;

import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import java.io.Closeable;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Wrapper class for the command topic. Used for reading from the topic (either all messages from
 * the beginning until now, or any new messages since then), and writing to it.
 */
public class CommandStore implements Closeable {

  private static final Logger log = LoggerFactory.getLogger(CommandStore.class);

  private static final Duration POLLING_TIMEOUT_FOR_COMMAND_TOPIC = Duration.ofMillis(5000);

  private final String commandTopic;
  private final Consumer<CommandId, Command> commandConsumer;
  private final Producer<CommandId, Command> commandProducer;
  private final CommandIdAssigner commandIdAssigner;
  private final AtomicBoolean closed;
  private final StatementExecutor statementExecutor;

  public CommandStore(
      final String commandTopic,
      final Consumer<CommandId, Command> commandConsumer,
      final Producer<CommandId, Command> commandProducer,
      final CommandIdAssigner commandIdAssigner,
      final StatementExecutor statementExecutor
  ) {
    this.commandTopic = commandTopic;
    this.commandConsumer = commandConsumer;
    this.commandProducer = commandProducer;
    this.commandIdAssigner = commandIdAssigner;
    this.statementExecutor = statementExecutor;

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
   * Write the given statement to the command topic, to be read by all nodes in the current
   * cluster.
   * Does not return until the statement has been successfully written, or an exception is thrown.
   *
   * @param statementString The string of the statement to be distributed
   * @param statement The statement to be distributed
   * @param overwriteProperties Any command-specific Streams properties to use.
   * @return The ID assigned to the statement
   */
  public CommandStatusFuture distributeStatement(
      final String statementString,
      final Statement statement,
      final KsqlConfig ksqlConfig,
      final Map<String, Object> overwriteProperties
  ) throws KsqlException {
    final CommandId commandId = commandIdAssigner.getCommandId(statement);
    final Command command = new Command(
        statementString,
        overwriteProperties,
        ksqlConfig.getAllConfigPropsWithSecretsObfuscated());
    final CommandStatusFuture statusFuture = statementExecutor.registerQueuedStatement(commandId);
    try {
      commandProducer.send(new ProducerRecord<>(commandTopic, commandId, command)).get();
    } catch (final Exception e) {
      throw new KsqlException(
          String.format(
              "Could not write the statement '%s' into the "
              + "command topic"
              + ".", statementString
          ),
          e
      );
    }
    return statusFuture;
  }

  /**
   * Poll for new commands, blocking until at least one is available.
   *
   * @return The commands that have been polled from the command topic
   */
  public ConsumerRecords<CommandId, Command> getNewCommands() {
    return commandConsumer.poll(Duration.ofMillis(Long.MAX_VALUE));
  }

  RestoreCommands getRestoreCommands() {
    final RestoreCommands restoreCommands = new RestoreCommands();

    final Collection<TopicPartition> cmdTopicPartitions = getTopicPartitionsForTopic(commandTopic);

    commandConsumer.seekToBeginning(cmdTopicPartitions);

    log.debug("Reading prior command records");

    final Map<CommandId, ConsumerRecord<CommandId, Command>> commands = new LinkedHashMap<>();
    ConsumerRecords<CommandId, Command> records =
        commandConsumer.poll(POLLING_TIMEOUT_FOR_COMMAND_TOPIC);
    while (!records.isEmpty()) {
      log.debug("Received {} records from poll", records.count());
      for (final ConsumerRecord<CommandId, Command> record : records) {
        restoreCommands.addCommand(record.key(), record.value());
      }
      records = commandConsumer.poll(POLLING_TIMEOUT_FOR_COMMAND_TOPIC);
    }
    log.debug("Retrieved records:" + commands.size());
    return restoreCommands;
  }

  private Collection<TopicPartition> getTopicPartitionsForTopic(final String topic) {
    final List<PartitionInfo> partitionInfoList = commandConsumer.partitionsFor(topic);

    final Collection<TopicPartition> result = new HashSet<>();
    for (final PartitionInfo partitionInfo : partitionInfoList) {
      result.add(new TopicPartition(partitionInfo.topic(), partitionInfo.partition()));
    }
    return result;
  }
}
