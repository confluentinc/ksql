/*
 * Copyright 2020 Confluent Inc.
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

package io.confluent.ksql.rest.server;

import com.google.common.collect.Lists;
import io.confluent.ksql.rest.server.computation.QueuedCommand;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CommandTopic {

  private static final Logger log = LoggerFactory.getLogger(CommandTopic.class);
  private final TopicPartition commandTopicPartition;

  private Consumer<byte[], byte[]> commandConsumer;
  private final String commandTopicName;
  private CommandTopicBackup commandTopicBackup;

  public CommandTopic(
      final String commandTopicName,
      final Map<String, Object> kafkaConsumerProperties,
      final CommandTopicBackup commandTopicBackup
  ) {
    this(
        commandTopicName,
        new KafkaConsumer<>(
            Objects.requireNonNull(kafkaConsumerProperties, "kafkaClientProperties"),
            new ByteArrayDeserializer(),
            new ByteArrayDeserializer()
        ),
        commandTopicBackup
    );
  }

  CommandTopic(
      final String commandTopicName,
      final Consumer<byte[], byte[]> commandConsumer,
      final CommandTopicBackup commandTopicBackup
  ) {
    this.commandTopicPartition = new TopicPartition(commandTopicName, 0);
    this.commandConsumer = Objects.requireNonNull(commandConsumer, "commandConsumer");
    this.commandTopicName = Objects.requireNonNull(commandTopicName, "commandTopicName");
    this.commandTopicBackup = Objects.requireNonNull(commandTopicBackup, "commandTopicBackup");
  }

  public String getCommandTopicName() {
    return commandTopicName;
  }

  public void start() {
    commandTopicBackup.initialize();
    commandConsumer.assign(Collections.singleton(commandTopicPartition));
  }

  public Iterable<ConsumerRecord<byte[], byte[]>> getNewCommands(final Duration timeout) {
    final Iterable<ConsumerRecord<byte[], byte[]>> iterable = commandConsumer.poll(timeout);

    if (iterable != null) {
      iterable.forEach(this::backupRecord);
    }

    return iterable;
  }

  public List<QueuedCommand> getRestoreCommands(final Duration duration) {
    final List<QueuedCommand> restoreCommands = Lists.newArrayList();

    commandConsumer.seekToBeginning(
        Collections.singletonList(commandTopicPartition));

    log.debug("Reading prior command records");
    ConsumerRecords<byte[], byte[]> records =
        commandConsumer.poll(duration);
    while (!records.isEmpty()) {
      log.debug("Received {} records from poll", records.count());
      for (final ConsumerRecord<byte[], byte[]> record : records) {
        backupRecord(record);

        if (record.value() == null) {
          continue;
        }
        restoreCommands.add(
            new QueuedCommand(
                record.key(),
                record.value(),
                Optional.empty(),
                record.offset()));
      }
      records = commandConsumer.poll(duration);
    }
    return restoreCommands;
  }

  public long getCommandTopicConsumerPosition() {
    return commandConsumer.position(commandTopicPartition);
  }

  public long getEndOffset() {
    return commandConsumer.endOffsets(Collections.singletonList(commandTopicPartition))
        .get(commandTopicPartition);
  }

  public void wakeup() {
    commandConsumer.wakeup();
  }

  public void close() {
    commandConsumer.close();
    commandTopicBackup.close();
  }

  private void backupRecord(final ConsumerRecord<byte[], byte[]> record) {
    commandTopicBackup.writeRecord(record);
  }
}
