/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.ksql.rest.server;

import com.google.common.collect.Lists;
import io.confluent.ksql.rest.server.computation.Command;
import io.confluent.ksql.rest.server.computation.CommandId;
import io.confluent.ksql.rest.server.computation.QueuedCommand;
import io.confluent.ksql.rest.server.computation.QueuedCommandStatus;
import io.confluent.ksql.rest.server.computation.RestoreCommands;
import io.confluent.ksql.rest.util.CommandTopicJsonSerdeUtil;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CommandTopic {

  private static final Logger log = LoggerFactory.getLogger(CommandTopic.class);

  private final Consumer<CommandId, Command> commandConsumer;
  private final Producer<CommandId, Command> commandProducer;
  private final String commandTopicName;

  public CommandTopic(
      final String commandTopicName,
      final Map<String, Object> kafkaClientProperties
  ) {
    this(
        commandTopicName,
        new KafkaConsumer<>(
            Objects.requireNonNull(kafkaClientProperties, "kafkaClientProperties"),
            CommandTopicJsonSerdeUtil.getJsonDeserializer(CommandId.class, true),
            CommandTopicJsonSerdeUtil.getJsonDeserializer(Command.class, false)
    ),

    new KafkaProducer<>(
        Objects.requireNonNull(kafkaClientProperties, "kafkaClientProperties"),
        CommandTopicJsonSerdeUtil.getJsonSerializer(true),
        CommandTopicJsonSerdeUtil.getJsonSerializer(false)
    ));
    commandConsumer.assign(Collections.singleton(new TopicPartition(commandTopicName, 0)));
  }

  CommandTopic(
      final String commandTopicName,
      final Consumer<CommandId, Command> commandConsumer,
      final Producer<CommandId, Command> commandProducer
  ) {
    Objects.requireNonNull(commandTopicName, "commandTopicName");
    Objects.requireNonNull(commandConsumer, "commandConsumer");
    Objects.requireNonNull(commandProducer, "commandProducer");
    this.commandConsumer = commandConsumer;
    this.commandProducer = commandProducer;
    this.commandTopicName = commandTopicName;
  }


  @SuppressWarnings("unchecked")
  public void send(final CommandId commandId, final Command command)
      throws Exception {
    Objects.requireNonNull(commandId, "commandId");
    Objects.requireNonNull(command, "command");
    final ProducerRecord producerRecord = new ProducerRecord<>(
        commandTopicName,
        commandId,
        command);
    try {
      commandProducer.send(producerRecord).get();
    } catch (final ExecutionException e) {
      if (e.getCause() instanceof Exception) {
        throw (Exception)e.getCause();
      }
      throw new RuntimeException(e.getCause());
    }
  }

  public Iterable<ConsumerRecord<CommandId, Command>> getNewCommands(final Duration timeout) {
    return commandConsumer.poll(timeout);
  }

  public RestoreCommands getRestoreCommands(
      final Duration duration
  ) {
    Objects.requireNonNull(duration, "duration");
    final RestoreCommands restoreCommands = new RestoreCommands();

    final Collection<TopicPartition> cmdTopicPartitions =
        getTopicPartitionsForTopic(commandTopicName);

    commandConsumer.seekToBeginning(cmdTopicPartitions);

    log.debug("Reading prior command records");

    int readCommandCount = 0;
    ConsumerRecords<CommandId, Command> records =
        commandConsumer.poll(duration);
    while (!records.isEmpty()) {
      log.debug("Received {} records from poll", records.count());
      for (final ConsumerRecord<CommandId, Command> record : records) {
        restoreCommands.addCommand(record.key(), record.value());
        readCommandCount ++;
      }
      records = commandConsumer.poll(duration);
    }
    log.debug("Retrieved records:" + readCommandCount);
    return restoreCommands;
  }

  private Collection<TopicPartition> getTopicPartitionsForTopic(final String topic) {
    return commandConsumer.partitionsFor(topic).stream()
        .map(partitionInfo -> new TopicPartition(partitionInfo.topic(), partitionInfo.partition()))
        .collect(Collectors.toList());
  }

  public void close() {
    commandConsumer.wakeup();
    commandConsumer.close();
    commandProducer.close();
  }
}
