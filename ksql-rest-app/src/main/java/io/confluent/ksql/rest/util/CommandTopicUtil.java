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

package io.confluent.ksql.rest.util;

import com.google.common.collect.Lists;
import io.confluent.kafka.serializers.KafkaJsonDeserializer;
import io.confluent.kafka.serializers.KafkaJsonDeserializerConfig;
import io.confluent.kafka.serializers.KafkaJsonSerializer;
import io.confluent.ksql.rest.server.computation.Command;
import io.confluent.ksql.rest.server.computation.CommandId;
import io.confluent.ksql.rest.server.computation.QueuedCommand;
import io.confluent.ksql.rest.server.computation.QueuedCommandStatus;
import io.confluent.ksql.rest.server.computation.RestoreCommands;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CommandTopicUtil {

  private static final Logger log = LoggerFactory.getLogger(CommandTopicUtil.class);

  private final Consumer<CommandId, Command> commandConsumer;
  private final Producer<CommandId, Command> commandProducer;

  public CommandTopicUtil(
      final String commandTopic,
      final Map<String, Object> commandConsumerProperties
  ) {
    this.commandConsumer = new KafkaConsumer<>(
        commandConsumerProperties,
        CommandTopicUtil.getJsonDeserializer(CommandId.class, true),
        CommandTopicUtil.getJsonDeserializer(Command.class, false)
    );

    this.commandProducer = new KafkaProducer<>(
        commandConsumerProperties,
        CommandTopicUtil.getJsonSerializer(true),
        CommandTopicUtil.getJsonSerializer(false)
    );
    commandConsumer.assign(Collections.singleton(new TopicPartition(commandTopic, 0)));
  }

  public CommandTopicUtil(
      final Consumer<CommandId, Command> commandConsumer,
      final Producer<CommandId, Command> commandProducer
  ) {
    this.commandConsumer = commandConsumer;
    this.commandProducer = commandProducer;
  }


  @SuppressWarnings("unchecked")
  public void send(final ProducerRecord producerRecord)
      throws ExecutionException, InterruptedException {
    Objects.requireNonNull(producerRecord);
    commandProducer.send(producerRecord).get();
  }

  public List<QueuedCommand> getNewCommands(
      final Map<CommandId, QueuedCommandStatus> commandStatusMap
  ) {
    final List<QueuedCommand> queuedCommands = Lists.newArrayList();
    commandConsumer.poll(Duration.ofMillis(Long.MAX_VALUE)).forEach(
        c -> queuedCommands.add(
            new QueuedCommand(
                c.key(),
                Optional.ofNullable(c.value()),
                Optional.ofNullable(commandStatusMap.remove(c.key()))
            )
        )
    );
    return queuedCommands;
  }

  public RestoreCommands getRestoreCommands(
      final String commandTopic,
      final Duration duration
  ) {
    final RestoreCommands restoreCommands = new RestoreCommands();

    final Collection<TopicPartition> cmdTopicPartitions = getTopicPartitionsForTopic(commandTopic);

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
    final List<PartitionInfo> partitionInfoList = commandConsumer.partitionsFor(topic);

    final Collection<TopicPartition> result = new HashSet<>();
    for (final PartitionInfo partitionInfo : partitionInfoList) {
      result.add(new TopicPartition(partitionInfo.topic(), partitionInfo.partition()));
    }
    return result;
  }

  public void close() {
    commandConsumer.wakeup();
    commandProducer.close();
  }

  public Consumer<CommandId, Command> getCommandConsumer() {
    return commandConsumer;
  }

  public Producer<CommandId, Command> getCommandProducer() {
    return commandProducer;
  }

  public static <T> Serializer<T> getJsonSerializer(final boolean isKey) {
    final Serializer<T> result = new KafkaJsonSerializer<>();
    result.configure(Collections.emptyMap(), isKey);
    return result;
  }

  public static <T> Deserializer<T> getJsonDeserializer(
      final Class<T> classs,
      final boolean isKey) {
    final Deserializer<T> result = new KafkaJsonDeserializer<>();
    final String typeConfigProperty = isKey
        ? KafkaJsonDeserializerConfig.JSON_KEY_TYPE
        : KafkaJsonDeserializerConfig.JSON_VALUE_TYPE;

    final Map<String, ?> props = Collections.singletonMap(
        typeConfigProperty,
        classs
    );
    result.configure(props, isKey);
    return result;
  }


}
