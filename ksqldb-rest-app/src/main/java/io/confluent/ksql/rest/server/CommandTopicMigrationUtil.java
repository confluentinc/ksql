/*
 * Copyright 2022 Confluent Inc.
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

import io.confluent.ksql.rest.entity.CommandId;
import io.confluent.ksql.rest.server.computation.Command;
import io.confluent.ksql.rest.server.computation.CommandStore;
import io.confluent.ksql.rest.server.computation.InternalTopicSerdes;
import io.confluent.ksql.rest.server.computation.QueuedCommand;
import io.confluent.ksql.util.KsqlConfig;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

public final class CommandTopicMigrationUtil {

  private CommandTopicMigrationUtil() {

  }

  public static void commandTopicMigration(
      final String commandTopic,
      final KsqlRestConfig restConfig,
      final KsqlConfig config
  ) {
    final TopicPartition topicPartition = new TopicPartition(commandTopic, 0);

    // producer for the new command topic
    final Producer<CommandId, Command> newBrokerProducer = new KafkaProducer<>(
        restConfig.getCommandProducerProperties(),
        InternalTopicSerdes.serializer(),
        InternalTopicSerdes.serializer()
    );

    // read all the commands from the old command topic
    final org.apache.kafka.clients.consumer.Consumer<byte[], byte[]> oldBrokerConsumer =
        new KafkaConsumer<>(
            config.originals(),
            new ByteArrayDeserializer(),
            new ByteArrayDeserializer()
    );
    oldBrokerConsumer.assign(Collections.singleton(topicPartition));

    final List<QueuedCommand> commands = CommandTopic.getAllCommandsInCommandTopic(
        oldBrokerConsumer,
        topicPartition,
        Optional.empty(),
        CommandStore.POLLING_TIMEOUT_FOR_COMMAND_TOPIC
    );

    // produce a higher version command to the old command topic so that other servers
    // stop writing to it since they'll be degraded
    final Producer<CommandId, Command> oldBrokerProducer = new KafkaProducer<>(
        config.originals(),
        InternalTopicSerdes.serializer(),
        InternalTopicSerdes.serializer()
    );
    final ProducerRecord<CommandId, Command> degradedCommand = new ProducerRecord<>(
        commandTopic,
        0,
        new CommandId(CommandId.Type.TERMINATE, "", CommandId.Action.TERMINATE),
        new Command(
            "",
            Collections.emptyMap(),
            Collections.emptyMap(),
            Optional.empty(),
            Optional.of(Integer.MAX_VALUE - 1),
            Integer.MAX_VALUE
        )
    );
    oldBrokerProducer.send(degradedCommand);

    // re-create commnad topic
    for (QueuedCommand command : commands) {
      final ProducerRecord<CommandId, Command> producerRecord = new ProducerRecord<>(
          commandTopic,
          0,
          command.getAndDeserializeCommandId(),
          command.getAndDeserializeCommand(InternalTopicSerdes.deserializer(Command.class)));
      newBrokerProducer.send(producerRecord);
    }
  }
}
