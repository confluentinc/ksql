/**
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

package io.confluent.ksql.rest.server.mock;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.TopicPartition;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import io.confluent.ksql.metastore.MetaStoreImpl;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.rest.server.computation.Command;
import io.confluent.ksql.rest.server.computation.CommandId;
import io.confluent.ksql.rest.server.computation.CommandIdAssigner;
import io.confluent.ksql.rest.server.computation.CommandStore;
import io.confluent.ksql.rest.server.utils.TestUtils;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.Pair;

public class MockCommandStore extends CommandStore {

  CommandIdAssigner commandIdAssigner;

  private final AtomicBoolean closed;
  private boolean isFirstCall = true;

  public MockCommandStore(String commandTopic,
                          Consumer<CommandId, Command> commandConsumer,
                          Producer<CommandId, Command> commandProducer,
                          CommandIdAssigner commandIdAssigner) {
    super(commandTopic, commandConsumer, commandProducer,
          new CommandIdAssigner(new MetaStoreImpl()));

    commandIdAssigner = new CommandIdAssigner(new MetaStoreImpl());
    closed = new AtomicBoolean(false);
  }

  @Override
  public void close() {
    closed.set(true);
  }

  @Override
  public ConsumerRecords<CommandId, Command> getNewCommands() {
    List<ConsumerRecord<CommandId, Command>> records = new ArrayList<>();
    Map<TopicPartition, List<ConsumerRecord<CommandId, Command>>> recordsMap = new HashMap<>();
    if (isFirstCall) {
      List<Pair<CommandId, Command>> commands = new TestUtils().getAllPriorCommandRecords();
      for (Pair<CommandId, Command> commandIdCommandPair: commands) {
        records.add(new ConsumerRecord<CommandId, Command>(
            "T1",10, 100,
            commandIdCommandPair.getLeft(), commandIdCommandPair.getRight()));
      }

      recordsMap.put(new TopicPartition("T1", 1), records);
      isFirstCall = false;
    } else {
      close();
    }
    return new ConsumerRecords<>(recordsMap);
  }

  @Override
  public CommandId distributeStatement(
      String statementString,
      Statement statement,
      Map<String, Object> streamsProperties
  ) throws KsqlException {
    CommandId commandId = commandIdAssigner.getCommandId(statement);
    return commandId;
  }

  @Override
  public List<Pair<CommandId, Command>> getPriorCommands() {
    return new TestUtils().getAllPriorCommandRecords();
  }

}
