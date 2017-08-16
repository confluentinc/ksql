package io.confluent.ksql.rest.server.computation;


import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import io.confluent.kafka.serializers.KafkaJsonDeserializer;
import io.confluent.kafka.serializers.KafkaJsonSerializer;
import io.confluent.ksql.metastore.MetaStoreImpl;
import io.confluent.ksql.rest.entity.CommandStatus;
import io.confluent.ksql.rest.server.StatementParser;
import io.confluent.ksql.rest.server.mock.MockCommandStore;
import io.confluent.ksql.rest.server.mock.MockKafkaTopicClient;
import io.confluent.ksql.rest.server.mock.MockKsqkEngine;
import io.confluent.ksql.rest.server.utils.TestUtils;

public class CommandRunnerTest {

  private MockKsqkEngine mockKsqkEngine = new MockKsqkEngine(
      TestUtils.getMockKsqlConfig(), new MockKafkaTopicClient());
  private StatementParser statementParser = new StatementParser(mockKsqkEngine);
  StatementExecutor statementExecutor = new StatementExecutor(mockKsqkEngine, statementParser);
  CommandRunner commandRunner = null;

  private CommandRunner getCommanRunner() {
    if (commandRunner != null) {
      return commandRunner;
    }
    Map<String, Object> commandConsumerProperties = new HashMap<>();
    commandConsumerProperties.put("bootstrap.servers", "localhost:9092");
    Serializer<Command> commandSerializer = new KafkaJsonSerializer<>();
    Deserializer<Command> commandDeserializer = new KafkaJsonDeserializer<>();
    Serializer<CommandId> commandIdSerializer = new KafkaJsonSerializer<>();
    Deserializer<CommandId> commandIdDeserializer = new KafkaJsonDeserializer<>();

    KafkaConsumer<CommandId, Command> commandConsumer = new KafkaConsumer<>(
        commandConsumerProperties,
        commandIdDeserializer,
        commandDeserializer
    );

    CommandRunner commandRunner = new CommandRunner(statementExecutor, new MockCommandStore
        ("CT", commandConsumer, null,
         new CommandIdAssigner(new MetaStoreImpl())));
    return commandRunner;
  }


  @Test
  public void testThread() throws InterruptedException {
    CommandRunner commandRunner = getCommanRunner();
    new Thread(commandRunner).start();
    Thread.sleep(5000);
    commandRunner.close();
    CommandId topicCommandId =  new CommandId(CommandId.Type.TOPIC, "_CSASTopicGen");
    CommandId csCommandId =  new CommandId(CommandId.Type.STREAM, "_CSASStreamGen");
    CommandId csasCommandId =  new CommandId(CommandId.Type.STREAM, "_CSASGen");
    CommandId ctasCommandId =  new CommandId(CommandId.Type.TABLE, "_CTASGen");

    Map<CommandId, CommandStatus> statusStore = statementExecutor.getStatuses();
    Assert.assertNotNull(statusStore);
    Assert.assertEquals(statusStore.size(), 4);
    Assert.assertEquals(statusStore.get(topicCommandId).getStatus(), CommandStatus.Status.SUCCESS);
    Assert.assertEquals(statusStore.get(csCommandId).getStatus(), CommandStatus.Status.SUCCESS);
    Assert.assertEquals(statusStore.get(csasCommandId).getStatus(), CommandStatus.Status.ERROR);
    Assert.assertEquals(statusStore.get(ctasCommandId).getStatus(), CommandStatus.Status.ERROR);
  }

  @Test
  public void testPriorCommandsRun() throws Exception {
    CommandRunner commandRunner = getCommanRunner();
    commandRunner.processPriorCommands();
    CommandId topicCommandId =  new CommandId(CommandId.Type.TOPIC, "_CSASTopicGen");
    CommandId csCommandId =  new CommandId(CommandId.Type.STREAM, "_CSASStreamGen");
    CommandId csasCommandId =  new CommandId(CommandId.Type.STREAM, "_CSASGen");
    CommandId ctasCommandId =  new CommandId(CommandId.Type.TABLE, "_CTASGen");
    Map<CommandId, CommandStatus> statusStore = statementExecutor.getStatuses();
    Assert.assertNotNull(statusStore);
    Assert.assertEquals(statusStore.size(), 4);
    Assert.assertEquals(statusStore.get(topicCommandId).getStatus(), CommandStatus.Status.SUCCESS);
    Assert.assertEquals(statusStore.get(csCommandId).getStatus(), CommandStatus.Status.SUCCESS);
    Assert.assertEquals(statusStore.get(csasCommandId).getStatus(), CommandStatus.Status.ERROR);
    Assert.assertEquals(statusStore.get(ctasCommandId).getStatus(), CommandStatus.Status.ERROR);
  }

}
