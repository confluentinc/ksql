package io.confluent.ksql.rest.server.computation;


import org.easymock.EasyMockSupport;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import io.confluent.ksql.rest.entity.CommandStatus;
import io.confluent.ksql.rest.server.StatementParser;
import io.confluent.ksql.rest.server.mock.MockKafkaTopicClient;
import io.confluent.ksql.rest.server.mock.MockKsqkEngine;
import io.confluent.ksql.util.KsqlConfig;

public class StatementExecutorTest extends EasyMockSupport {

  private MockKsqkEngine mockKsqkEngine = new MockKsqkEngine(
      getMockKsqlConfig(), new MockKafkaTopicClient());

  private StatementParser statementParser = new StatementParser(mockKsqkEngine);

  StatementExecutor statementExecutor = new StatementExecutor(mockKsqkEngine, statementParser);



  @Test
  public void handleCorrectDDLStatement() throws Exception {
    Command command = new Command("REGISTER TOPIC users_topic WITH (value_format = 'json', "
                                  + "kafka_topic='user_topic_json');", new HashMap<>());
    CommandId commandId =  new CommandId(CommandId.Type.TOPIC, "_CorrectTopicGen");
    statementExecutor.handleStatement(command, commandId);
    Map<CommandId, CommandStatus> statusStore = statementExecutor.getStatuses();
    Assert.assertNotNull(statusStore);
    Assert.assertEquals(statusStore.size(), 1);
    Assert.assertEquals(statusStore.get(commandId).getStatus(), CommandStatus.Status.SUCCESS);

  }

  @Test
  public void handleIncorrectDDLStatement() throws Exception {
    Command command = new Command("REGIST ER TOPIC users_topic WITH (value_format = 'json', "
                                  + "kafka_topic='user_topic_json');", new HashMap<>());
    CommandId commandId =  new CommandId(CommandId.Type.TOPIC, "_IncorrectTopicGen");
    statementExecutor.handleStatement(command, commandId);
    Map<CommandId, CommandStatus> statusStore = statementExecutor.getStatuses();
    Assert.assertNotNull(statusStore);
    Assert.assertEquals(statusStore.size(), 1);
    Assert.assertEquals(statusStore.get(commandId).getStatus(), CommandStatus.Status.ERROR);

  }

  @Test
  public void handleCSAS_CTASStatement() throws Exception {

    Command topicCommand = new Command("REGISTER TOPIC pageview_topic WITH "
                                       + "(value_format = 'json', "
                                       + "kafka_topic='pageview_topic_json');", new HashMap<>());
    CommandId topicCommandId =  new CommandId(CommandId.Type.TOPIC, "_CSASTopicGen");
    statementExecutor.handleStatement(topicCommand, topicCommandId);

    Command csCommand = new Command("CREATE STREAM pageview "
                                    + "(viewtime bigint, pageid varchar, userid varchar) "
                                    + "WITH (registered_topic = 'pageview_topic');",
                                    new HashMap<>());
    CommandId csCommandId =  new CommandId(CommandId.Type.STREAM, "_CSASStreamGen");
    statementExecutor.handleStatement(csCommand, csCommandId);

    Command csasCommand = new Command("CREATE STREAM user1pv "
                                    + " AS select * from pageview WHERE userid = 'user1';",
                                    new HashMap<>());

    CommandId csasCommandId =  new CommandId(CommandId.Type.STREAM, "_CSASGen");
    statementExecutor.handleStatement(csasCommand, csasCommandId);

    Command ctasCommand = new Command("CREATE TABLE user1pvtb "
                                      + " AS select * from pageview window tumbling(size 5 "
                                      + "second) WHERE userid = "
                                      + "'user1' group by pageid;",
                                      new HashMap<>());

    CommandId ctasCommandId =  new CommandId(CommandId.Type.TABLE, "_CTASGen");
    statementExecutor.handleStatement(ctasCommand, ctasCommandId);

    Command terminateCommand = new Command("TERMINATE 1;",
                                      new HashMap<>());

    CommandId terminateCommandId =  new CommandId(CommandId.Type.TABLE, "_TerminateGen");
    statementExecutor.handleStatement(terminateCommand, terminateCommandId);

    Map<CommandId, CommandStatus> statusStore = statementExecutor.getStatuses();
    Assert.assertNotNull(statusStore);
    Assert.assertEquals(statusStore.size(), 5);
    Assert.assertEquals(statusStore.get(topicCommandId).getStatus(), CommandStatus.Status.SUCCESS);
    Assert.assertEquals(statusStore.get(csCommandId).getStatus(), CommandStatus.Status.SUCCESS);
    Assert.assertEquals(statusStore.get(csasCommandId).getStatus(), CommandStatus.Status.ERROR);
    Assert.assertEquals(statusStore.get(ctasCommandId).getStatus(), CommandStatus.Status.ERROR);
    Assert.assertEquals(statusStore.get(terminateCommandId).getStatus(), CommandStatus.Status.ERROR);
  }

  @Test
  public void handlePriorStatement() throws Exception {
    LinkedHashMap<CommandId, Command> priorCommands = new LinkedHashMap<>();

    Command topicCommand = new Command("REGISTER TOPIC pageview_topic WITH "
                                       + "(value_format = 'json', "
                                       + "kafka_topic='pageview_topic_json');", new HashMap<>());
    CommandId topicCommandId =  new CommandId(CommandId.Type.TOPIC, "_CSASTopicGen");
    priorCommands.put(topicCommandId, topicCommand);


    Command csCommand = new Command("CREATE STREAM pageview "
                                    + "(viewtime bigint, pageid varchar, userid varchar) "
                                    + "WITH (registered_topic = 'pageview_topic');",
                                    new HashMap<>());
    CommandId csCommandId =  new CommandId(CommandId.Type.STREAM, "_CSASStreamGen");
    priorCommands.put(csCommandId, csCommand);

    Command csasCommand = new Command("CREATE STREAM user1pv "
                                      + " AS select * from pageview WHERE userid = 'user1';",
                                      new HashMap<>());

    CommandId csasCommandId =  new CommandId(CommandId.Type.STREAM, "_CSASGen");
    priorCommands.put(csasCommandId, csasCommand);


    Command ctasCommand = new Command("CREATE TABLE user1pvtb "
                                      + " AS select * from pageview window tumbling(size 5 "
                                      + "second) WHERE userid = "
                                      + "'user1' group by pageid;",
                                      new HashMap<>());

    CommandId ctasCommandId =  new CommandId(CommandId.Type.TABLE, "_CTASGen");
    priorCommands.put(ctasCommandId, ctasCommand);

    statementExecutor.handleStatements(priorCommands);

    Map<CommandId, CommandStatus> statusStore = statementExecutor.getStatuses();
    Assert.assertNotNull(statusStore);
    Assert.assertEquals(statusStore.size(), 4);
    Assert.assertEquals(statusStore.get(topicCommandId).getStatus(), CommandStatus.Status.SUCCESS);
    Assert.assertEquals(statusStore.get(csCommandId).getStatus(), CommandStatus.Status.SUCCESS);
    Assert.assertEquals(statusStore.get(csasCommandId).getStatus(), CommandStatus.Status.ERROR);
    Assert.assertEquals(statusStore.get(ctasCommandId).getStatus(), CommandStatus.Status.ERROR);
  }

  private static KsqlConfig getMockKsqlConfig() {
    Map<String, Object> props = new HashMap<>();
    props.put("application.id", "ksqlStatementExecutorTest");
    props.put("bootstrap.servers", "localhost:9092");
    return new KsqlConfig(props);
  }

}
