package io.confluent.ksql.rest.server.utils;


import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import io.confluent.ksql.rest.server.computation.Command;
import io.confluent.ksql.rest.server.computation.CommandId;
import io.confluent.ksql.util.KsqlConfig;

public class TestUtils {

  public static KsqlConfig getMockKsqlConfig() {
    Map<String, Object> props = new HashMap<>();
    props.put("application.id", "ksqlStatementExecutorTest");
    props.put("bootstrap.servers", "localhost:9092");
    return new KsqlConfig(props);
  }

  public LinkedHashMap<CommandId, Command> getAllPriorCommandRecords() {
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

    return priorCommands;
  }
}
