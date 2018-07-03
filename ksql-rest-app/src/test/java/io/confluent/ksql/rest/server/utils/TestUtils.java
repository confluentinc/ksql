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

package io.confluent.ksql.rest.server.utils;


import java.io.IOException;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.KsqlEngine;
import io.confluent.ksql.function.InternalFunctionRegistry;
import io.confluent.ksql.metastore.MetaStoreImpl;
import io.confluent.ksql.rest.server.computation.Command;
import io.confluent.ksql.rest.server.computation.CommandId;
import io.confluent.ksql.util.KafkaTopicClient;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.Pair;

public class TestUtils {

  public List<Pair<CommandId, Command>> getAllPriorCommandRecords() {
    List<Pair<CommandId, Command>> priorCommands = new ArrayList<>();

    Command topicCommand = new Command(
        "REGISTER TOPIC pageview_topic WITH "
            + "(value_format = 'json', kafka_topic='pageview_topic_json');",
        Collections.emptyMap(), Collections.emptyMap());
    CommandId topicCommandId =  new CommandId(CommandId.Type.TOPIC, "_CSASTopicGen", CommandId.Action.CREATE);
    priorCommands.add(new Pair<>(topicCommandId, topicCommand));


    Command csCommand = new Command("CREATE STREAM pageview "
                                    + "(viewtime bigint, pageid varchar, userid varchar) "
                                    + "WITH (registered_topic = 'pageview_topic');",
                                    Collections.emptyMap(), Collections.emptyMap());
    CommandId csCommandId =  new CommandId(CommandId.Type.STREAM, "_CSASStreamGen", CommandId.Action.CREATE);
    priorCommands.add(new Pair<>(csCommandId, csCommand));

    Command csasCommand = new Command("CREATE STREAM user1pv "
                                      + " AS select * from pageview WHERE userid = 'user1';",
                                      Collections.emptyMap(), Collections.emptyMap());

    CommandId csasCommandId =  new CommandId(CommandId.Type.STREAM, "_CSASGen", CommandId.Action.CREATE);
    priorCommands.add(new Pair<>(csasCommandId, csasCommand));


    Command ctasCommand = new Command("CREATE TABLE user1pvtb "
                                      + " AS select * from pageview window tumbling(size 5 "
                                      + "second) WHERE userid = "
                                      + "'user1' group by pageid;",
                                      Collections.emptyMap(), Collections.emptyMap());

    CommandId ctasCommandId =  new CommandId(CommandId.Type.TABLE, "_CTASGen", CommandId.Action.CREATE);
    priorCommands.add(new Pair<>(ctasCommandId, ctasCommand));

    return priorCommands;
  }

  public static KsqlEngine createKsqlEngine(final KsqlConfig ksqlConfig,
                                            final KafkaTopicClient topicClient,
                                            final SchemaRegistryClient schemaRegistryClient) {
    class TestKsqlEngine extends KsqlEngine {
      private TestKsqlEngine() {
        super(
            topicClient,
            schemaRegistryClient,
            new MetaStoreImpl(new InternalFunctionRegistry()));
      }
    };

    return new TestKsqlEngine();
  }

  public static int randomFreeLocalPort() throws IOException {
    ServerSocket s = new ServerSocket(0);
    int port = s.getLocalPort();
    s.close();
    return port;
  }
}
