/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.ksql.rest.server.utils;

import io.confluent.ksql.rest.entity.CommandId;
import io.confluent.ksql.rest.server.computation.Command;
import io.confluent.ksql.util.Pair;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class TestUtils {

  public List<Pair<CommandId, Command>> getAllPriorCommandRecords() {
    final List<Pair<CommandId, Command>> priorCommands = new ArrayList<>();

    final Command csCommand = new Command("CREATE STREAM pageview "
                                    + "(viewtime bigint, pageid varchar, userid varchar) "
                                    + "WITH (kafka_topic='pageview_topic_json', value_format='json');", true,
            Collections.emptyMap(), Collections.emptyMap());
    final CommandId csCommandId =  new CommandId(CommandId.Type.STREAM, "_CSASStreamGen", CommandId.Action.CREATE);
    priorCommands.add(new Pair<>(csCommandId, csCommand));

    final Command csasCommand = new Command("CREATE STREAM user1pv "
                                      + " AS select * from pageview WHERE userid = 'user1';", true,
            Collections.emptyMap(), Collections.emptyMap());

    final CommandId csasCommandId =  new CommandId(CommandId.Type.STREAM, "_CSASGen", CommandId.Action.CREATE);
    priorCommands.add(new Pair<>(csasCommandId, csasCommand));


    final Command ctasCommand = new Command("CREATE TABLE user1pvtb "
                                      + " AS select * from pageview window tumbling(size 5 "
                                      + "second) WHERE userid = "
                                      + "'user1' group by pageid;", true,
            Collections.emptyMap(), Collections.emptyMap());

    final CommandId ctasCommandId =  new CommandId(CommandId.Type.TABLE, "_CTASGen", CommandId.Action.CREATE);
    priorCommands.add(new Pair<>(ctasCommandId, ctasCommand));

    return priorCommands;
  }

  /**
   * Find a free port.
   *
   * <p>Note: Has a inherent race condition:
   * after finding the free port it releases it so the caller can use it. This opens up a window in
   * which another application can grab the free port, causing the test to fail.
   *
   * <p>Use only where there is no alternative. Jetty, for example, can allocate its own free
   * port. Where you do use it, ensure you do so in a loop that will retry if the port is no longer
   * free by the time the test comes to using it.
   *
   * @return a port that was just free and hopefully still is.
   */
  public static int randomFreeLocalPort() throws IOException {
    final ServerSocket s = new ServerSocket(0);
    final int port = s.getLocalPort();
    s.close();
    return port;
  }
}
