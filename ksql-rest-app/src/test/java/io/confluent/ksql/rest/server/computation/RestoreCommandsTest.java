/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.confluent.ksql.rest.server.computation;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.util.Pair;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class RestoreCommandsTest {

  private final RestoreCommands restoreCommands = new RestoreCommands();
  private final CommandId createId = new CommandId(CommandId.Type.TABLE, "foo", CommandId.Action.CREATE);
  private final CommandId dropId = new CommandId(CommandId.Type.TABLE, "foo", CommandId.Action.DROP);
  private final CommandId terminateId = new CommandId(CommandId.Type.TERMINATE,
      "queryId",
      CommandId.Action.EXECUTE);
  private final Command createCommand = new Command("create table foo", Collections.emptyMap());
  private final Command dropCommand = new Command("drop table foo", Collections.emptyMap());
  private final Command terminateCommand = new Command("terminate query 'queryId'", Collections.emptyMap());

  @Test
  public void shouldHaveMapContainingTerminatedQueriesThatWereIssuedAfterCreate() {
    restoreCommands.addCommand(createId,
        createCommand);
    restoreCommands.addCommand(terminateId,
        terminateCommand);

    restoreCommands.forEach((commandId, command, terminatedQueries, droppedEntities) -> {
      assertThat(commandId, equalTo(createId));
      assertThat(command, equalTo(createCommand));
      assertThat(terminatedQueries, equalTo(Collections.singletonMap(new QueryId("queryId"),
          terminateId)));
    });
  }

  @Test
  public void shouldNotHaveTerminatedQueryWhenDroppedAndRecreatedAfterTerminate() {
    restoreCommands.addCommand(createId,
        createCommand);
    restoreCommands.addCommand(terminateId,
        terminateCommand);
    // drop
    restoreCommands.addCommand(dropId, dropCommand);
    // recreate
    restoreCommands.addCommand(createId, createCommand);
    final List<Pair<CommandId,  Map<QueryId, CommandId>>> results = new ArrayList<>();
    restoreCommands.forEach((commandId, command, terminatedQueries, droppedEntities) -> {
      results.add(new Pair<>(commandId, terminatedQueries));
    });

    assertThat(results, equalTo(Arrays.asList(
        new Pair<>(createId, Collections.singletonMap(new QueryId("queryId"), terminateId)),
        new Pair<>(dropId, Collections.<QueryId, CommandId>emptyMap()),
        new Pair<>(createId, Collections.<QueryId, CommandId>emptyMap()))));
  }

  @Test
  public void shouldNotHaveTerminatedQueriesIssuedBeforeCreate() {
    restoreCommands.addCommand(terminateId,
        terminateCommand);
    restoreCommands.addCommand(createId,
        createCommand);

    restoreCommands.forEach((commandId, command, terminatedQueries, dropped) -> {
      assertThat(commandId, equalTo(createId));
      assertThat(command, equalTo(createCommand));
      assertThat(terminatedQueries, equalTo(Collections.emptyMap()));
    });

  }

  @Test
  public void shouldIterateCommandsInOrderTheyWereIssued() {
    restoreCommands.addCommand(createId,
        createCommand);
    final CommandId createStreamOneId =
        new CommandId(CommandId.Type.STREAM, "stream", CommandId.Action.CREATE);

    restoreCommands.addCommand(createStreamOneId,
        new Command("create stream one", Collections.emptyMap()));

    final List<CommandId> results = new ArrayList<>();
    restoreCommands.forEach((commandId, command, terminatedQueries, dropped) -> {
      results.add(commandId);
    });
    assertThat(results, equalTo(Arrays.asList(createId, createStreamOneId)));
  }

  @Test
  public void shouldHaveTerminatedQueriesWhenMultipleCreateDropTerminateForCommand() {
    // create
    restoreCommands.addCommand(createId, createCommand);
    // terminate
    restoreCommands.addCommand(terminateId, terminateCommand);
    // drop
    restoreCommands.addCommand(dropId, dropCommand);
    // recreate
    restoreCommands.addCommand(createId, createCommand);
    // another one for good measure
    restoreCommands.addCommand(new CommandId(CommandId.Type.STREAM, "bar", CommandId.Action.CREATE), createCommand);
    // terminate again
    restoreCommands.addCommand(terminateId, terminateCommand);

    final Map<CommandId, Map<QueryId, CommandId>> commandIdToTerminate = new HashMap<>();
    restoreCommands.forEach((commandId, command, terminatedQueries, dropped)
        -> commandIdToTerminate.put(commandId, terminatedQueries));

    assertThat(commandIdToTerminate.get(createId), equalTo(Collections.singletonMap(new QueryId("queryId"),
        terminateId)));
  }

  @Test
  public void shouldBeDroppedWhenDropCommandAfterCreate() {
    restoreCommands.addCommand(createId, createCommand);
    restoreCommands.addCommand(dropId, dropCommand);

    final Map<CommandId, Boolean> results = new HashMap<>();
    restoreCommands.forEach((commandId, command, terminatedQueries, dropped)
        -> results.put(commandId, dropped));

    assertTrue(results.get(createId));
  }

  @Test
  public void shouldNotBeDroppedWhenDropCommandBeforeCreate() {
    restoreCommands.addCommand(dropId, dropCommand);
    restoreCommands.addCommand(createId, createCommand);

    final Map<CommandId, Boolean> results = new HashMap<>();
    restoreCommands.forEach((commandId, command, terminatedQueries, dropped)
        -> results.put(commandId, dropped));

    assertFalse(results.get(createId));
  }

}