/*
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

package io.confluent.ksql.rest.server.computation;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.fail;

import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.rest.server.CommandTopic;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.Test;
import org.mockito.Mockito;


@SuppressWarnings("unchecked")
public class CommandStoreTest {

  private static final KsqlConfig KSQL_CONFIG = new KsqlConfig(Collections.emptyMap());
  private final CommandIdAssigner commandIdAssigner = Mockito.mock(CommandIdAssigner.class);
  private final CommandTopic commandTopic = Mockito.mock(CommandTopic.class);
  private final CommandStore commandStore = new CommandStore(commandIdAssigner, commandTopic);

  @Test
  public void shouldCallCloseCorrectly() {
    // Given:

    // When:
    commandStore.close();

    // Then:
    Mockito.verify(commandTopic).close();
  }

  @Test
  public void shouldEnqueueCommandCorrectly() throws Exception {
    // Given:
    final Statement statement = Mockito.mock(Statement.class);
    final CommandId commandId = Mockito.mock(CommandId.class);
    Mockito.when(commandIdAssigner.getCommandId(statement)).thenReturn(commandId);

    // When:
    commandStore.enqueueCommand(
        "",
        statement,
        KSQL_CONFIG,
        Collections.emptyMap()
    );

    // Then:
    Mockito.verify(commandTopic).send(Mockito.eq(commandId), Mockito.any(Command.class));
  }

  @Test
  public void shouldNotEnqueueCommandIfThereIsAnException() throws Exception {
    // Given:
    final Statement statement = Mockito.mock(Statement.class);
    final CommandId commandId = Mockito.mock(CommandId.class);
    Mockito.when(commandIdAssigner.getCommandId(statement)).thenReturn(commandId);
    Mockito.doThrow(new InterruptedException()).when(commandTopic).send(Mockito.eq(commandId), Mockito.any(Command.class));

    // When:
    try {
      commandStore.enqueueCommand(
          "",
          statement,
          KSQL_CONFIG,
          Collections.emptyMap()
      );
    } catch (KsqlException e) {
      assertThat(e.getMessage(), equalTo("Could not write the statement '' into the command topic."));
      return;
    }
    fail();
  }

  @Test
  public void shouldGetNewCommandsCorrectly() {
    // Given:
    final QueuedCommand queuedCommand = Mockito.mock(QueuedCommand.class);
    Mockito.when(commandTopic.getNewCommands(Mockito.anyMap())).thenReturn(Collections.singletonList(queuedCommand));

    // When:
    final List<QueuedCommand> queuedCommandList = commandStore.getNewCommands();

    // Then:
    assertThat(queuedCommandList, equalTo(Collections.singletonList(queuedCommand)));
  }

  @Test
  public void shouldGetRestoreCommands() {
    // Given:
    final RestoreCommands restoreCommands = Mockito.mock(RestoreCommands.class);
    Mockito.when(commandTopic.getRestoreCommands(Mockito.any(Duration.class))).thenReturn(restoreCommands);

    // When:
    final RestoreCommands restoreCommands1 = commandStore.getRestoreCommands();

    // Then:
    assertThat(restoreCommands1, equalTo(restoreCommands));
  }
}
