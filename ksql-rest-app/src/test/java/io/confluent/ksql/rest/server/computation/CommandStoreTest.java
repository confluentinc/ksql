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
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.rest.entity.CommandStatus.Status;
import io.confluent.ksql.rest.server.CommandTopic;
import io.confluent.ksql.util.KsqlConfig;
import java.util.Collections;
import java.util.List;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

@RunWith(MockitoJUnitRunner.class)
@SuppressWarnings("unchecked")
public class CommandStoreTest {

  @Mock
  private KsqlConfig ksqlConfig;
  @Mock
  private CommandIdAssigner commandIdAssigner;
  @Mock
  private CommandTopic commandTopic;
  @Mock
  private Statement statement;

  @Mock
  private CommandId commandId1;
  @Mock
  private Command command1;
  @Mock
  private CommandId commandId2;

  private CommandStore commandStore;

  @Before
  public void setup() {
    commandStore = new CommandStore(commandIdAssigner, commandTopic);
  }

  @Test(expected = IllegalStateException.class)
  public void shouldFailEnqueueIfCommandWithSameIdRegistered() {
    // Given:
    when(commandIdAssigner.getCommandId(any())).thenReturn(commandId1)
        .thenReturn(commandId1);
    commandStore.enqueueCommand("foo", statement, ksqlConfig, Collections.emptyMap());

    // When:
    commandStore.enqueueCommand("foo", statement, ksqlConfig, Collections.emptyMap());
  }

  @Test
  public void shouldCleanupCommandStatusOnProduceError() {
    // Given:
    when(commandIdAssigner.getCommandId(any())).thenReturn(commandId1)
        .thenReturn(commandId1);
    doThrow(new RuntimeException("oops!")).doNothing().when(commandTopic).send(any(), any());
    try {
      commandStore.enqueueCommand("foo", statement, ksqlConfig, Collections.emptyMap());
      fail();
    } catch (final RuntimeException r) {
      // Do nothing.
    }

    // When:
    commandStore.enqueueCommand("foo", statement, ksqlConfig, Collections.emptyMap());

    // Then:
    verify(commandTopic, times(2)).send(any(), any());

  }

  @Test
  public void shouldEnqueueNewAfterHandlingExistingCommand() {
    // Given:
    when(commandIdAssigner.getCommandId(any())).thenReturn(commandId1)
        .thenReturn(commandId1);

    // When:
    commandStore.enqueueCommand("foo", statement, ksqlConfig, Collections.emptyMap());

    // Then:
    verify(commandTopic).send(same(commandId1), any());

  }

  @Test
  public void shouldRegisterBeforeDistributeAndReturnStatusOnGetNewCommands() {
    // Given:
    when(commandIdAssigner.getCommandId(any())).thenReturn(commandId1)
        .thenReturn(commandId1);
    // ensure that the command was registered and the status was set.
    doAnswer(
        (Answer) invocation -> {
          assertThat(commandStore.getCommandStatusMap().size(), equalTo(1));
          assertThat(commandStore.getCommandStatusMap().get(commandId1).getStatus().getStatus(),
              equalTo(Status.QUEUED));
          return null;

        }
    ).when(commandTopic).send(any(), any());

    // When:
    commandStore.enqueueCommand("foo", statement, ksqlConfig, Collections.emptyMap());

    // Then:
    verify(commandTopic).send(same(commandId1), any(Command.class));
  }

  @Test
  public void shouldFilterNullCommands() {
    // Given:
    when(commandTopic.getNewCommands(any()))
        .thenReturn((Iterable) ImmutableList.of(
            new ConsumerRecord<>("", 1, 1, commandId1, command1),
            new ConsumerRecord<>("", 1, 1, commandId2, null)
        ));

    // When:
    final List<QueuedCommand> commands = commandStore.getNewCommands();

    // Then:
    assertThat(commands, equalTo(ImmutableList.of(new QueuedCommand(commandId1, command1))));
  }


  @Test
  public void shouldDistributeCommand() {
    // Given:
    when(commandIdAssigner.getCommandId(any())).thenReturn(commandId1)
        .thenReturn(commandId1);

    // When:
    commandStore.enqueueCommand("foo", statement, ksqlConfig, Collections.emptyMap());

    // Then:
    verify(commandTopic).send(same(commandId1), any());
  }

  @Test
  public void shouldCloseCommandTopicOnClose() {
    // When:
    commandStore.close();

    // Then:
    verify(commandTopic).close();
  }

}
