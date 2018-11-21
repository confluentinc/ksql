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
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.rest.server.CommandTopic;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;

import org.mockito.junit.MockitoJUnitRunner;


@SuppressWarnings("unchecked")
@RunWith(MockitoJUnitRunner.class)
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
  @Mock
  private Command command2;
  @Mock
  private CommandId commandId3;
  @Mock
  private Command command3;
  @Mock
  private RestoreCommands restoreCommands;
  @Captor
  private ArgumentCaptor<Duration> durationCaptor;

  private CommandStore commandStore;

  @Before
  public void setup() {
    commandStore = new CommandStore(commandIdAssigner, commandTopic);
  }

  @Test
  public void shouldCallCloseCorrectly() {
    // When:
    commandStore.close();

    // Then:
    verify(commandTopic).close();
  }

  @Test
  public void shouldEnqueueCommandCorrectly() throws Exception {
    // Given:
    when(commandIdAssigner.getCommandId(statement)).thenReturn(commandId1);
    when(ksqlConfig.getAllConfigPropsWithSecretsObfuscated()).thenReturn(Collections.emptyMap());

    // When:
    commandStore.enqueueCommand(
        "",
        statement,
        ksqlConfig,
        Collections.emptyMap()
    );

    // Then:
    verify(commandTopic).send(eq(commandId1), any(Command.class));
  }

  @Test (expected = KsqlException.class)
  public void shouldNotEnqueueCommandIfThereIsAnException() throws Exception {
    // Given:
    when(commandIdAssigner.getCommandId(statement)).thenReturn(commandId1);
    doThrow(new KsqlException("")).when(commandTopic).send(eq(commandId1), any(Command.class));

    // When:
    commandStore.enqueueCommand(
        "",
        statement,
        ksqlConfig,
        Collections.emptyMap()
    );
  }

  @Test
  public void shouldGetNewCommandsCorrectly() {
    // Given:
    final ConsumerRecords<CommandId, Command> records = new ConsumerRecords<>(
        Collections.singletonMap(new TopicPartition("topic", 0), Arrays.asList(
            new ConsumerRecord<>("topic", 0, 0, commandId1, command1),
            new ConsumerRecord<>("topic", 0, 0, commandId2, command2),
            new ConsumerRecord<>("topic", 0, 0, commandId3, command3))
        ));
    when(commandTopic.getNewCommands(any(Duration.class))).thenReturn(records);

    // When:
    final List<QueuedCommand> queuedCommandList = commandStore.getNewCommands();

    // Then:
    assertThat(queuedCommandList.get(0).getCommand(), equalTo(Optional.of(command1)));
    assertThat(queuedCommandList.get(0).getCommandId(), equalTo(commandId1));
    assertThat(queuedCommandList.get(1).getCommand(), equalTo(Optional.of(command2)));
    assertThat(queuedCommandList.get(1).getCommandId(), equalTo(commandId2));
    assertThat(queuedCommandList.get(2).getCommand(), equalTo(Optional.of(command3)));
    assertThat(queuedCommandList.get(2).getCommandId(), equalTo(commandId3));

  }

  @Test
  public void shouldGetRestoreCommands() {
    // Given:
    when(commandTopic.getRestoreCommands(any(Duration.class))).thenReturn(restoreCommands);

    // When:
    final RestoreCommands restoreCommands1 = commandStore.getRestoreCommands();

    // Then:
    assertThat(restoreCommands1, equalTo(restoreCommands));
  }

  @Test
  public void shouldPassTheCorrectDuration() {

    // When:
    final RestoreCommands restoreCommands1 = commandStore.getRestoreCommands();

    // Then:
    verify(commandTopic).getRestoreCommands(durationCaptor.capture());
    assertThat(durationCaptor.getValue(), equalTo(Duration.ofMillis(5000)));
  }
}
