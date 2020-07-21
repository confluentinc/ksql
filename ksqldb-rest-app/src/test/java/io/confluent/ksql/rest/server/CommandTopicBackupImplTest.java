/*
 * Copyright 2020 Confluent Inc.
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

package io.confluent.ksql.rest.server;

import com.google.common.base.Ticker;
import io.confluent.ksql.rest.entity.CommandId;
import io.confluent.ksql.rest.server.computation.Command;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.Pair;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Optional;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class CommandTopicBackupImplTest {
  private static final String COMMAND_TOPIC_NAME = "command_topic";

  private Pair<CommandId, Command> command1 = newStreamRecord("stream1");
  private Pair<CommandId, Command> command2 = newStreamRecord("stream2");
  private Pair<CommandId, Command> command3 = newStreamRecord("stream3");

  @Mock
  private Ticker ticker;

  @Rule
  public TemporaryFolder backupLocation = new TemporaryFolder();

  private CommandTopicBackupImpl commandTopicBackup;

  @Before
  public void setup() {
    commandTopicBackup = new CommandTopicBackupImpl(
        backupLocation.getRoot().getAbsolutePath(), COMMAND_TOPIC_NAME, ticker);
  }

  private Pair<CommandId, Command> newStreamRecord(final String streamName) {
    final CommandId commandId = new CommandId(
        CommandId.Type.STREAM, streamName, CommandId.Action.CREATE);
    final Command command = new Command(
        String.format("CREATE STREAM %s (id INT) WITH (kafka_topic='%s", streamName, streamName),
        Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty()
    );

    return new Pair<>(commandId, command);
  }

  @Test
  public void shouldThrowWhenBackupLocationIsNotDirectory() throws IOException {
    // Given
    final File file = backupLocation.newFile();

    // When
    final Exception e = assertThrows(
        KsqlException.class,
        () -> new CommandTopicBackupImpl(file.getAbsolutePath(), COMMAND_TOPIC_NAME)
    );

    // Then
    assertThat(e.getMessage(), containsString(String.format(
        "Backup location '%s' does not exist or it is not a directory.",
        file.getAbsolutePath()
    )));
  }

  @Test
  public void shouldThrowWhenBackupLocationDoesNotExist() {
    // When
    final Exception e = assertThrows(
        KsqlException.class,
        () -> new CommandTopicBackupImpl("/not-existing-directory", COMMAND_TOPIC_NAME)
    );

    // Then
    assertThat(e.getMessage(), containsString(String.format(
        "Backup location '/not-existing-directory' does not exist or it is not a directory."
    )));
  }

  @Test
  public void shouldWriteRecordsToReplayFile() throws IOException {
    // Given
    commandTopicBackup.initialize();

    // When
    final ConsumerRecord<CommandId, Command> record = newConsumerRecord(command1);
    commandTopicBackup.writeRecord(record);

    // Then
    final List<Pair<CommandId, Command>> commands =
        commandTopicBackup.getReplayFile().readRecords();
    assertThat(commands.size(), is(1));
    assertThat(commands.get(0).left, is(command1.left));
    assertThat(commands.get(0).right, is(command1.right));
  }

  @Test
  public void shouldIgnoreRecordPreviouslyReplayed() throws IOException {
    // Given
    final ConsumerRecord<CommandId, Command> record = newConsumerRecord(command1);
    commandTopicBackup.initialize();
    commandTopicBackup.writeRecord(record);
    final BackupReplayFile previousReplayFile = commandTopicBackup.getReplayFile();

    // When
    // A 2nd initialize call will open the latest backup and read the previous replayed commands
    commandTopicBackup.initialize();
    commandTopicBackup.writeRecord(record);
    final BackupReplayFile currentReplayFile = commandTopicBackup.getReplayFile();

    // Then
    final List<Pair<CommandId, Command>> commands = currentReplayFile.readRecords();
    assertThat(commands.size(), is(1));
    assertThat(commands.get(0).left, is(command1.left));
    assertThat(commands.get(0).right, is(command1.right));
    assertThat(currentReplayFile.getPath(), is(previousReplayFile.getPath()));
  }

  @Test
  public void shouldCreateNewReplayFileIfNewRecordsDoNotMatchPreviousBackups() throws IOException {
    // Given
    final ConsumerRecord<CommandId, Command> record1 = newConsumerRecord(command1);
    commandTopicBackup.initialize();
    commandTopicBackup.writeRecord(record1);
    final BackupReplayFile previousReplayFile = commandTopicBackup.getReplayFile();

    // When
    // A 2nd initialize call will open the latest backup and read the previous replayed commands
    commandTopicBackup.initialize();
    final ConsumerRecord<CommandId, Command> record2 = newConsumerRecord(command2);
    // Need to increase the ticker so the new file has a new timestamp
    when(ticker.read()).thenReturn(2L);
    // The write command will create a new replay file with the new command
    commandTopicBackup.writeRecord(record2);
    final BackupReplayFile currentReplayFile = commandTopicBackup.getReplayFile();

    // Then
    List<Pair<CommandId, Command>> commands = previousReplayFile.readRecords();
    assertThat(commands.size(), is(1));
    assertThat(commands.get(0).left, is(command1.left));
    assertThat(commands.get(0).right, is(command1.right));
    commands = currentReplayFile.readRecords();
    assertThat(commands.size(), is(1));
    assertThat(commands.get(0).left, is(command2.left));
    assertThat(commands.get(0).right, is(command2.right));
    assertThat(currentReplayFile.getPath(), not(previousReplayFile.getPath()));
  }

  @Test
  public void shouldWritePreviousReplayedRecordsAlreadyChecked() throws IOException {
    // Given
    final ConsumerRecord<CommandId, Command> record1 = newConsumerRecord(command1);
    final ConsumerRecord<CommandId, Command> record2 = newConsumerRecord(command2);
    commandTopicBackup.initialize();
    commandTopicBackup.writeRecord(record1);
    commandTopicBackup.writeRecord(record2);
    final BackupReplayFile previousReplayFile = commandTopicBackup.getReplayFile();

    // When
    // A 2nd initialize call will open the latest backup and read the previous replayed commands
    commandTopicBackup.initialize();
    // Need to increase the ticker so the new file has a new timestamp
    when(ticker.read()).thenReturn(2L);
    // command1 is ignored because it was previously replayed
    commandTopicBackup.writeRecord(record1);
    // The write command will create a new replay file with the new command, and command1 will
    // be written to have a complete backup
    final ConsumerRecord<CommandId, Command> record3 = newConsumerRecord(command3);
    commandTopicBackup.writeRecord(record3);
    final BackupReplayFile currentReplayFile = commandTopicBackup.getReplayFile();

    // Then
    List<Pair<CommandId, Command>> commands = previousReplayFile.readRecords();
    assertThat(commands.size(), is(2));
    assertThat(commands.get(0).left, is(command1.left));
    assertThat(commands.get(0).right, is(command1.right));
    assertThat(commands.get(1).left, is(command2.left));
    assertThat(commands.get(1).right, is(command2.right));
    commands = currentReplayFile.readRecords();
    assertThat(commands.size(), is(2));
    assertThat(commands.get(0).left, is(command1.left));
    assertThat(commands.get(0).right, is(command1.right));
    assertThat(commands.get(1).left, is(command3.left));
    assertThat(commands.get(1).right, is(command3.right));
    assertThat(currentReplayFile.getPath(), not(previousReplayFile.getPath()));
  }

  @Test
  public void shouldCreateNewReplayFileWhenNoBackupFilesExist() {
    // Given:
    when(ticker.read()).thenReturn(123L);

    // When:
    final BackupReplayFile replayFile = commandTopicBackup.openOrCreateReplayFile();

    // Then:
    assertThat(replayFile.getPath(), is(String.format(
        "%s/backup_command_topic_123", backupLocation.getRoot().getAbsolutePath()
    )));
  }

  @Test
  public void shouldOpenLatestReplayFileWhenOneExists() throws IOException {
    // Given:
    backupLocation.newFile("backup_command_topic_111");

    // When:
    final BackupReplayFile replayFile = commandTopicBackup.openOrCreateReplayFile();

    // Then:
    assertThat(replayFile.getPath(), is(String.format(
        "%s/backup_command_topic_111", backupLocation.getRoot().getAbsolutePath()
    )));
  }

  @Test
  public void shouldOpenLatestReplayFileWhenTwoExist() throws IOException {
    // Given:
    backupLocation.newFile("backup_command_topic_111");
    backupLocation.newFile("backup_command_topic_222");

    // When:
    final BackupReplayFile replayFile = commandTopicBackup.openOrCreateReplayFile();

    // Then:
    assertThat(replayFile.getPath(), is(String.format(
        "%s/backup_command_topic_222", backupLocation.getRoot().getAbsolutePath()
    )));
  }

  @Test
  public void shouldOpenLatestReplayFileWhenDifferentCommandTopicNamesExist() throws IOException {
    // Given:
    backupLocation.newFile("backup_command_topic_111");
    backupLocation.newFile("backup_other_command_topic_222");

    // When:
    final BackupReplayFile replayFile = commandTopicBackup.openOrCreateReplayFile();

    // Then:
    assertThat(replayFile.getPath(), is(String.format(
        "%s/backup_command_topic_111", backupLocation.getRoot().getAbsolutePath()
    )));
  }

  @Test
  public void shouldOpenReplayFileAndIgnoreFileWithInvalidTimestamp() throws IOException {
    // Given:
    backupLocation.newFile("backup_command_topic_111");
    backupLocation.newFile("backup_command_topic_222x");

    // When:
    final BackupReplayFile replayFile = commandTopicBackup.openOrCreateReplayFile();

    // Then:
    assertThat(replayFile.getPath(), is(String.format(
        "%s/backup_command_topic_111", backupLocation.getRoot().getAbsolutePath()
    )));
  }

  private ConsumerRecord<CommandId, Command> newConsumerRecord(
      final Pair<CommandId, Command> record
  ) {
    return new ConsumerRecord<>("topic", 0, 0, record.left, record.right);
  }
}
